"""The tool contract.

Three rules hold for every tool, and all three are enforced here rather than asked for in
a prompt.

No tool returns a raw string blob. Inputs are validated Pydantic models and outputs are
typed models, so the graph reasons over fields instead of re-parsing prose it just asked a
model to write.

No tool raises into the graph. A tool that cannot answer returns a typed failure, because
"the metadata database was unreachable" is evidence the investigation should weigh, not a
reason to abandon an incident that other tools could still have explained.

Every tool has a timeout. A hung connection would otherwise consume the whole
investigation budget in one call.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from collections.abc import Mapping
from enum import StrEnum
from typing import ClassVar, Protocol

from pydantic import BaseModel, ConfigDict, JsonValue, ValidationError

from dag_doctor.core.exceptions import DagDoctorError, ReadOnlyViolationError
from dag_doctor.core.logging import get_logger

logger = get_logger(__name__)

#: Fallback when no budget is supplied. Real runs pass BudgetSettings.tool_timeout_s.
DEFAULT_TIMEOUT_S = 30.0


class ToolStatus(StrEnum):
    """How a tool call ended.

    Only ``OK`` carries data. The rest are distinguished rather than collapsed into one
    error because the graph routes on them: a ``NOT_FOUND`` is often itself the finding,
    while a ``TIMEOUT`` says nothing about the pipeline and should be retried or dropped.
    """

    OK = "ok"
    NOT_FOUND = "not_found"
    INVALID_INPUT = "invalid_input"
    TIMEOUT = "timeout"
    UNAVAILABLE = "unavailable"
    FORBIDDEN = "forbidden"
    ERROR = "error"


class ToolInput(BaseModel):
    """Base class for tool inputs.

    ``extra="forbid"`` matters more than it looks. The arguments come from a language
    model, and silently accepting a field the tool does not read turns a hallucinated
    parameter into a query that answers a different question than the one asked.
    """

    model_config = ConfigDict(extra="forbid")


class ToolOutput(BaseModel):
    """Base class for tool outputs."""

    @abstractmethod
    def summarise(self) -> str:
        """Describe this result in one line.

        The graph puts this on the evidence record so a later node, or a human reading the
        trace, can see what a tool found without unpacking its payload.
        """


class ToolResult(BaseModel):
    """The envelope every tool call returns, successful or not."""

    tool: str
    status: ToolStatus
    data: ToolOutput | None = None
    error: str | None = None
    duration_ms: int = 0

    @property
    def ok(self) -> bool:
        """Whether the call produced data."""
        return self.status is ToolStatus.OK

    def summarise(self) -> str:
        """One line describing the outcome, whatever it was."""
        if self.data is not None:
            return self.data.summarise()
        return f"{self.tool} returned {self.status.value}: {self.error or 'no detail'}"

    def payload(self) -> dict[str, JsonValue]:
        """The result as plain JSON, for the evidence record and the step trace."""
        dumped: dict[str, JsonValue] = self.model_dump(mode="json")
        return dumped


class RunnableTool(Protocol):
    """What the registry and the graph need from a tool, without its concrete types."""

    @property
    def name(self) -> str:
        """The registered name."""
        ...

    @property
    def description(self) -> str:
        """What the tool does, as shown to the model."""
        ...

    def input_schema(self) -> dict[str, JsonValue]:
        """The JSON schema for this tool's arguments."""
        ...

    async def run(self, raw_input: Mapping[str, JsonValue]) -> ToolResult:
        """Validate, execute under a timeout, and return a typed result."""
        ...


class BaseTool[InputT: ToolInput, OutputT: ToolOutput](ABC):
    """Base class for every tool.

    Subclasses implement :meth:`execute` against a validated input model and return a
    typed output. Everything that must hold for all tools, validation, the timeout, and
    turning a fault into a typed result, happens in :meth:`run` so that no subclass can
    forget it.
    """

    name: ClassVar[str]
    description: ClassVar[str]
    input_model: type[InputT]

    def __init__(self, timeout_s: float = DEFAULT_TIMEOUT_S) -> None:
        """Initialise the tool.

        Args:
            timeout_s: How long one call may take before it is abandoned.
        """
        self.timeout_s = timeout_s

    def input_schema(self) -> dict[str, JsonValue]:
        """The JSON schema for this tool's arguments, as shown to the model."""
        schema: dict[str, JsonValue] = self.input_model.model_json_schema()
        return schema

    @abstractmethod
    async def execute(self, tool_input: InputT) -> OutputT:
        """Do the work.

        Args:
            tool_input: The validated arguments.

        Returns:
            The structured result.
        """

    async def run(self, raw_input: Mapping[str, JsonValue]) -> ToolResult:
        """Validate, execute under a timeout, and return a typed result.

        Args:
            raw_input: The arguments as the caller supplied them, unvalidated.

        Returns:
            A result carrying either data or a typed failure. This never raises: a tool
            that cannot answer is evidence, not a reason to abandon the investigation.
        """
        started = time.perf_counter()
        try:
            tool_input = self.input_model.model_validate(dict(raw_input))
        except ValidationError as exc:
            return self._failed(
                ToolStatus.INVALID_INPUT,
                f"{exc.error_count()} invalid argument(s) for {self.name}",
                started,
            )

        try:
            data = await asyncio.wait_for(self.execute(tool_input), timeout=self.timeout_s)
        except TimeoutError:
            return self._failed(
                ToolStatus.TIMEOUT, f"{self.name} timed out after {self.timeout_s}s", started
            )
        except ReadOnlyViolationError as exc:
            return self._failed(ToolStatus.FORBIDDEN, exc.message, started)
        except TargetNotFoundError as exc:
            return self._failed(ToolStatus.NOT_FOUND, str(exc), started)
        except DagDoctorError as exc:
            return self._failed(ToolStatus.UNAVAILABLE, exc.message, started)
        except Exception as exc:
            # The one deliberate catch-all in the package. A driver raising something
            # undocumented must not take down an investigation that other tools could
            # still have completed, and the graph needs the failure as evidence.
            logger.exception("tool.unexpected_error", tool=self.name)
            return self._failed(ToolStatus.ERROR, f"{type(exc).__name__}: {exc}", started)

        result = ToolResult(
            tool=self.name,
            status=ToolStatus.OK,
            data=data,
            duration_ms=_elapsed_ms(started),
        )
        logger.debug("tool.ok", tool=self.name, duration_ms=result.duration_ms)
        return result

    def _failed(self, status: ToolStatus, error: str, started: float) -> ToolResult:
        """Build a failure result and log it once."""
        logger.warning("tool.failed", tool=self.name, status=status.value, error=error)
        return ToolResult(
            tool=self.name, status=status, error=error, duration_ms=_elapsed_ms(started)
        )


class TargetNotFoundError(Exception):
    """Raised inside a tool when the thing it was asked about does not exist.

    Deliberately not a DagDoctorError: absence is a finding rather than a fault, and
    :meth:`BaseTool.run` maps it to ``NOT_FOUND`` rather than ``UNAVAILABLE``.
    """


def _elapsed_ms(started: float) -> int:
    """Milliseconds since a perf_counter reading."""
    return int((time.perf_counter() - started) * 1000)
