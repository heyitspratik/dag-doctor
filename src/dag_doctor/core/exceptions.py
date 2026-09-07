"""The exception hierarchy for the whole package.

Every failure the application raises deliberately descends from :class:`DagDoctorError`.
Each subclass carries a stable machine-readable ``code`` and an HTTP status, which is what
lets the API render one consistent error envelope from an exception handler instead of
scattering ``HTTPException`` raises through the route functions.

Note what is deliberately *not* here: a tool failing is not an exception. Tools return a
typed error result so the graph can reason about it as evidence, and a raise from inside a
tool would abort an investigation that could still have concluded. See
:mod:`dag_doctor.tools.base`.
"""

from collections.abc import Mapping
from typing import ClassVar


class DagDoctorError(Exception):
    """Base class for every error this package raises on purpose."""

    code: ClassVar[str] = "INTERNAL_ERROR"
    http_status: ClassVar[int] = 500

    def __init__(self, message: str, *, details: Mapping[str, object] | None = None) -> None:
        """Initialise the error.

        Args:
            message: Human-readable description, safe to show to an API caller.
            details: Structured context rendered into the API error envelope.
        """
        super().__init__(message)
        self.message = message
        self.details: dict[str, object] = dict(details or {})


class ConfigValidationError(DagDoctorError):
    """A settings value or a scenario definition failed validation."""

    code: ClassVar[str] = "CONFIG_INVALID"
    http_status: ClassVar[int] = 422


class InvalidFailureEventError(DagDoctorError):
    """A message on the failure topic could not be parsed into a failure event.

    Raised by the consumer, which routes the offending message to the dead-letter topic
    rather than retrying it. A malformed payload will never become well-formed on a retry.
    """

    code: ClassVar[str] = "INVALID_FAILURE_EVENT"
    http_status: ClassVar[int] = 422


class ToolExecutionError(DagDoctorError):
    """A tool could not run at all, as opposed to running and finding nothing.

    Reserved for faults outside the investigation: an unregistered tool name, input that
    fails its Pydantic schema, or a broken registry. Timeouts and unreachable backends are
    reported as typed tool results instead.
    """

    code: ClassVar[str] = "TOOL_EXECUTION_ERROR"
    http_status: ClassVar[int] = 500


class UnknownToolError(ToolExecutionError):
    """The graph asked for a tool that is not in the registry."""

    code: ClassVar[str] = "UNKNOWN_TOOL"
    http_status: ClassVar[int] = 422


class ReadOnlyViolationError(ToolExecutionError):
    """A tool attempted a statement that is not a read.

    This is enforced in code rather than trusted to the prompt or to database grants
    alone, so a prompt injection in a log line cannot turn a diagnosis into a write.
    """

    code: ClassVar[str] = "READ_ONLY_VIOLATION"
    http_status: ClassVar[int] = 403


class BudgetExhaustedError(DagDoctorError):
    """An investigation asked for more iterations or tool calls than its budget allows.

    Ordinarily the graph checks its budget and routes to ``escalate``, which is a normal
    terminal state producing an inconclusive diagnosis. This exception is the backstop for
    a caller that bypasses that routing, so a budget can never be silently exceeded.
    """

    code: ClassVar[str] = "BUDGET_EXHAUSTED"
    http_status: ClassVar[int] = 409


class ProviderUnavailableError(DagDoctorError):
    """The configured LLM provider is misconfigured, unreachable, or failed a call."""

    code: ClassVar[str] = "PROVIDER_UNAVAILABLE"
    http_status: ClassVar[int] = 502


class MessagingError(DagDoctorError):
    """The event log rejected an operation or is unreachable."""

    code: ClassVar[str] = "MESSAGING_ERROR"
    http_status: ClassVar[int] = 502


class PersistenceError(DagDoctorError):
    """The agent's own database rejected an operation or is unreachable."""

    code: ClassVar[str] = "PERSISTENCE_ERROR"
    http_status: ClassVar[int] = 502


class ResourceNotFoundError(DagDoctorError):
    """A requested incident, diagnosis, or investigation does not exist."""

    code: ClassVar[str] = "NOT_FOUND"
    http_status: ClassVar[int] = 404
