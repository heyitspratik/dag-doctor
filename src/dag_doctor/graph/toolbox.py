"""The tools as the graph sees them.

Two jobs. It renders the tool catalogue into something a model can choose from, and it
turns a tool call into an :class:`Evidence` record whatever happens, including when the
model invents a tool that does not exist. A hallucinated tool name is evidence that the
model guessed, and the investigation should carry on and be told, not crash.
"""

from collections.abc import Mapping, Sequence

from pydantic import JsonValue

from dag_doctor.core.exceptions import UnknownToolError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import Evidence
from dag_doctor.tools.base import RunnableTool

logger = get_logger(__name__)


class Toolbox:
    """The tools one investigation may use."""

    def __init__(self, tools: Sequence[RunnableTool]) -> None:
        """Initialise the toolbox.

        Args:
            tools: The tools to expose. Anything not in here cannot be reached, which is
                the boundary the model is confined to.
        """
        self._tools = {tool.name: tool for tool in tools}

    @property
    def names(self) -> list[str]:
        """Every tool name, sorted."""
        return sorted(self._tools)

    def describe(self) -> str:
        """Render the catalogue for a prompt.

        Arguments are listed with their types and which are required, because a model
        given only a tool name will invent plausible-looking arguments that fail
        validation and waste a call.
        """
        lines: list[str] = []
        for name in self.names:
            tool = self._tools[name]
            lines.append(f"  - {name}: {tool.description}")
            lines.append(f"    arguments: {_render_arguments(tool)}")
        return "\n".join(lines)

    async def run(
        self,
        name: str,
        arguments: Mapping[str, JsonValue],
        context: Mapping[str, JsonValue] | None = None,
    ) -> Evidence:
        """Run one tool and record the outcome as evidence.

        Args:
            name: The tool the model asked for.
            arguments: The arguments it supplied, unvalidated.
            context: Facts the investigation already knows, filled in for any argument the
                tool declares and the model left out.

        Returns:
            An evidence record. Never raises: a tool that could not answer is a finding.
        """
        tool = self._tools.get(name)
        if tool is None:
            available = ", ".join(self.names)
            logger.warning("toolbox.unknown_tool", tool=name)
            return Evidence(
                tool_name=name,
                tool_input=dict(arguments),
                result={"error": "unknown_tool", "available": available},
                summary=f"no tool named {name!r}; available tools are {available}",
                succeeded=False,
            )

        supplied = self.arguments_for(name, arguments, context)
        result = await tool.run(supplied)
        return Evidence(
            tool_name=name,
            tool_input=dict(supplied),
            result=result.payload(),
            summary=result.summarise(),
            succeeded=result.ok,
            duration_ms=result.duration_ms,
        )

    def arguments_for(
        self,
        name: str,
        arguments: Mapping[str, JsonValue],
        context: Mapping[str, JsonValue] | None = None,
    ) -> dict[str, JsonValue]:
        """The arguments a call would actually run with, known facts included.

        Exposed so a caller can tell whether a call it is about to make is one it has
        already made, without running it to find out.
        """
        tool = self._tools.get(name)
        if tool is None:
            return dict(arguments)
        return self._with_context(tool, arguments, context)

    def _with_context(
        self,
        tool: RunnableTool,
        arguments: Mapping[str, JsonValue],
        context: Mapping[str, JsonValue] | None,
    ) -> dict[str, JsonValue]:
        """Fill in the facts the investigation already holds.

        Which DAG, task and run failed are properties of the incident, not choices a model
        should be making. Requiring it to copy them into every call adds a failure mode
        with no upside, and small models get them wrong often enough to lose an entire
        investigation to it.

        Only arguments the tool actually declares are filled, and anything the model did
        supply wins, so it can still deliberately point a tool at a different task. A null
        or empty value does not count as supplied: that is the model admitting it does not
        know, which is exactly the case this exists to cover.
        """
        # A model that cannot fill a field emits null for it rather than leaving it out.
        # Read literally that is a value, so it fails validation on a required argument
        # and overrides a perfectly good default on an optional one.
        supplied: dict[str, JsonValue] = {
            key: value for key, value in arguments.items() if value is not None and value != ""
        }
        if not context:
            return supplied
        properties = tool.input_schema().get("properties")
        declared = set(properties) if isinstance(properties, dict) else set()
        known = {
            key: value for key, value in context.items() if key in declared and key not in supplied
        }
        return {**known, **supplied}

    def find(self, name: str) -> RunnableTool | None:
        """Look a tool up, returning nothing rather than raising when it is not there."""
        return self._tools.get(name)

    def require(self, name: str) -> RunnableTool:
        """Fetch a tool by name.

        Args:
            name: The tool name.

        Returns:
            The tool.

        Raises:
            UnknownToolError: If it is not in this toolbox.
        """
        try:
            return self._tools[name]
        except KeyError:
            raise UnknownToolError(
                f"No tool named {name!r}", details={"tool": name, "available": self.names}
            ) from None


def _render_arguments(tool: RunnableTool) -> str:
    """Summarise a tool's argument schema in one line."""
    schema = tool.input_schema()
    properties = schema.get("properties")
    if not isinstance(properties, dict) or not properties:
        return "none"
    required = schema.get("required")
    required_names = set(required) if isinstance(required, list) else set()
    parts = []
    for field_name, spec in properties.items():
        kind = spec.get("type", "any") if isinstance(spec, dict) else "any"
        marker = "" if field_name in required_names else "?"
        parts.append(f"{field_name}{marker}: {kind}")
    return ", ".join(parts)
