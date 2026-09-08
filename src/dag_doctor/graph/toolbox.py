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

    async def run(self, name: str, arguments: Mapping[str, JsonValue]) -> Evidence:
        """Run one tool and record the outcome as evidence.

        Args:
            name: The tool the model asked for.
            arguments: The arguments it supplied, unvalidated.

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

        result = await tool.run(arguments)
        return Evidence(
            tool_name=name,
            tool_input=dict(arguments),
            result=result.payload(),
            summary=result.summarise(),
            succeeded=result.ok,
            duration_ms=result.duration_ms,
        )

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
