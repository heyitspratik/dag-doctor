"""The tool registry.

Tools are addressed by name, so the graph never imports a tool module directly and adding
one is a registration rather than an edit to the nodes. The registry is also the boundary
the model is confined to: it can ask for a registered name or be refused, and it can never
reach a callable that was not deliberately exposed.
"""

from collections.abc import Iterator, Mapping

from dag_doctor.core.exceptions import UnknownToolError
from dag_doctor.tools.base import RunnableTool

_REGISTRY: dict[str, RunnableTool] = {}


def register(tool: RunnableTool) -> RunnableTool:
    """Register a tool instance under its own name.

    Args:
        tool: The tool to expose.

    Returns:
        The tool, so this reads naturally at the end of a module.

    Raises:
        UnknownToolError: If the name is already taken. Two tools answering to one name
            would make which one runs depend on import order.
    """
    if tool.name in _REGISTRY:
        raise UnknownToolError(
            f"A tool named {tool.name!r} is already registered",
            details={"tool": tool.name},
        )
    _REGISTRY[tool.name] = tool
    return tool


def get(name: str) -> RunnableTool:
    """Look a tool up by name.

    Args:
        name: The registered name, usually chosen by the model.

    Returns:
        The tool.

    Raises:
        UnknownToolError: If nothing is registered under that name, which is what happens
            when a model invents one.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        raise UnknownToolError(
            f"No tool named {name!r}",
            details={"tool": name, "available": sorted(_REGISTRY)},
        ) from None


def names() -> list[str]:
    """Every registered tool name, sorted."""
    return sorted(_REGISTRY)


def all_tools() -> Mapping[str, RunnableTool]:
    """Every registered tool, keyed by name."""
    return dict(_REGISTRY)


def catalogue() -> Iterator[tuple[str, str]]:
    """Name and description for each tool, for building a prompt."""
    for name in names():
        yield name, _REGISTRY[name].description


def clear() -> None:
    """Empty the registry.

    For tests only. A test that registers a tool must not leak it into the next one.
    """
    _REGISTRY.clear()
