"""Working out what to pass a tool.

The first version of this asked the model for a free-form ``dict`` of arguments, described
only in prose in the prompt. A frontier model copes with that. A 3B model does not: given
no schema it invents a shape, and the observed failure was every scalar wrapped in a
dictionary, right values in the wrong container, rejected by validation every time.

So the model is asked against the tool's real schema, and only for the fields it actually
has to decide. Anything the investigation already knows, which DAG, task and run failed, is
filled in rather than requested, and a tool needing nothing beyond those is called without
consulting the model at all.
"""

from typing import cast

from pydantic import BaseModel, JsonValue, create_model

from dag_doctor.core.exceptions import DagDoctorError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import NodeName
from dag_doctor.graph import prompts
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.state import InvestigationState
from dag_doctor.graph.toolbox import Toolbox

logger = get_logger(__name__)


class ArgumentResolver:
    """Decides the arguments for one tool call."""

    def __init__(self, caller: ModelCaller, toolbox: Toolbox) -> None:
        """Initialise the resolver.

        Args:
            caller: How to reach a model when a choice genuinely has to be made.
            toolbox: The tools, and therefore their schemas.
        """
        self._caller = caller
        self._toolbox = toolbox

    async def resolve(
        self, node: NodeName, tool_name: str, state: InvestigationState, why: str = ""
    ) -> dict[str, JsonValue]:
        """Work out what to pass a tool.

        Args:
            node: The node asking, so the configured per-node model is used.
            tool_name: The tool about to run.
            state: The investigation, which supplies the facts already known.
            why: What the model said it was trying to find out, kept as context.

        Returns:
            The arguments the model chose. The known facts are added by the toolbox, so
            an empty result is normal rather than a failure.
        """
        tool = self._toolbox.find(tool_name)
        if tool is None:
            return {}

        choices = _fields_left_to_choose(tool.input_model, set(state.tool_context()))
        if choices is None:
            # Everything this tool takes is something the incident already tells us, so
            # there is nothing worth asking a model about.
            logger.debug("arguments.no_choice_needed", tool=tool_name)
            return {}

        prompt = prompts.render(
            "tool_arguments",
            tool=tool_name,
            description=tool.description,
            why=why or "no reason given",
            dag_id=state.failure.dag_id,
            task_id=state.failure.task_id,
            exception_type=state.failure.exception_type or "(none recorded)",
            exception_message=state.failure.exception_message or "(none recorded)",
            evidence=_render_evidence(state),
            connections=", ".join(_known_connections()),
        )
        try:
            answer = await self._caller.call(node, prompt, choices)
        except DagDoctorError as exc:
            # Better to run the tool on what we know and let it say what was missing than
            # to abandon the investigation because one argument could not be chosen.
            logger.warning("arguments.unresolved", tool=tool_name, error=exc.message)
            return {}
        # mode="json" because a field such as a datetime otherwise comes back as a Python
        # object, and the evidence record it lands in only accepts JSON values.
        return cast(dict[str, JsonValue], answer.value.model_dump(mode="json", exclude_none=True))


def _fields_left_to_choose(input_model: type[BaseModel], known: set[str]) -> type[BaseModel] | None:
    """Build a model of just the arguments a model still has to decide.

    Args:
        input_model: The tool's own argument model.
        known: Field names the investigation can fill in itself.

    Returns:
        A model covering the remaining fields, or ``None`` when none remain.
    """
    remaining = {
        name: (field.annotation, field)
        for name, field in input_model.model_fields.items()
        if name not in known
    }
    if not remaining:
        return None
    built: type[BaseModel] = create_model(  # type: ignore[call-overload]
        f"{input_model.__name__}Choice", **remaining
    )
    return built


def _known_connections() -> list[str]:
    """The connection names a tool may be pointed at.

    Listed in the prompt because a model that invents one gets a refusal and wastes a
    call, and there are only ever a couple to choose between.
    """
    return ["warehouse", "airflow"]


def _render_evidence(state: InvestigationState) -> str:
    """The evidence so far, so an argument can build on what is already known."""
    if not state.evidence:
        return "  (nothing gathered yet)"
    return "\n".join(
        f"  {index}. [{item.tool_name}] {item.summary}"
        for index, item in enumerate(state.evidence, start=1)
    )
