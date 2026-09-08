"""Gathering evidence: decide which tools to call, then call them.

The model chooses; this node enforces. It caps how many calls run against the remaining
tool budget, so a model that asks for nine tools when four calls remain gets four rather
than overrunning the budget, and it records every result as evidence, including the
failures. A tool that could not answer is something the investigation should know.
"""

from typing import ClassVar

from pydantic import BaseModel, Field, JsonValue

from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import Evidence, NodeName
from dag_doctor.graph import prompts
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.nodes.base import InvestigationNode, NodeUpdate
from dag_doctor.graph.state import InvestigationState
from dag_doctor.graph.toolbox import Toolbox

logger = get_logger(__name__)

#: Calls allowed in one pass. Keeps a single iteration from consuming the whole budget
#: before any of its results have been read.
MAX_CALLS_PER_ITERATION = 4


class PlannedCall(BaseModel):
    """One tool call the model wants made."""

    tool: str
    arguments: dict[str, JsonValue] = Field(default_factory=dict)
    why: str = ""


class ToolPlan(BaseModel):
    """What the model is asked for when gathering evidence."""

    calls: list[PlannedCall] = Field(default_factory=list)


class GatherEvidenceNode(InvestigationNode):
    """Choose and run the next tool calls."""

    node: ClassVar[NodeName] = "gather_evidence"

    def __init__(self, caller: ModelCaller, toolbox: Toolbox) -> None:
        """Initialise the node.

        Args:
            caller: How this node reaches a model.
            toolbox: The tools it may run, and the only ones it can.
        """
        self._caller = caller
        self._toolbox = toolbox

    async def run(self, state: InvestigationState) -> NodeUpdate:
        """Ask for a plan, run what the budget allows, and record the results."""
        allowed = min(MAX_CALLS_PER_ITERATION, state.tool_calls_remaining)
        prompt = prompts.render(
            "gather_evidence",
            dag_id=state.failure.dag_id,
            task_id=state.failure.task_id,
            run_id=state.failure.run_id,
            try_number=state.failure.try_number,
            exception_type=state.failure.exception_type or "(none recorded)",
            exception_message=state.failure.exception_message or "(none recorded)",
            triage_category=state.triage.category.value if state.triage else "unclassified",
            triage_rationale=state.triage.rationale if state.triage else "no triage recorded",
            evidence=_render_evidence(state),
            tools=self._toolbox.describe(),
            max_calls=allowed,
            budget_note=(
                f"{state.tool_calls_remaining} tool call(s) and "
                f"{state.iterations_remaining} iteration(s) remain in the budget."
            ),
        )
        plan = await self._caller.call(self.node, prompt, ToolPlan)

        gathered: list[Evidence] = []
        for call in plan.value.calls[:allowed]:
            gathered.append(await self._toolbox.run(call.tool, call.arguments))

        return NodeUpdate(
            updates={
                "evidence": [*state.evidence, *gathered],
                "tool_calls_made": state.tool_calls_made + len(gathered),
                "iteration": state.iteration + 1,
            },
            step_input={
                "requested": [call.tool for call in plan.value.calls],
                "allowed": allowed,
            },
            step_output={
                "ran": [item.tool_name for item in gathered],
                "succeeded": [item.tool_name for item in gathered if item.succeeded],
            },
            model_used=plan.model_used,
            prompt_tokens=plan.prompt_tokens,
            completion_tokens=plan.completion_tokens,
        )


def _render_evidence(state: InvestigationState) -> str:
    """List the evidence so far as numbered one-line summaries.

    Summaries rather than payloads: the full tool results would crowd the prompt, and each
    tool already knows how to describe its own finding in one line.
    """
    if not state.evidence:
        return "  (nothing gathered yet)"
    return "\n".join(
        f"  {index}. [{item.tool_name}] {item.summary}"
        for index, item in enumerate(state.evidence, start=1)
    )
