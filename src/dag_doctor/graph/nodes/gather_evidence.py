"""Gathering evidence: decide which tools to call, then call them.

The model chooses; this node enforces. It caps how many calls run against the remaining
tool budget, so a model that asks for nine tools when four calls remain gets four rather
than overrunning the budget, and it records every result as evidence, including the
failures. A tool that could not answer is something the investigation should know.
"""

import json
from collections.abc import Mapping
from typing import ClassVar

from pydantic import BaseModel, JsonValue

from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import Evidence, NodeName
from dag_doctor.graph import prompts
from dag_doctor.graph.arguments import ArgumentResolver
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.nodes.base import InvestigationNode, NodeUpdate
from dag_doctor.graph.state import InvestigationState
from dag_doctor.graph.toolbox import Toolbox

logger = get_logger(__name__)

#: Calls allowed in one pass. Keeps a single iteration from consuming the whole budget
#: before any of its results have been read.
MAX_CALLS_PER_ITERATION = 4


class PlannedCall(BaseModel):
    """One tool the model wants called.

    Deliberately no arguments. Asking for them here means asking against a free-form
    object with no schema, and a small model answers that by inventing a shape: the
    observed failure was every scalar wrapped in a dictionary. Arguments are resolved
    afterwards against each tool's real model, by
    :class:`dag_doctor.graph.arguments.ArgumentResolver`.
    """

    tool: str
    why: str = ""


class ToolPlan(BaseModel):
    """What the model is asked for when gathering evidence."""

    calls: list[PlannedCall] = []


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
        self._arguments = ArgumentResolver(caller, toolbox)

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

        context = state.tool_context()
        already = {(item.tool_name, _fingerprint(item.tool_input)) for item in state.evidence}
        gathered: list[Evidence] = []
        repeated = 0

        for call in plan.value.calls[:allowed]:
            arguments = await self._arguments.resolve(self.node, call.tool, state, call.why)
            final = self._toolbox.arguments_for(call.tool, arguments, context)
            signature = (call.tool, _fingerprint(final))

            if signature in already:
                # The answer is already in the evidence, and in the prompt the model just
                # read. Running it again spends a tool call to learn nothing, and a model
                # that keeps re-asking one settled question can burn a third of a budget
                # doing it.
                repeated += 1
                logger.info("evidence.already_gathered", tool=call.tool)
                continue

            already.add(signature)
            gathered.append(await self._toolbox.run(call.tool, arguments, context))

        return NodeUpdate(
            updates={
                "evidence": [*state.evidence, *gathered],
                "tool_calls_made": state.tool_calls_made + len(gathered),
                "iteration": state.iteration + 1,
                # A round that gathered nothing new has nothing left to give. Small models
                # re-request the same tools every iteration despite the prompt forbidding
                # it, and without this the loop spends its whole budget re-reading what it
                # already has and then escalates as though the failure were ambiguous.
                "evidence_exhausted": not gathered,
            },
            step_input={
                "requested": [call.tool for call in plan.value.calls],
                "allowed": allowed,
            },
            step_output={
                "ran": [item.tool_name for item in gathered],
                "succeeded": [item.tool_name for item in gathered if item.succeeded],
                "already_known": repeated,
                "new_evidence": len(gathered),
            },
            model_used=plan.model_used,
            prompt_tokens=plan.prompt_tokens,
            completion_tokens=plan.completion_tokens,
        )


def _fingerprint(arguments: Mapping[str, JsonValue]) -> str:
    """A stable identity for one set of tool arguments, order-independent."""
    return json.dumps(dict(arguments), sort_keys=True, default=str)


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
