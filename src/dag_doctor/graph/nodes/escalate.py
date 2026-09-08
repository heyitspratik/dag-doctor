"""Escalating: stop honestly.

No model call. The budget is exhausted or nothing survived its test, and spending more
tokens to write a nicer summary of having failed would defeat the budget that brought us
here.

The category is deliberately ``unknown`` even when a signature matched. An inconclusive
result that names a plausible cause reads as an answer, and would score as one. What was
suspected goes in the summary, where a human can weigh it, and what is missing goes in
unknowns, which is the more useful half.
"""

from typing import ClassVar

from dag_doctor.core.models import (
    Diagnosis,
    HaltReason,
    HypothesisOutcome,
    NodeName,
    RootCauseCategory,
)
from dag_doctor.graph.confidence import compute_confidence
from dag_doctor.graph.nodes.base import InvestigationNode, NodeUpdate
from dag_doctor.graph.routing import halt_reason_for_route
from dag_doctor.graph.state import InvestigationState


class EscalateNode(InvestigationNode):
    """Emit an inconclusive diagnosis saying what was found and what was not."""

    node: ClassVar[NodeName] = "escalate"

    async def run(self, state: InvestigationState) -> NodeUpdate:
        """Summarise the investigation's dead end, in code."""
        # The same function the router used to get here, so the reason recorded on the
        # diagnosis cannot drift from the reason the edge was taken.
        reason = halt_reason_for_route(state)
        breakdown = compute_confidence(state)
        unknowns = [*state.unknowns, *_open_questions(state, reason)]

        diagnosis = Diagnosis(
            incident_id=state.incident_id,
            root_cause_category=RootCauseCategory.UNKNOWN,
            summary=_summarise(state, reason),
            confidence=breakdown.total,
            halt_reason=reason,
            evidence_chain=[item.id for item in state.successful_evidence],
            proposed_fix=None,
            responsible_dag_id=None,
            responsible_task_id=None,
            unknowns=unknowns,
            model_used=state.model_used,
        )
        return NodeUpdate(
            updates={"diagnosis": diagnosis, "halt_reason": reason, "unknowns": unknowns},
            step_input={"iteration": state.iteration, "tool_calls_made": state.tool_calls_made},
            step_output={"halt_reason": reason.value, "confidence": breakdown.total},
        )


def _summarise(state: InvestigationState, reason: HaltReason) -> str:
    """Say plainly what happened, including the leading suspicion."""
    parts = [
        f"Inconclusive: {reason.value.replace('_', ' ')} after {state.iteration} "
        f"iteration(s) and {state.tool_calls_made} tool call(s)."
    ]
    if state.triage is not None:
        parts.append(
            f"Triage suspected {state.triage.category.value} "
            f"({state.triage.rationale}), which was not established."
        )
    leading = state.current_hypothesis
    if leading is not None and leading.outcome is not HypothesisOutcome.CONFIRMED:
        parts.append(f"Leading untested explanation: {leading.statement}.")
    refuted = state.refuted_hypotheses
    if refuted:
        parts.append(f"Ruled out: {'; '.join(item.statement for item in refuted)}.")
    if not state.successful_evidence:
        parts.append("No tool returned usable evidence.")
    return " ".join(parts)


def _open_questions(state: InvestigationState, reason: HaltReason) -> list[str]:
    """What a human would need to look at next."""
    questions: list[str] = []
    if reason is HaltReason.ITERATION_BUDGET_EXHAUSTED:
        questions.append(
            f"The investigation used all {state.max_iterations} iterations without "
            f"confirming a cause; raising MAX_ITERATIONS may help on this failure."
        )
    if reason is HaltReason.TOOL_CALL_BUDGET_EXHAUSTED:
        questions.append(
            f"The investigation used all {state.max_tool_calls} tool calls; "
            f"raising MAX_TOOL_CALLS may help on this failure."
        )
    if reason is HaltReason.INVESTIGATION_ERROR:
        questions.append("The investigation stopped on an internal error; see the step trace.")
    if reason is HaltReason.NO_HYPOTHESIS_FORMED:
        questions.append("No candidate cause could be formed from the evidence gathered.")
    failed = {item.tool_name for item in state.evidence if not item.succeeded}
    if failed:
        questions.append(f"These tools could not answer: {', '.join(sorted(failed))}.")
    return questions
