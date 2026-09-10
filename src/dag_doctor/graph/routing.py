"""The conditional edges.

Routing is pure: every decision is a function of the state and the budgets, with no side
effects and no model call, which is what makes the loop, the shortcuts and the budget
enforcement testable directly rather than only through a whole graph run.

The edge that matters is ``test_hypothesis -> gather_evidence``. A refuted hypothesis
sends the investigation back for more evidence, and that cycle is the reason this is a
state machine rather than a chain.
"""

from collections.abc import Callable
from typing import Literal

from dag_doctor.core.models import HaltReason, HypothesisOutcome
from dag_doctor.core.settings import BudgetSettings
from dag_doctor.graph.confidence import compute_confidence
from dag_doctor.graph.state import InvestigationState

type TriageRoute = Literal["conclude", "gather_evidence", "escalate"]
type GatherRoute = Literal["form_hypothesis", "conclude", "escalate"]
type FormRoute = Literal["test_hypothesis", "escalate"]
type TestRoute = Literal["conclude", "gather_evidence", "escalate"]


def _halted(state: InvestigationState) -> bool:
    """Whether something has already ended the investigation unsuccessfully."""
    return state.halt_reason is not None and state.halt_reason is not HaltReason.CONCLUDED


def after_triage(state: InvestigationState, budgets: BudgetSettings) -> TriageRoute:
    """Decide whether triage alone was enough.

    The shortcut requires the code-computed triage confidence to clear the bar and the
    model to have said no investigation is needed. Both, because skipping the
    investigation on a wrong classification produces a confident wrong answer, which is
    the worst thing this agent could do.

    Args:
        state: The investigation after triage.
        budgets: The configured limits and thresholds.

    Returns:
        The next node.
    """
    if _halted(state):
        return "escalate"
    triage = state.triage
    if (
        triage is not None
        and not triage.needs_investigation
        and triage.confidence >= budgets.triage_shortcut_confidence
    ):
        return "conclude"
    if state.budget_exhausted:
        return "escalate"
    return "gather_evidence"


def after_gather_evidence(state: InvestigationState) -> GatherRoute:
    """Move on to forming a hypothesis, unless the budget or the evidence ran out.

    A round that gathered nothing new ends the investigation on what it has. Looping again
    would re-run the same tools for the same answers, so the remaining budget buys nothing
    and spending it would report an ambiguous failure where there was really an exhausted
    one. Concluding here cannot manufacture certainty: the confidence is computed from the
    evidence, so a thin investigation still scores low and says so.

    Requires a hypothesis to already exist, so a first round that gathers nothing still
    gets its chance to form one rather than concluding from triage alone.

    Args:
        state: The investigation after a round of tool calls.

    Returns:
        The next node.
    """
    if _halted(state) or state.budget_exhausted:
        return "escalate"
    if state.evidence_exhausted and state.hypotheses:
        return "conclude"
    return "form_hypothesis"


def after_form_hypothesis(state: InvestigationState) -> FormRoute:
    """Test the leading hypothesis, unless none could be formed.

    Args:
        state: The investigation after hypotheses were proposed.

    Returns:
        The next node.
    """
    if _halted(state):
        return "escalate"
    if state.current_hypothesis is None:
        return "escalate"
    return "test_hypothesis"


def after_test_hypothesis(state: InvestigationState, budgets: BudgetSettings) -> TestRoute:
    """The loop.

    A confirmed hypothesis concludes, but only if the evidence behind it clears the
    confidence bar: a hypothesis confirmed by one weak test is not a diagnosis. A refuted
    one goes back for more evidence while budget remains, and that back edge is the cycle
    the whole design exists for. When the budget is gone, the investigation escalates
    rather than concluding on what it happens to have.

    Args:
        state: The investigation after a hypothesis was tested.
        budgets: The configured limits and thresholds.

    Returns:
        The next node.
    """
    if _halted(state):
        return "escalate"

    hypothesis = state.current_hypothesis
    if hypothesis is not None and hypothesis.outcome is HypothesisOutcome.CONFIRMED:
        if compute_confidence(state).total >= budgets.min_conclude_confidence:
            return "conclude"
        return "escalate"

    if state.budget_exhausted:
        return "escalate"
    return "gather_evidence"


def halt_reason_for_route(state: InvestigationState) -> HaltReason:
    """Why the investigation is being escalated, for the record.

    Args:
        state: The investigation at the point of escalation.

    Returns:
        The most specific reason that applies.
    """
    if state.halt_reason is not None and state.halt_reason is not HaltReason.CONCLUDED:
        return state.halt_reason
    exhausted = state.exhaustion_reason()
    if exhausted is not None:
        return exhausted
    if state.current_hypothesis is None:
        return HaltReason.NO_HYPOTHESIS_FORMED
    return HaltReason.CONFIDENCE_TOO_LOW


def triage_router(budgets: BudgetSettings) -> Callable[[InvestigationState], TriageRoute]:
    """Bind the triage decision to a set of budgets, for LangGraph's single-argument edges."""
    return lambda state: after_triage(state, budgets)


def test_router(budgets: BudgetSettings) -> Callable[[InvestigationState], TestRoute]:
    """Bind the test decision to a set of budgets, for LangGraph's single-argument edges."""
    return lambda state: after_test_hypothesis(state, budgets)
