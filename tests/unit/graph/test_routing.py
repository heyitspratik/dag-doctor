"""The conditional edges, driven directly.

Routing is pure, so every decision can be checked without running a node. This is where
the loop, the shortcuts and the budget enforcement are pinned down, and it is the file to
read first to understand how the investigation moves.
"""

import pytest

from dag_doctor.core.models import (
    Evidence,
    HaltReason,
    Hypothesis,
    HypothesisOutcome,
    RootCauseCategory,
)
from dag_doctor.core.settings import BudgetSettings
from dag_doctor.graph.builder import initial_state
from dag_doctor.graph.routing import (
    after_form_hypothesis,
    after_gather_evidence,
    after_test_hypothesis,
    after_triage,
    halt_reason_for_route,
)
from dag_doctor.graph.state import InvestigationState, TriageResult


def _state(failure_event, budgets: BudgetSettings, **overrides) -> InvestigationState:
    state = initial_state(__import__("uuid").uuid4(), failure_event, budgets)
    return state.model_copy(update=overrides)


def _triage(confidence: float, needs_investigation: bool = True) -> TriageResult:
    return TriageResult(
        category=RootCauseCategory.SCHEMA_DRIFT,
        confidence=confidence,
        rationale="known signature",
        needs_investigation=needs_investigation,
    )


def _hypothesis(outcome: HypothesisOutcome) -> Hypothesis:
    return Hypothesis(
        statement="a column was renamed",
        root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
        proposed_test="diff the schema",
        outcome=outcome,
    )


def _evidence(*tools: str) -> list[Evidence]:
    return [Evidence(tool_name=tool, summary=f"{tool} found something") for tool in tools]


def test_an_unmistakable_signature_skips_the_investigation(failure_event, budgets):
    state = _state(failure_event, budgets, triage=_triage(0.95, needs_investigation=False))

    assert after_triage(state, budgets) == "conclude"


def test_a_confident_signature_the_model_still_wants_investigated_is_investigated(
    failure_event, budgets
):
    # Both sides must agree to skip. A model that says more evidence would help is
    # telling us something, and overriding it to save one iteration is a false economy.
    state = _state(failure_event, budgets, triage=_triage(0.95, needs_investigation=True))

    assert after_triage(state, budgets) == "gather_evidence"


def test_an_ambiguous_signature_is_investigated_however_sure_the_model_sounds(
    failure_event, budgets
):
    state = _state(failure_event, budgets, triage=_triage(0.4, needs_investigation=False))

    assert after_triage(state, budgets) == "gather_evidence"


def test_triage_with_no_result_at_all_investigates(failure_event, budgets):
    assert after_triage(_state(failure_event, budgets), budgets) == "gather_evidence"


def test_a_failure_inside_triage_escalates_rather_than_investigating(failure_event, budgets):
    state = _state(failure_event, budgets, halt_reason=HaltReason.INVESTIGATION_ERROR)

    assert after_triage(state, budgets) == "escalate"


def test_gathering_leads_to_forming_a_hypothesis(failure_event, budgets):
    state = _state(failure_event, budgets, iteration=1, tool_calls_made=2)

    assert after_gather_evidence(state) == "form_hypothesis"


@pytest.mark.parametrize(
    "overrides",
    [{"iteration": 5}, {"tool_calls_made": 20}],
)
def test_a_budget_that_runs_out_while_gathering_escalates(failure_event, budgets, overrides):
    state = _state(failure_event, budgets, **overrides)

    assert after_gather_evidence(state) == "escalate"


def test_a_hypothesis_is_tested_once_it_is_formed(failure_event, budgets):
    state = _state(
        failure_event, budgets, current_hypothesis=_hypothesis(HypothesisOutcome.UNTESTED)
    )

    assert after_form_hypothesis(state) == "test_hypothesis"


def test_forming_nothing_escalates_rather_than_testing_nothing(failure_event, budgets):
    assert after_form_hypothesis(_state(failure_event, budgets)) == "escalate"


def test_a_confirmed_hypothesis_with_real_support_concludes(failure_event, budgets):
    state = _state(
        failure_event,
        budgets,
        current_hypothesis=_hypothesis(HypothesisOutcome.CONFIRMED),
        evidence=_evidence("fetch_task_logs", "compare_schema_snapshot", "get_dag_run_history"),
    )

    assert after_test_hypothesis(state, budgets) == "conclude"


def test_a_confirmed_hypothesis_nothing_supports_escalates(failure_event, budgets):
    # Confirmation by a single weak test is not a diagnosis. Escalating here is the
    # difference between an agent that reports uncertainty and one that manufactures
    # certainty from one lucky tool call.
    strict = BudgetSettings(min_conclude_confidence=0.9)
    state = _state(
        failure_event, strict, current_hypothesis=_hypothesis(HypothesisOutcome.CONFIRMED)
    )

    assert after_test_hypothesis(state, strict) == "escalate"


def test_a_refuted_hypothesis_goes_back_for_more_evidence(failure_event, budgets):
    # The back edge. This cycle is the whole reason the investigation is a state machine
    # rather than a chain, and it is the edge most worth protecting with a test.
    state = _state(
        failure_event,
        budgets,
        iteration=1,
        current_hypothesis=_hypothesis(HypothesisOutcome.REFUTED),
    )

    assert after_test_hypothesis(state, budgets) == "gather_evidence"


def test_an_inconclusive_test_also_goes_back_for_more_evidence(failure_event, budgets):
    state = _state(
        failure_event,
        budgets,
        iteration=1,
        current_hypothesis=_hypothesis(HypothesisOutcome.INCONCLUSIVE),
    )

    assert after_test_hypothesis(state, budgets) == "gather_evidence"


@pytest.mark.parametrize(
    "overrides",
    [{"iteration": 5}, {"tool_calls_made": 20}],
)
def test_a_refuted_hypothesis_with_no_budget_left_escalates(failure_event, budgets, overrides):
    state = _state(
        failure_event,
        budgets,
        current_hypothesis=_hypothesis(HypothesisOutcome.REFUTED),
        **overrides,
    )

    assert after_test_hypothesis(state, budgets) == "escalate"


def test_the_two_budgets_are_reported_separately(failure_event, budgets):
    # Out of iterations means an ambiguous failure; out of tool calls means an expensive
    # one. Collapsing them would hide which knob to turn.
    out_of_iterations = _state(failure_event, budgets, iteration=5)
    out_of_calls = _state(failure_event, budgets, tool_calls_made=20)

    assert halt_reason_for_route(out_of_iterations) is HaltReason.ITERATION_BUDGET_EXHAUSTED
    assert halt_reason_for_route(out_of_calls) is HaltReason.TOOL_CALL_BUDGET_EXHAUSTED


def test_an_investigation_error_keeps_its_own_reason(failure_event, budgets):
    state = _state(failure_event, budgets, halt_reason=HaltReason.INVESTIGATION_ERROR)

    assert halt_reason_for_route(state) is HaltReason.INVESTIGATION_ERROR


def test_having_formed_nothing_is_reported_as_such(failure_event, budgets):
    assert halt_reason_for_route(_state(failure_event, budgets)) is HaltReason.NO_HYPOTHESIS_FORMED


def test_a_tested_hypothesis_that_did_not_convince_is_a_confidence_problem(failure_event, budgets):
    state = _state(
        failure_event, budgets, current_hypothesis=_hypothesis(HypothesisOutcome.REFUTED)
    )

    assert halt_reason_for_route(state) is HaltReason.CONFIDENCE_TOO_LOW
