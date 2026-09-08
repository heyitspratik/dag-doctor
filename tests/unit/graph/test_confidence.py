"""Confidence, which is computed rather than asked for.

A model asked how sure it is says ninety percent whether it is right or not. These tests
pin down what the number actually responds to.
"""

from uuid import uuid4

import pytest

from dag_doctor.core.models import Evidence, HypothesisOutcome, RootCauseCategory
from dag_doctor.graph.builder import initial_state
from dag_doctor.graph.confidence import (
    BASE_CONFIDENCE,
    compute_confidence,
    independent_tool_count,
    match_signature,
    triage_confidence,
)


def _state(failure, budgets, **overrides):
    return initial_state(uuid4(), failure, budgets).model_copy(update=overrides)


def _evidence(*tools: str, succeeded: bool = True) -> list[Evidence]:
    return [Evidence(tool_name=tool, summary="found", succeeded=succeeded) for tool in tools]


def _hypothesis(outcome: HypothesisOutcome):
    from dag_doctor.core.models import Hypothesis

    return Hypothesis(
        statement="a column was renamed",
        root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
        proposed_test="diff the schema",
        outcome=outcome,
    )


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ('column "customer_id" does not exist', RootCauseCategory.SCHEMA_DRIFT),
        (
            "null value in column violates not-null constraint",
            RootCauseCategory.DATA_QUALITY_REGRESSION,
        ),
        ("invalid input syntax for type integer", RootCauseCategory.TYPE_MISMATCH),
        ("connection refused", RootCauseCategory.TRANSIENT_INFRASTRUCTURE),
        ("MemoryError", RootCauseCategory.RESOURCE_EXHAUSTION),
        ('relation "raw.orders" does not exist', RootCauseCategory.MISSING_UPSTREAM_DATA),
    ],
)
def test_known_failure_patterns_are_recognised(message, expected):
    signature = match_signature(None, message)

    assert signature is not None
    assert signature.category is expected


def test_an_unrecognised_message_matches_nothing():
    assert match_signature("ValueError", "the vibes were off") is None


def test_an_empty_failure_matches_nothing():
    assert match_signature(None, None) is None


def test_an_ambiguous_pattern_scores_lower_than_a_decisive_one():
    # "relation does not exist" could be a rename or an upstream job that never ran, so
    # it must not be decisive enough to skip the investigation.
    decisive = match_signature(None, 'column "x" does not exist')
    ambiguous = match_signature(None, 'relation "x" does not exist')

    assert ambiguous.strength < decisive.strength


def test_triage_needs_both_a_pattern_and_an_agreeing_model():
    signature = match_signature(None, 'column "x" does not exist')

    assert triage_confidence(signature, model_agrees=True) == signature.strength
    assert triage_confidence(signature, model_agrees=False) < signature.strength
    assert triage_confidence(None, model_agrees=True) == 0.0


def test_an_investigation_that_found_nothing_scores_low(drift_failure, budgets):
    breakdown = compute_confidence(_state(drift_failure, budgets))

    # The signature alone contributes; having merely run does not.
    assert breakdown.evidence_support == 0.0
    assert breakdown.hypothesis_test == 0.0
    assert breakdown.total < 0.4


def test_repeating_one_tool_is_not_independent_support(drift_failure, budgets):
    # Three calls to one tool is one line of argument retold. Counting it as three would
    # let a confident investigation manufacture its own support.
    repeated = _state(drift_failure, budgets, evidence=_evidence("fetch_task_logs") * 3)
    varied = _state(
        drift_failure,
        budgets,
        evidence=_evidence("fetch_task_logs", "compare_schema_snapshot", "get_dag_run_history"),
    )

    assert independent_tool_count(repeated) == 1
    assert independent_tool_count(varied) == 3
    assert compute_confidence(varied).total > compute_confidence(repeated).total


def test_a_tool_that_failed_does_not_support_a_conclusion(drift_failure, budgets):
    state = _state(drift_failure, budgets, evidence=_evidence("profile_table", succeeded=False))

    assert compute_confidence(state).evidence_support == 0.0


def test_surviving_a_test_counts_for_more_than_never_being_tested(drift_failure, budgets):
    confirmed = _state(
        drift_failure, budgets, current_hypothesis=_hypothesis(HypothesisOutcome.CONFIRMED)
    )
    untested = _state(
        drift_failure, budgets, current_hypothesis=_hypothesis(HypothesisOutcome.UNTESTED)
    )

    assert compute_confidence(confirmed).hypothesis_test > 0
    assert compute_confidence(untested).hypothesis_test == 0


def test_a_test_that_decided_nothing_counts_for_a_little(drift_failure, budgets):
    inconclusive = _state(
        drift_failure, budgets, current_hypothesis=_hypothesis(HypothesisOutcome.INCONCLUSIVE)
    )
    confirmed = _state(
        drift_failure, budgets, current_hypothesis=_hypothesis(HypothesisOutcome.CONFIRMED)
    )

    score = compute_confidence(inconclusive).hypothesis_test
    assert 0 < score < compute_confidence(confirmed).hypothesis_test


def test_a_cause_found_on_the_fourth_attempt_is_worth_less_than_one_found_first(
    drift_failure, budgets
):
    first = _state(
        drift_failure,
        budgets,
        current_hypothesis=_hypothesis(HypothesisOutcome.CONFIRMED),
        hypotheses=[_hypothesis(HypothesisOutcome.CONFIRMED)],
    )
    fourth = _state(
        drift_failure,
        budgets,
        current_hypothesis=_hypothesis(HypothesisOutcome.CONFIRMED),
        hypotheses=[_hypothesis(HypothesisOutcome.REFUTED) for _ in range(3)],
    )

    assert compute_confidence(fourth).total < compute_confidence(first).total


def test_the_refutation_penalty_is_capped(drift_failure, budgets):
    many = _state(
        drift_failure,
        budgets,
        hypotheses=[_hypothesis(HypothesisOutcome.REFUTED) for _ in range(20)],
    )

    assert compute_confidence(many).refutation_penalty == -0.15


def test_a_thorough_investigation_that_confirms_its_hypothesis_scores_high(drift_failure, budgets):
    state = _state(
        drift_failure,
        budgets,
        current_hypothesis=_hypothesis(HypothesisOutcome.CONFIRMED),
        hypotheses=[_hypothesis(HypothesisOutcome.CONFIRMED)],
        evidence=_evidence(
            "fetch_task_logs", "compare_schema_snapshot", "get_dag_run_history", "get_dag_source"
        ),
    )

    assert compute_confidence(state).total > 0.9


def test_confidence_never_leaves_the_unit_interval(drift_failure, budgets):
    state = _state(
        drift_failure,
        budgets,
        current_hypothesis=_hypothesis(HypothesisOutcome.CONFIRMED),
        evidence=_evidence(*[f"tool_{index}" for index in range(50)]),
    )

    assert 0.0 <= compute_confidence(state).total <= 1.0


def test_the_breakdown_shows_its_working(drift_failure, budgets):
    # A number nobody can argue with is a number nobody should trust.
    explanation = compute_confidence(_state(drift_failure, budgets)).explain()

    assert f"{BASE_CONFIDENCE:.2f} base" in explanation
    assert "signature" in explanation
    assert "refutations" in explanation
