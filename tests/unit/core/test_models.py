from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from dag_doctor.core.models import (
    Diagnosis,
    Evidence,
    FailureEvent,
    HaltReason,
    Hypothesis,
    HypothesisOutcome,
    RootCauseCategory,
)


def _failure(**overrides) -> FailureEvent:
    fields = {
        "dag_id": "schema_drift_orders",
        "task_id": "load_orders",
        "run_id": "manual__2026-09-07T00:00:00+00:00",
        "try_number": 1,
    }
    return FailureEvent(**{**fields, **overrides})


def test_idempotency_key_covers_the_whole_identifying_tuple():
    key = _failure().idempotency_key

    assert key == "schema_drift_orders/load_orders/manual__2026-09-07T00:00:00+00:00/1/-1"


def test_a_retry_is_a_different_incident_from_the_first_attempt():
    assert _failure(try_number=1).idempotency_key != _failure(try_number=2).idempotency_key


def test_mapped_task_instances_are_separate_incidents():
    assert _failure(map_index=0).idempotency_key != _failure(map_index=1).idempotency_key


def test_a_failure_event_is_immutable():
    event = _failure()

    with pytest.raises(ValidationError):
        event.dag_id = "something_else"


@pytest.mark.parametrize(
    "overrides",
    [{"dag_id": ""}, {"task_id": ""}, {"run_id": ""}, {"try_number": 0}],
)
def test_an_incomplete_failure_event_is_refused(overrides):
    with pytest.raises(ValidationError):
        _failure(**overrides)


def test_failed_at_defaults_to_an_aware_timestamp():
    assert _failure().failed_at.tzinfo is not None


def test_a_failed_tool_call_is_still_evidence():
    evidence = Evidence(
        tool_name="check_connection_health",
        result={"error": "timeout"},
        summary="connection probe timed out after 30s",
        succeeded=False,
    )

    assert evidence.succeeded is False
    assert evidence.collected_at.tzinfo is not None


def test_a_hypothesis_starts_untested():
    hypothesis = Hypothesis(
        statement="orders.customer_id was renamed to customer_uuid",
        root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
        proposed_test="compare the current orders schema against yesterday's snapshot",
    )

    assert hypothesis.outcome is HypothesisOutcome.UNTESTED


def test_a_concluded_diagnosis_reads_as_conclusive():
    diagnosis = Diagnosis(
        incident_id=uuid4(),
        root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
        summary="orders.customer_id was renamed to customer_uuid upstream",
        confidence=0.82,
        created_at=datetime.now(UTC),
    )

    assert diagnosis.is_conclusive


@pytest.mark.parametrize(
    ("category", "halt_reason"),
    [
        (RootCauseCategory.UNKNOWN, HaltReason.CONCLUDED),
        (RootCauseCategory.SCHEMA_DRIFT, HaltReason.ITERATION_BUDGET_EXHAUSTED),
        (RootCauseCategory.UNKNOWN, HaltReason.TOOL_CALL_BUDGET_EXHAUSTED),
    ],
)
def test_an_unknown_cause_or_an_exhausted_budget_is_not_conclusive(category, halt_reason):
    diagnosis = Diagnosis(
        incident_id=uuid4(),
        root_cause_category=category,
        summary="ran out of budget before a hypothesis survived its test",
        confidence=0.2,
        halt_reason=halt_reason,
    )

    assert not diagnosis.is_conclusive


@pytest.mark.parametrize("confidence", [-0.1, 1.1])
def test_confidence_outside_zero_to_one_is_refused(confidence):
    with pytest.raises(ValidationError):
        Diagnosis(
            incident_id=uuid4(),
            root_cause_category=RootCauseCategory.QUERY_DEFECT,
            summary="cartesian join",
            confidence=confidence,
        )


def test_root_cause_categories_are_stable_strings():
    # The database stores these as an enum and the scorer compares them, so the wire
    # values must not drift with a rename of the Python member.
    assert RootCauseCategory.SCHEMA_DRIFT == "schema_drift"
    assert RootCauseCategory("upstream_dependency_failure") is (
        RootCauseCategory.UPSTREAM_DEPENDENCY_FAILURE
    )
