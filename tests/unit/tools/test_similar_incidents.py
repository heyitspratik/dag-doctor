from datetime import UTC, datetime, timedelta

import pytest

from dag_doctor.core.models import RootCauseCategory
from dag_doctor.db.models import DiagnosisRecord, Incident
from dag_doctor.tools.similar_incidents import (
    MIN_SIMILARITY,
    SearchSimilarIncidents,
    jaccard,
    normalise_signature,
    signature_tokens,
)

DRIFT_ERROR = 'psycopg2.errors.UndefinedColumn: column "customer_id" does not exist'


async def _seed(session_factory, *, diagnosed: bool = True, verdict: bool | None = None):
    async with session_factory() as session:
        incident = Incident(
            dag_id="schema_drift_orders",
            task_id="build_orders_by_customer",
            run_id="scheduled__2026-08-01",
            try_number=1,
            map_index=-1,
            failed_at=datetime.now(UTC) - timedelta(days=7),
            received_at=datetime.now(UTC) - timedelta(days=7),
            exception_type="psycopg2.errors.UndefinedColumn",
            exception_message='column "customer_id" does not exist',
        )
        session.add(incident)
        await session.flush()
        if diagnosed:
            session.add(
                DiagnosisRecord(
                    incident_id=incident.id,
                    root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
                    summary="orders.customer_id was renamed to customer_uuid upstream",
                    confidence=0.86,
                    halt_reason="concluded",
                    proposed_fix="Select customer_uuid, or restore the old name upstream",
                    human_verdict=verdict,
                )
            )
        await session.commit()
        return incident


def test_volatile_parts_of_an_error_are_stripped_before_comparing():
    # Two instances of the same failure differ in their row counts, timestamps and ids.
    left = normalise_signature("Timeout after 30.5s connecting to 10.0.0.7:5432")
    right = normalise_signature("Timeout after 120s connecting to 10.0.0.9:5432")

    assert left == right


def test_the_identifiers_that_name_a_failure_survive():
    tokens = signature_tokens(DRIFT_ERROR)

    assert "customer_id" in tokens
    assert "undefinedcolumn" in tokens


def test_words_common_to_every_traceback_carry_no_signal():
    assert "traceback" not in signature_tokens("Traceback (most recent call last)")


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        (frozenset("ab"), frozenset("ab"), 1.0),
        (frozenset("ab"), frozenset("cd"), 0.0),
        (frozenset(), frozenset("ab"), 0.0),
        (frozenset("abc"), frozenset("abd"), 0.5),
    ],
)
def test_overlap_is_scored_between_zero_and_one(left, right, expected):
    assert jaccard(left, right) == expected


async def test_the_same_failure_seen_before_is_found_with_its_diagnosis(session_factory):
    await _seed(session_factory)

    result = await SearchSimilarIncidents(session_factory).run(
        {"error_signature": DRIFT_ERROR, "exception_type": "psycopg2.errors.UndefinedColumn"}
    )

    assert result.ok
    match = result.data.matches[0]
    assert match.similarity == 1.0
    assert match.root_cause_category is RootCauseCategory.SCHEMA_DRIFT
    assert match.proposed_fix is not None


async def test_an_unrelated_failure_is_not_offered_as_a_match(session_factory):
    await _seed(session_factory)

    result = await SearchSimilarIncidents(session_factory).run(
        {"error_signature": "MemoryError: unable to allocate 4 GiB for the join buffer"}
    )

    assert result.data.matches == []
    assert "no past incident resembles" in result.data.summarise()


async def test_a_human_confirmed_diagnosis_is_marked_as_such(session_factory):
    # A past diagnosis nobody checked is a guess the agent made. Treating it as
    # settled evidence would let one early mistake compound across every later incident.
    await _seed(session_factory, verdict=True)

    result = await SearchSimilarIncidents(session_factory).run({"error_signature": DRIFT_ERROR})

    assert len(result.data.confirmed_matches) == 1
    assert "1 human-confirmed" in result.data.summarise()


async def test_an_unreviewed_diagnosis_is_not_counted_as_confirmed(session_factory):
    await _seed(session_factory, verdict=None)

    result = await SearchSimilarIncidents(session_factory).run({"error_signature": DRIFT_ERROR})

    assert result.data.matches
    assert result.data.confirmed_matches == []


async def test_an_incident_that_was_never_diagnosed_still_matches(session_factory):
    await _seed(session_factory, diagnosed=False)

    result = await SearchSimilarIncidents(session_factory).run({"error_signature": DRIFT_ERROR})

    match = result.data.matches[0]
    assert match.root_cause_category is None
    assert "undiagnosed" in match.summarise()


async def test_an_empty_history_is_answered_rather_than_failed(session_factory):
    result = await SearchSimilarIncidents(session_factory).run({"error_signature": DRIFT_ERROR})

    assert result.ok
    assert result.data.searched == 0


async def test_the_number_of_matches_returned_is_bounded(session_factory):
    for _ in range(4):
        async with session_factory() as session:
            incident = Incident(
                dag_id="d",
                task_id="t",
                run_id=f"run-{_}",
                try_number=1,
                map_index=-1,
                failed_at=datetime.now(UTC),
                received_at=datetime.now(UTC),
                exception_type="psycopg2.errors.UndefinedColumn",
                exception_message='column "customer_id" does not exist',
            )
            session.add(incident)
            await session.commit()

    result = await SearchSimilarIncidents(session_factory).run(
        {"error_signature": DRIFT_ERROR, "limit": 2}
    )

    assert len(result.data.matches) == 2
    assert result.data.searched == 4


async def test_weak_overlap_is_below_the_bar(session_factory):
    await _seed(session_factory)

    result = await SearchSimilarIncidents(session_factory).run(
        {"error_signature": "column does not"}
    )

    assert all(match.similarity >= MIN_SIMILARITY for match in result.data.matches)
