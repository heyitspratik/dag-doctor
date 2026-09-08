"""Persisting a finished investigation, and reading it back."""

import pytest

from dag_doctor.core.models import (
    Diagnosis,
    Evidence,
    HaltReason,
    Hypothesis,
    HypothesisOutcome,
    IncidentStatus,
    RootCauseCategory,
)
from dag_doctor.core.settings import BudgetSettings
from dag_doctor.db.repositories import IncidentRepository, InvestigationRepository
from dag_doctor.db.session import session_scope
from dag_doctor.graph.builder import initial_state
from dag_doctor.graph.state import StepRecord


def _finished_state(incident_id, failure):
    state = initial_state(incident_id, failure, BudgetSettings())
    evidence = [
        Evidence(tool_name="fetch_task_logs", summary="UndefinedColumn on customer_id"),
        Evidence(tool_name="profile_table", summary="unavailable", succeeded=False),
    ]
    hypothesis = Hypothesis(
        statement="orders.customer_id was renamed to customer_uuid",
        root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
        proposed_test="diff the schema against the stored snapshot",
        test_tool="compare_schema_snapshot",
        test_arguments={"connection": "warehouse", "table": "orders"},
        outcome=HypothesisOutcome.CONFIRMED,
        test_notes="the diff shows the rename",
        supporting_evidence_ids=[evidence[0].id],
        responsible_task_id="land_raw_orders",
    )
    return state.model_copy(
        update={
            "evidence": evidence,
            "hypotheses": [hypothesis],
            "current_hypothesis": hypothesis,
            "steps": [
                StepRecord(node="triage", sequence=1, output={"category": "schema_drift"}),
                StepRecord(node="conclude", sequence=2, model_used="scripted", prompt_tokens=9),
            ],
            "diagnosis": Diagnosis(
                incident_id=incident_id,
                root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
                summary="customer_id was renamed to customer_uuid upstream",
                confidence=0.86,
                evidence_chain=[evidence[0].id],
                proposed_fix="select customer_uuid",
                responsible_task_id="land_raw_orders",
                model_used="scripted",
            ),
        }
    )


@pytest.fixture
async def incident_id(session_factory, failure_event):
    async with session_scope(session_factory) as session:
        incident, _created = await IncidentRepository(session).get_or_create(failure_event)
        return incident.id


async def test_a_finished_investigation_is_stored_whole(
    session_factory, incident_id, failure_event
):
    state = _finished_state(incident_id, failure_event)

    async with session_scope(session_factory) as session:
        await InvestigationRepository(session).save(state, attempt=1)

    async with session_factory() as session:
        repository = InvestigationRepository(session)
        assert len(await repository.steps(incident_id)) == 2
        assert len(await repository.evidence(incident_id)) == 2
        assert len(await repository.hypotheses(incident_id)) == 1
        assert (await repository.latest_diagnosis(incident_id)) is not None


async def test_a_failed_tool_call_is_kept_as_evidence(session_factory, incident_id, failure_event):
    # Knowing that a tool could not answer is part of the record. Dropping it would make
    # the trace look tidier than the investigation actually was.
    async with session_scope(session_factory) as session:
        await InvestigationRepository(session).save(
            _finished_state(incident_id, failure_event), attempt=1
        )

    async with session_factory() as session:
        stored = await InvestigationRepository(session).evidence(incident_id)

    assert [item.succeeded for item in stored] == [True, False]


async def test_the_hypothesis_keeps_the_call_that_tested_it(
    session_factory, incident_id, failure_event
):
    async with session_scope(session_factory) as session:
        await InvestigationRepository(session).save(
            _finished_state(incident_id, failure_event), attempt=1
        )

    async with session_factory() as session:
        stored = (await InvestigationRepository(session).hypotheses(incident_id))[0]

    assert stored.test_call["tool"] == "compare_schema_snapshot"
    assert stored.test_call["arguments"]["table"] == "orders"
    assert stored.outcome is HypothesisOutcome.CONFIRMED


async def test_the_diagnosis_keeps_its_evidence_chain(session_factory, incident_id, failure_event):
    state = _finished_state(incident_id, failure_event)

    async with session_scope(session_factory) as session:
        await InvestigationRepository(session).save(state, attempt=1)

    async with session_factory() as session:
        stored = await InvestigationRepository(session).latest_diagnosis(incident_id)

    assert stored.confidence == 0.86
    assert stored.evidence_chain == [str(state.evidence[0].id)]
    assert stored.responsible_task_id == "land_raw_orders"


async def test_a_replay_sits_beside_the_run_it_is_compared_with(
    session_factory, incident_id, failure_event
):
    # Overwriting would defeat the point of replay, which is comparing a changed prompt
    # or a different model against what the original run actually did.
    state = _finished_state(incident_id, failure_event)

    async with session_scope(session_factory) as session:
        repository = InvestigationRepository(session)
        assert await repository.next_attempt(incident_id) == 1
        await repository.save(state, attempt=1)

    async with session_scope(session_factory) as session:
        repository = InvestigationRepository(session)
        second = await repository.next_attempt(incident_id)
        assert second == 2
        await repository.save(_finished_state(incident_id, failure_event), attempt=second)

    async with session_factory() as session:
        repository = InvestigationRepository(session)
        assert len(await repository.steps(incident_id)) == 4
        assert len(await repository.steps(incident_id, attempt=2)) == 2
        assert (await repository.latest_diagnosis(incident_id)).attempt == 2


async def test_a_replay_continues_the_step_numbering(session_factory, incident_id, failure_event):
    async with session_scope(session_factory) as session:
        await InvestigationRepository(session).save(
            _finished_state(incident_id, failure_event), attempt=1
        )
    async with session_scope(session_factory) as session:
        await InvestigationRepository(session).save(
            _finished_state(incident_id, failure_event), attempt=2
        )

    async with session_factory() as session:
        sequences = [
            step.sequence for step in await InvestigationRepository(session).steps(incident_id)
        ]

    assert sequences == [1, 2, 3, 4]


async def test_an_investigation_that_produced_no_diagnosis_still_leaves_its_trace(
    session_factory, incident_id, failure_event
):
    state = _finished_state(incident_id, failure_event).model_copy(update={"diagnosis": None})

    async with session_scope(session_factory) as session:
        stored = await InvestigationRepository(session).save(state, attempt=1)

    assert stored is None
    async with session_factory() as session:
        assert len(await InvestigationRepository(session).steps(incident_id)) == 2


async def test_an_incident_never_investigated_has_no_diagnosis(session_factory, incident_id):
    async with session_factory() as session:
        assert await InvestigationRepository(session).latest_diagnosis(incident_id) is None


async def test_an_inconclusive_diagnosis_records_its_halt_reason(
    session_factory, incident_id, failure_event
):
    state = _finished_state(incident_id, failure_event)
    state = state.model_copy(
        update={
            "diagnosis": state.diagnosis.model_copy(
                update={
                    "halt_reason": HaltReason.ITERATION_BUDGET_EXHAUSTED,
                    "root_cause_category": RootCauseCategory.UNKNOWN,
                }
            )
        }
    )

    async with session_scope(session_factory) as session:
        await InvestigationRepository(session).save(state, attempt=1)

    async with session_factory() as session:
        stored = await InvestigationRepository(session).latest_diagnosis(incident_id)

    assert stored.halt_reason == "iteration_budget_exhausted"
    assert stored.root_cause_category is RootCauseCategory.UNKNOWN


async def test_a_failed_unit_of_work_leaves_no_partial_investigation(
    session_factory, incident_id, failure_event
):
    # A diagnosis whose evidence is missing would be worse than no diagnosis at all.
    with pytest.raises(RuntimeError):
        async with session_scope(session_factory) as session:
            await InvestigationRepository(session).save(
                _finished_state(incident_id, failure_event), attempt=1
            )
            raise RuntimeError("the worker died here")

    async with session_factory() as session:
        repository = InvestigationRepository(session)
        assert await repository.steps(incident_id) == []
        assert await repository.evidence(incident_id) == []
        assert await repository.latest_diagnosis(incident_id) is None


async def test_the_incident_lifecycle_is_independent_of_the_trace(
    session_factory, incident_id, failure_event
):
    async with session_scope(session_factory) as session:
        incident = await IncidentRepository(session).get(incident_id)
        assert incident.status is IncidentStatus.RECEIVED
