"""One incident, from failure event to persisted diagnosis.

This is the phase's promise checked end to end: the real graph, the real repositories and
a real database, with only the model and the broker replaced. The headline scenario is the
seeded schema drift, and the assertions are the ones the README will claim.
"""

from uuid import uuid4

import pytest

from dag_doctor.core.models import (
    FailureEvent,
    HaltReason,
    HypothesisOutcome,
    IncidentStatus,
    RootCauseCategory,
)
from dag_doctor.core.settings import BudgetSettings, Settings
from dag_doctor.db.repositories import IncidentRepository, InvestigationRepository
from dag_doctor.messaging.producer import DiagnosisPublisher
from dag_doctor.messaging.schemas import DiagnosisCompletedMessage
from dag_doctor.worker.investigator import Investigator
from tests.fakes import (
    ScriptedCaller,
    conclusion,
    hypothesis_set,
    tool_plan,
    triage_answer,
    verdict,
)

from ..messaging.conftest import FakeProducer


def _script() -> dict[str, list]:
    return {
        "triage": [triage_answer()],
        "gather_evidence": [tool_plan("fetch_task_logs", "compare_schema_snapshot")],
        "form_hypothesis": [hypothesis_set()],
        "test_hypothesis": [verdict("confirmed")],
        "conclude": [conclusion()],
    }


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.fixture
def diagnosis_producer() -> FakeProducer:
    return FakeProducer()


async def _investigate(settings, session_factory, toolbox, event, script, producer=None):
    publisher = DiagnosisPublisher(settings.kafka, producer) if producer is not None else None
    caller = ScriptedCaller(script)
    async with Investigator(settings, session_factory, caller, toolbox, publisher) as service:
        diagnosis = await service.handle(event)
    return diagnosis, caller


async def test_the_schema_drift_failure_is_diagnosed_end_to_end(
    settings, session_factory, toolbox, failure_event, diagnosis_producer
):
    diagnosis, _caller = await _investigate(
        settings, session_factory, toolbox, failure_event, _script(), diagnosis_producer
    )

    assert diagnosis is not None
    assert diagnosis.root_cause_category is RootCauseCategory.SCHEMA_DRIFT
    assert diagnosis.is_conclusive
    assert diagnosis.confidence > 0.5
    assert diagnosis.proposed_fix is not None
    # The true culprit is the upstream task, not the one that visibly failed.
    assert diagnosis.responsible_task_id == "land_raw_orders"


async def test_the_incident_ends_up_marked_diagnosed(
    settings, session_factory, toolbox, failure_event
):
    await _investigate(settings, session_factory, toolbox, failure_event, _script())

    async with session_factory() as session:
        incidents = await IncidentRepository(session)._find(failure_event), None
        incident = incidents[0]

    assert incident is not None
    assert incident.status is IncidentStatus.DIAGNOSED


async def test_the_full_trace_is_persisted(settings, session_factory, toolbox, failure_event):
    # The "show your work" endpoint is only worth demonstrating if every node left a row.
    diagnosis, _caller = await _investigate(
        settings, session_factory, toolbox, failure_event, _script()
    )

    async with session_factory() as session:
        repository = InvestigationRepository(session)
        steps = await repository.steps(diagnosis.incident_id)
        evidence = await repository.evidence(diagnosis.incident_id)
        hypotheses = await repository.hypotheses(diagnosis.incident_id)

    assert [step.node for step in steps] == [
        "triage",
        "gather_evidence",
        "form_hypothesis",
        "test_hypothesis",
        "conclude",
    ]
    assert [step.sequence for step in steps] == [1, 2, 3, 4, 5]
    assert len(evidence) == 3
    assert hypotheses[0].outcome is HypothesisOutcome.CONFIRMED


async def test_the_trace_records_token_spend_per_node(
    settings, session_factory, toolbox, failure_event
):
    diagnosis, _caller = await _investigate(
        settings, session_factory, toolbox, failure_event, _script()
    )

    async with session_factory() as session:
        steps = await InvestigationRepository(session).steps(diagnosis.incident_id)

    assert sum(step.prompt_tokens for step in steps) == 55
    assert all(step.model_used == "scripted-model" for step in steps)


async def test_the_finished_investigation_is_announced(
    settings, session_factory, toolbox, failure_event, diagnosis_producer
):
    await _investigate(
        settings, session_factory, toolbox, failure_event, _script(), diagnosis_producer
    )

    topic, value, _key = diagnosis_producer.records[0]
    message = DiagnosisCompletedMessage.model_validate_json(value)
    assert topic == "agent.diagnosis.completed"
    assert message.root_cause_category is RootCauseCategory.SCHEMA_DRIFT
    assert message.conclusive is True
    assert message.attempt == 1


async def test_a_broker_that_will_not_take_the_announcement_does_not_lose_the_diagnosis(
    settings, session_factory, toolbox, failure_event
):
    # The database copy is the one that matters. Failing the investigation because an
    # optional announcement did not land would be the wrong trade.
    diagnosis, _caller = await _investigate(
        settings,
        session_factory,
        toolbox,
        failure_event,
        _script(),
        FakeProducer(fail_on_send=True),
    )

    assert diagnosis is not None
    async with session_factory() as session:
        assert await InvestigationRepository(session).latest_diagnosis(diagnosis.incident_id)


async def test_a_redelivered_failure_is_not_investigated_twice(
    settings, session_factory, toolbox, failure_event
):
    # Kafka delivers at least once. Paying a model again to reach the same answer is
    # the cost this check exists to avoid.
    _first, _caller = await _investigate(
        settings, session_factory, toolbox, failure_event, _script()
    )

    second, caller = await _investigate(
        settings, session_factory, toolbox, failure_event, _script()
    )

    assert second is None
    assert caller.calls == []


async def test_an_incident_that_was_never_diagnosed_is_investigated_on_redelivery(
    settings, session_factory, toolbox, failure_event
):
    from dag_doctor.graph.nodes.form_hypothesis import HypothesisSet

    stalled = {
        "triage": [triage_answer()],
        "gather_evidence": [tool_plan("fetch_task_logs")],
        "form_hypothesis": [HypothesisSet(hypotheses=[])],
    }
    first, _caller = await _investigate(settings, session_factory, toolbox, failure_event, stalled)
    assert first.halt_reason is HaltReason.NO_HYPOTHESIS_FORMED

    second, caller = await _investigate(
        settings, session_factory, toolbox, failure_event, _script()
    )

    # Inconclusive is terminal too: rerunning would produce the same answer.
    assert second is None
    assert caller.calls == []


async def test_an_inconclusive_investigation_is_persisted_and_marked(
    settings, session_factory, toolbox, failure_event
):
    from dag_doctor.graph.nodes.form_hypothesis import HypothesisSet

    # Two iterations, so the budget survives one gather and the empty hypothesis set is
    # what actually ends the investigation.
    tight = Settings()
    tight.budgets = BudgetSettings(max_iterations=2)
    diagnosis, _caller = await _investigate(
        tight,
        session_factory,
        toolbox,
        failure_event,
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs")],
            "form_hypothesis": [HypothesisSet(hypotheses=[])],
        },
    )

    assert diagnosis.root_cause_category is RootCauseCategory.UNKNOWN
    assert not diagnosis.is_conclusive

    async with session_factory() as session:
        incident = await IncidentRepository(session).get(diagnosis.incident_id)
        stored = await InvestigationRepository(session).latest_diagnosis(diagnosis.incident_id)

    assert incident.status is IncidentStatus.INCONCLUSIVE
    assert stored.halt_reason == "no_hypothesis_formed"


async def test_a_second_failure_of_the_same_task_is_its_own_investigation(
    settings, session_factory, toolbox, failure_event
):
    retry = failure_event.model_copy(update={"try_number": 2})

    first, _c1 = await _investigate(settings, session_factory, toolbox, failure_event, _script())
    second, _c2 = await _investigate(settings, session_factory, toolbox, retry, _script())

    assert first is not None
    assert second is not None
    assert first.incident_id != second.incident_id


async def test_a_replay_is_recorded_beside_the_original(
    settings, session_factory, toolbox, failure_event
):
    diagnosis, _caller = await _investigate(
        settings, session_factory, toolbox, failure_event, _script()
    )
    caller = ScriptedCaller(_script(), model_used="a-different-model")

    async with Investigator(settings, session_factory, caller, toolbox) as service:
        replayed = await service.investigate(diagnosis.incident_id, failure_event)

    assert replayed is not None
    async with session_factory() as session:
        repository = InvestigationRepository(session)
        assert len(await repository.steps(diagnosis.incident_id)) == 10
        assert (await repository.latest_diagnosis(diagnosis.incident_id)).attempt == 2


async def test_replaying_an_unknown_incident_is_refused(settings, session_factory, toolbox):
    from dag_doctor.core.exceptions import ResourceNotFoundError

    event = FailureEvent(dag_id="d", task_id="t", run_id="r", try_number=1)
    caller = ScriptedCaller(_script())

    async with Investigator(settings, session_factory, caller, toolbox) as service:
        with pytest.raises(ResourceNotFoundError):
            await service.investigate(uuid4(), event)


async def test_a_graph_that_cannot_run_marks_the_incident_failed_not_inconclusive(
    settings, session_factory, toolbox, failure_event, monkeypatch
):
    # An investigation that broke is an operational problem. Recording it as merely
    # inconclusive would hide a broken agent inside its ordinary uncertainty.
    from dag_doctor.core.exceptions import PersistenceError

    caller = ScriptedCaller(_script())
    async with Investigator(settings, session_factory, caller, toolbox) as service:

        async def explode(*_args, **_kwargs):
            raise PersistenceError("the checkpointer is unreachable")

        monkeypatch.setattr(service._graph, "ainvoke", explode)
        diagnosis = await service.handle(failure_event)

    assert diagnosis is not None
    assert diagnosis.halt_reason is HaltReason.INVESTIGATION_ERROR
    assert not diagnosis.is_conclusive
    assert "checkpointer is unreachable" in diagnosis.summary

    async with session_factory() as session:
        incident = await IncidentRepository(session).get(diagnosis.incident_id)
        stored = await InvestigationRepository(session).latest_diagnosis(diagnosis.incident_id)

    assert incident.status is IncidentStatus.FAILED
    assert stored.halt_reason == "investigation_error"


async def test_an_investigation_with_no_broker_still_runs(
    settings, session_factory, toolbox, failure_event
):
    # The database copy is the record. A deployment without the diagnosis topic should
    # still diagnose.
    caller = ScriptedCaller(_script())

    async with Investigator(settings, session_factory, caller, toolbox, None) as service:
        diagnosis = await service.handle(failure_event)

    assert diagnosis is not None
    assert diagnosis.is_conclusive
