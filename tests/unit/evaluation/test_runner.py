"""The harness itself, driven against a fake Airflow and a fake agent API.

A harness that silently reports zero because it could not reach anything would be worse
than no harness, so the failure modes are tested as carefully as the happy path.
"""

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from dag_doctor.core.models import RootCauseCategory
from dag_doctor.core.settings import Settings
from dag_doctor.evaluation.runner import (
    Clients,
    EvaluationError,
    await_diagnosis,
    build_clients,
    evaluate,
    seed,
    trigger,
)
from dag_doctor.evaluation.scenarios import SCENARIOS, by_dag_id

DRIFT = by_dag_id("schema_drift_orders")
CONTROL = by_dag_id("healthy_baseline")


class FakeStack:
    """Stands in for Airflow and the agent's API together."""

    def __init__(self, *, diagnose: dict[str, dict] | None = None) -> None:
        self.triggered: list[tuple[str, str]] = []
        self.diagnose = diagnose or {}
        self.received_at = datetime.now(UTC) + timedelta(seconds=1)
        self.airflow_status = 200

    def airflow(self, request: httpx.Request) -> httpx.Response:
        dag_id = str(request.url.path).split("/dags/")[1].split("/")[0]
        self.triggered.append((dag_id, str(request.url)))
        return httpx.Response(self.airflow_status, json={"dag_run_id": "eval"})

    def agent(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/incidents":
            dag_id = request.url.params.get("dag_id", "")
            if dag_id not in self.diagnose:
                return httpx.Response(200, json={"items": [], "next_cursor": None})
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"id": f"incident-{dag_id}", "received_at": self.received_at.isoformat()}
                    ],
                    "next_cursor": None,
                },
            )
        dag_id = request.url.path.rsplit("incident-", 1)[-1]
        return httpx.Response(200, json={"diagnosis": self.diagnose.get(dag_id)})


def _diagnosis(**overrides) -> dict:
    body = {
        "root_cause_category": "schema_drift",
        "confidence": 0.86,
        "conclusive": True,
        "halt_reason": "concluded",
        "summary": "customer_id was renamed",
        "responsible_task_id": "land_raw_orders",
        "model_used": "llama3.2:3b",
        "created_at": datetime.now(UTC).isoformat(),
    }
    return {**body, **overrides}


def _clients(stack: FakeStack) -> Clients:
    return Clients(
        airflow=httpx.AsyncClient(
            transport=httpx.MockTransport(stack.airflow), base_url="http://airflow"
        ),
        agent=httpx.AsyncClient(
            transport=httpx.MockTransport(stack.agent), base_url="http://agent"
        ),
    )


async def test_triggering_asks_airflow_to_run_the_dag():
    stack = FakeStack()
    clients = _clients(stack)

    await trigger(clients, DRIFT, "eval__1")

    assert stack.triggered[0][0] == "schema_drift_orders"


async def test_an_airflow_that_refuses_the_trigger_is_an_error_not_a_zero_score():
    # Silently scoring zero because nothing could be triggered would look exactly like an
    # agent that got everything wrong.
    stack = FakeStack()
    stack.airflow_status = 404

    with pytest.raises(EvaluationError) as excinfo:
        await trigger(_clients(stack), DRIFT, "eval__1")

    assert excinfo.value.details["dag_id"] == "schema_drift_orders"


async def test_seeding_triggers_every_scenario_without_waiting():
    stack = FakeStack()

    await seed(_clients(stack), SCENARIOS)

    assert {dag_id for dag_id, _url in stack.triggered} == {s.dag_id for s in SCENARIOS}


async def test_a_diagnosis_is_picked_up_once_it_appears():
    stack = FakeStack(diagnose={"schema_drift_orders": _diagnosis()})

    observed = await await_diagnosis(_clients(stack), DRIFT, datetime.now(UTC), timeout_s=10.0)

    assert observed.category is RootCauseCategory.SCHEMA_DRIFT
    assert observed.responsible_task == "land_raw_orders"
    assert observed.conclusive is True


async def test_an_incident_from_an_earlier_run_is_not_counted_as_this_one():
    # Scoring a previous run's incident would report the old agent's accuracy, which is
    # the bug that makes a harness quietly lie.
    stack = FakeStack(diagnose={"schema_drift_orders": _diagnosis()})
    stack.received_at = datetime.now(UTC) - timedelta(hours=1)

    observed = await await_diagnosis(
        _clients(stack), DRIFT, datetime.now(UTC), timeout_s=0.05, poll_interval_s=0.01
    )

    assert not observed.diagnosed


async def test_waiting_gives_up_rather_than_hanging():
    stack = FakeStack()

    observed = await await_diagnosis(
        _clients(stack), DRIFT, datetime.now(UTC), timeout_s=0.05, poll_interval_s=0.01
    )

    assert not observed.diagnosed
    assert observed.confidence == 0.0


async def test_a_full_run_scores_every_scenario():
    diagnoses = {
        scenario.dag_id: _diagnosis(
            root_cause_category=scenario.expected_category.value,
            responsible_task_id=scenario.expected_responsible_task,
        )
        for scenario in SCENARIOS
        if scenario.expects_failure
    }
    stack = FakeStack(diagnose=diagnoses)

    scorecard = await evaluate(
        _clients(stack),
        SCENARIOS,
        timeout_s=1.0,
        model="llama3.2:3b",
        control_timeout_s=0.05,
        poll_interval_s=0.01,
    )

    assert scorecard.category_accuracy == 1.0
    assert scorecard.attribution_accuracy == 1.0
    assert len(scorecard.results) == 8


async def test_an_agent_that_diagnoses_the_control_is_marked_down():
    stack = FakeStack(diagnose={"healthy_baseline": _diagnosis()})

    scorecard = await evaluate(
        _clients(stack), (CONTROL,), timeout_s=1.0, control_timeout_s=0.05, poll_interval_s=0.01
    )

    assert scorecard.category_accuracy == 0.0
    assert scorecard.results[0].verdict == "false positive"


async def test_an_agent_that_answers_nothing_scores_zero_not_an_error():
    stack = FakeStack()

    scorecard = await evaluate(_clients(stack), (DRIFT,), timeout_s=0.05, poll_interval_s=0.01)

    assert scorecard.category_accuracy == 0.0
    assert scorecard.results[0].verdict == "no diagnosis"


async def test_every_scenario_is_triggered_before_any_is_awaited():
    # The agent is meant to handle concurrent incidents. Running them in lockstep would
    # test a situation that never happens in production.
    stack = FakeStack()

    await evaluate(
        _clients(stack), SCENARIOS, timeout_s=0.05, control_timeout_s=0.05, poll_interval_s=0.01
    )

    assert len(stack.triggered) == len(SCENARIOS)


def test_the_agent_client_carries_the_api_key_when_one_is_configured():
    from pydantic import SecretStr

    settings = Settings()
    settings.api_key = SecretStr("the-secret")

    clients = build_clients(settings, "http://agent")

    assert clients.agent.headers["X-API-Key"] == "the-secret"


def test_the_airflow_client_uses_the_configured_credentials():
    clients = build_clients(Settings(), "http://agent")

    assert str(clients.airflow.base_url) == "http://localhost:8080"
