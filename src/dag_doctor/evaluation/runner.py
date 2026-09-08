"""Triggering the seeded failures and scoring what the agent makes of them.

Deliberately an outside-in test. It talks to Airflow's REST API and the agent's own HTTP
API, exactly as a person would, rather than reaching into the database or calling the
graph directly. A harness that bypasses the moving parts is a harness that keeps passing
after they break.

The number this prints is the one the README publishes, so the mechanism producing it has
to be something a reader can run and check.
"""

import argparse
import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from dag_doctor.core.exceptions import DagDoctorError
from dag_doctor.core.logging import configure_logging, get_logger
from dag_doctor.core.models import RootCauseCategory
from dag_doctor.core.settings import Settings, get_settings
from dag_doctor.evaluation.scenarios import SCENARIOS, Scenario
from dag_doctor.evaluation.scorer import Observed, ScenarioResult, Scorecard

logger = get_logger(__name__)

#: How long to wait for a diagnosis before giving up on a scenario. Generous, because a
#: 3B model running five iterations against a cold Ollama genuinely takes minutes, and a
#: harness that times out early would report the model as wrong rather than as slow.
DEFAULT_TIMEOUT_S = 600.0
POLL_INTERVAL_S = 5.0

#: The control is given a shorter window: it is expected to produce nothing, and waiting
#: ten minutes to confirm an absence would double the run time for no information.
CONTROL_WAIT_S = 60.0


class EvaluationError(DagDoctorError):
    """The harness could not run, as distinct from the agent doing badly."""

    code = "EVALUATION_ERROR"
    http_status = 500


@dataclass
class Clients:
    """The two APIs the harness drives."""

    airflow: httpx.AsyncClient
    agent: httpx.AsyncClient


async def trigger(clients: Clients, scenario: Scenario, run_id: str) -> None:
    """Ask Airflow to run one seeded DAG.

    Args:
        clients: The Airflow and agent clients.
        scenario: The scenario to trigger.
        run_id: The run identifier to use, so the harness can find its own run again.

    Raises:
        EvaluationError: If Airflow refused to start the run.
    """
    response = await clients.airflow.post(
        f"/api/v1/dags/{scenario.dag_id}/dagRuns",
        json={"dag_run_id": run_id, "conf": {}},
    )
    if response.status_code >= httpx.codes.BAD_REQUEST:
        raise EvaluationError(
            f"Airflow refused to trigger {scenario.dag_id}: {response.status_code}",
            details={"dag_id": scenario.dag_id, "body": response.text[:400]},
        )
    logger.info("evaluation.triggered", dag_id=scenario.dag_id, run_id=run_id)


async def await_diagnosis(
    clients: Clients,
    scenario: Scenario,
    started: datetime,
    timeout_s: float,
    poll_interval_s: float = POLL_INTERVAL_S,
) -> Observed:
    """Poll the agent's API until this scenario's incident has been diagnosed.

    Args:
        clients: The Airflow and agent clients.
        scenario: The scenario being waited on.
        started: When the run was triggered, so an older incident cannot be mistaken for
            this one.
        timeout_s: How long to wait.
        poll_interval_s: How long to pause between polls.

    Returns:
        What the agent produced, or an empty observation if nothing arrived in time.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        observed = await _latest_diagnosis(clients, scenario, started)
        if observed is not None:
            return observed
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        # Never sleep past the deadline: overshooting turns a one second timeout into a
        # full polling interval, which is the difference between a fast test suite and a
        # slow one, and between an honest timeout and an approximate one.
        await asyncio.sleep(min(poll_interval_s, remaining))
    logger.warning("evaluation.timed_out", dag_id=scenario.dag_id, waited_s=timeout_s)
    return Observed()


async def _latest_diagnosis(
    clients: Clients, scenario: Scenario, started: datetime
) -> Observed | None:
    """Fetch this scenario's diagnosis, if the agent has produced one yet."""
    listing = await clients.agent.get(
        "/api/v1/incidents",
        params={"dag_id": scenario.dag_id, "task_id": scenario.failing_task, "limit": 10},
    )
    listing.raise_for_status()

    for item in listing.json()["items"]:
        received = datetime.fromisoformat(item["received_at"])
        if received < started:
            # An incident from an earlier evaluation run. Counting it would score the
            # previous run's agent, which is exactly the bug that makes a harness lie.
            continue
        detail = await clients.agent.get(f"/api/v1/incidents/{item['id']}")
        detail.raise_for_status()
        diagnosis = detail.json().get("diagnosis")
        if diagnosis is not None:
            return _observe(item, diagnosis, started)
    return None


def _observe(
    incident: dict[str, object], diagnosis: dict[str, object], started: datetime
) -> Observed:
    """Turn an API response into the shape the scorer reads."""
    diagnosed_at = datetime.fromisoformat(str(diagnosis["created_at"]))
    category = diagnosis.get("root_cause_category")
    return Observed(
        incident_id=str(incident["id"]),
        category=RootCauseCategory(category) if isinstance(category, str) else None,
        responsible_task=(
            str(diagnosis["responsible_task_id"])
            if diagnosis.get("responsible_task_id") is not None
            else None
        ),
        confidence=float(str(diagnosis["confidence"])),
        conclusive=bool(diagnosis["conclusive"]),
        halt_reason=str(diagnosis["halt_reason"]),
        summary=str(diagnosis["summary"]),
        model_used=str(diagnosis.get("model_used", "")),
        duration_s=(diagnosed_at - started).total_seconds(),
        diagnosed_at=diagnosed_at,
    )


async def evaluate(
    clients: Clients,
    scenarios: tuple[Scenario, ...] = SCENARIOS,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    model: str = "",
    control_timeout_s: float = CONTROL_WAIT_S,
    poll_interval_s: float = POLL_INTERVAL_S,
) -> Scorecard:
    """Trigger every scenario, wait for the diagnoses, and score them.

    Scenarios are triggered together and then awaited, rather than one at a time. The
    agent is meant to handle concurrent incidents, and running them in lockstep would test
    a situation that never happens in production.

    Args:
        clients: The Airflow and agent clients.
        scenarios: Which scenarios to run.
        timeout_s: How long to wait for each diagnosis.
        model: What to label the scorecard with.
        control_timeout_s: How long to wait before accepting that the control produced
            nothing, which is shorter because confirming an absence needs no patience.
        poll_interval_s: How long to pause between polls.

    Returns:
        The scorecard.
    """
    started = datetime.now(UTC)
    run_id = f"eval__{started:%Y%m%dT%H%M%S}"
    for scenario in scenarios:
        await trigger(clients, scenario, run_id)

    observations = await asyncio.gather(
        *(
            await_diagnosis(
                clients,
                scenario,
                started,
                timeout_s if scenario.expects_failure else control_timeout_s,
                poll_interval_s,
            )
            for scenario in scenarios
        )
    )
    return Scorecard(
        results=[
            ScenarioResult(scenario=scenario, observed=observed)
            for scenario, observed in zip(scenarios, observations, strict=True)
        ],
        model=model,
    )


async def seed(clients: Clients, scenarios: tuple[Scenario, ...] = SCENARIOS) -> None:
    """Trigger every seeded DAG and return without waiting.

    Args:
        clients: The Airflow and agent clients.
        scenarios: Which scenarios to trigger.
    """
    run_id = f"seed__{datetime.now(UTC):%Y%m%dT%H%M%S}"
    for scenario in scenarios:
        await trigger(clients, scenario, run_id)


def build_clients(settings: Settings, agent_url: str) -> Clients:
    """Open clients for Airflow and the agent.

    Args:
        settings: Where Airflow lives and how to authenticate.
        agent_url: Where the agent's API lives.

    Returns:
        The clients, which the caller must close.
    """
    headers = {}
    if settings.api_key is not None:
        headers["X-API-Key"] = settings.api_key.get_secret_value()
    return Clients(
        airflow=httpx.AsyncClient(
            base_url=settings.airflow.base_url.rstrip("/"),
            auth=(settings.airflow.username, settings.airflow.password.get_secret_value()),
            timeout=30.0,
        ),
        agent=httpx.AsyncClient(base_url=agent_url.rstrip("/"), headers=headers, timeout=30.0),
    )


async def _run(arguments: argparse.Namespace) -> int:
    """Do what the command line asked for."""
    settings = get_settings()
    configure_logging(settings.app_env, settings.log_level)
    clients = build_clients(settings, arguments.agent_url)
    try:
        if arguments.command == "seed":
            await seed(clients)
            return 0
        scorecard = await evaluate(
            clients,
            timeout_s=arguments.timeout,
            model=arguments.model or settings.llm.default_model,
        )
    finally:
        await clients.airflow.aclose()
        await clients.agent.aclose()

    table = scorecard.to_markdown()
    print(table)
    if arguments.output:
        arguments.output.write_text(table, encoding="utf-8")
    # A non-zero exit when accuracy falls below the floor, so this can gate a pipeline
    # rather than only inform one.
    return 0 if scorecard.category_accuracy >= arguments.min_accuracy else 1


def main() -> int:
    """Entry point for ``make seed-failures`` and ``make evaluate``."""
    parser = argparse.ArgumentParser(description="Run the seeded failure scenarios.")
    parser.add_argument("command", choices=["seed", "evaluate"])
    parser.add_argument("--agent-url", default="http://localhost:8000")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--control-timeout", type=float, default=CONTROL_WAIT_S)
    parser.add_argument("--model", default="", help="Label for the scorecard")
    parser.add_argument("--output", type=Path, default=None, help="Write the table here too")
    parser.add_argument(
        "--min-accuracy",
        type=float,
        default=0.0,
        help="Exit non-zero below this category accuracy",
    )
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
