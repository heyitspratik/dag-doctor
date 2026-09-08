"""Structural checks on the compose stack.

The stack cannot be started here, so these check the properties that are cheap to get
wrong and expensive to notice: a service that lost its dependency ordering, a healthcheck
that quietly disappeared, or a broker address pointing at the host from inside a container.
"""

from pathlib import Path

import pytest
import yaml

COMPOSE_FILE = Path(__file__).parents[2] / "docker" / "docker-compose.yml"
STACK = yaml.safe_load(COMPOSE_FILE.read_text())
SERVICES = STACK["services"]

EXPECTED_SERVICES = {
    "postgres",
    "redpanda",
    "redpanda-init",
    "ollama",
    "migrate",
    "agent-worker",
    "agent-api",
    "airflow-init",
    "airflow-scheduler",
    "airflow-webserver",
}

#: Services that run to completion rather than staying up, so they have nothing to probe.
ONE_SHOT = {"redpanda-init", "migrate", "airflow-init"}


def test_the_stack_declares_every_expected_service():
    assert set(SERVICES) == EXPECTED_SERVICES


@pytest.mark.parametrize("name", sorted(EXPECTED_SERVICES - ONE_SHOT - {"agent-worker"}))
def test_every_long_running_service_has_a_healthcheck(name):
    # depends_on: service_healthy is worth nothing without one, and its absence is silent.
    assert "healthcheck" in SERVICES[name]


@pytest.mark.parametrize("name", sorted(ONE_SHOT))
def test_one_shot_services_do_not_restart(name):
    assert SERVICES[name].get("restart") == "no"


def test_the_worker_waits_for_the_schema_and_the_topics():
    depends_on = SERVICES["agent-worker"]["depends_on"]

    assert depends_on["migrate"]["condition"] == "service_completed_successfully"
    assert depends_on["redpanda-init"]["condition"] == "service_completed_successfully"


def test_airflow_waits_for_its_database_and_its_own_initialisation():
    for name in ("airflow-scheduler", "airflow-webserver"):
        depends_on = SERVICES[name]["depends_on"]
        assert depends_on["postgres"]["condition"] == "service_healthy"
        assert depends_on["airflow-init"]["condition"] == "service_completed_successfully"


def test_containers_reach_the_broker_by_its_service_name():
    # localhost:19092 is the host-side listener. A container using it would connect to
    # itself, which fails in a way that looks like a broker outage.
    for name in ("agent-worker", "airflow-scheduler"):
        environment = SERVICES[name].get("environment", {})
        assert environment["KAFKA_BOOTSTRAP_SERVERS"] == "redpanda:9092"


def test_the_agent_reads_airflow_through_the_read_only_role():
    dsn = SERVICES["agent-worker"]["environment"]["AIRFLOW_DSN"]

    assert dsn.startswith("postgresql+psycopg://dagdoctor_ro:")


def test_the_worker_and_the_migration_share_one_image():
    assert SERVICES["agent-worker"]["image"] == SERVICES["migrate"]["image"]


def test_every_named_volume_is_declared():
    mounted = {
        mount.split(":", 1)[0]
        for service in SERVICES.values()
        for mount in service.get("volumes", [])
        if not mount.startswith((".", "/"))
    }
    airflow_mounts = {
        mount.split(":", 1)[0]
        for mount in STACK["x-airflow-common"]["volumes"]
        if not mount.startswith((".", "/"))
    }

    assert mounted | airflow_mounts <= set(STACK["volumes"])


def test_the_api_and_the_worker_run_the_same_image():
    # Two entrypoints from one build, which is what lets them be scaled separately
    # without maintaining two images.
    assert SERVICES["agent-api"]["image"] == SERVICES["agent-worker"]["image"]
    assert SERVICES["agent-api"]["command"] == ["dag-doctor-api"]


def test_the_worker_exposes_its_own_metrics_port():
    # Separate processes have separate registries, so one scrape endpoint cannot serve
    # both.
    assert SERVICES["agent-worker"]["environment"]["METRICS_PORT"] == 9100


def test_both_agent_services_read_the_warehouse_through_the_read_only_role():
    for name in ("agent-api", "agent-worker"):
        assert SERVICES[name]["environment"]["WAREHOUSE_DSN"].startswith(
            "postgresql+psycopg://dagdoctor_ro:"
        )
