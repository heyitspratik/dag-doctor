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


@pytest.mark.parametrize("name", sorted(EXPECTED_SERVICES - ONE_SHOT))
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
        # A "$" prefix is an overridable path, which may resolve to a host directory
        # rather than to a named volume this file has to declare.
        if not mount.startswith((".", "/", "$"))
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


@pytest.mark.parametrize("name", ["migrate", "agent-worker", "agent-api"])
def test_every_service_running_the_agent_image_can_also_build_it(name):
    # A service declaring only `image:` makes compose try to pull dag-doctor:local from a
    # registry, which fails noisily before some other service happens to build it, and
    # fails outright if that service is not among the ones being started.
    assert SERVICES[name]["build"]["dockerfile"] == "docker/Dockerfile"
    assert SERVICES[name]["image"] == "dag-doctor:local"


def test_the_worker_exposes_its_own_metrics_port():
    # Separate processes have separate registries, so one scrape endpoint cannot serve
    # both.
    assert SERVICES["agent-worker"]["environment"]["METRICS_PORT"] == 9100


def test_both_agent_services_read_the_warehouse_through_the_read_only_role():
    for name in ("agent-api", "agent-worker"):
        assert SERVICES[name]["environment"]["WAREHOUSE_DSN"].startswith(
            "postgresql+psycopg://dagdoctor_ro:"
        )


def test_the_model_store_can_point_at_an_existing_ollama():
    # Anyone already running Ollama has the weights. Making them download two gigabytes
    # again is a poor first impression, and the default stays a named volume so a fresh
    # clone is still self-contained.
    mounts = SERVICES["ollama"]["volumes"]

    assert any(mount.startswith("${OLLAMA_MODELS:-ollama-models}") for mount in mounts)


def _mount_targets(service: dict[str, object]) -> list[str]:
    """Where a service's volumes land inside the container."""
    volumes = service.get("volumes")
    return (
        [mount.split(":")[1] for mount in volumes if ":" in mount]
        if isinstance(volumes, list)
        else []
    )


@pytest.mark.parametrize("name", [*sorted(EXPECTED_SERVICES), "x-airflow-common"])
def test_no_mount_is_nested_inside_another(name):
    # Docker creates the mountpoint before mounting, and it cannot create a directory
    # inside a read-only bind mount. Nesting one mount under another fails at container
    # start with an error naming overlayfs rather than this file, which is a long way to
    # travel to find a one-line mistake.
    targets = _mount_targets(STACK["services"][name] if name in STACK["services"] else STACK[name])

    for target in targets:
        for other in targets:
            assert other == target or not target.startswith(other.rstrip("/") + "/"), (
                f"{name} mounts {target} inside {other}"
            )


def test_the_worker_probes_its_own_port_not_the_api_s():
    # Both run the same image, so the worker inherits a HEALTHCHECK aimed at the API's
    # port 8000. It serves nothing there, and an unoverridden probe reports unhealthy
    # forever while the consumer works perfectly.
    probe = " ".join(SERVICES["agent-worker"]["healthcheck"]["test"])

    assert "9100" in probe
    assert "8000" not in probe


def _shell_scripts() -> dict[str, str]:
    """Every service whose command is a shell script, and that script."""
    scripts = {}
    for name, service in SERVICES.items():
        entrypoint = service.get("entrypoint")
        command = service.get("command")
        if isinstance(entrypoint, list) and "-c" in entrypoint and isinstance(command, list):
            scripts[name] = "\n".join(str(part) for part in command)
    return scripts


def _effective_commands(script: str) -> list[str]:
    """What bash would see as separate commands.

    Comments are dropped and backslash continuations are joined, so what is left is one
    entry per command the shell would actually try to run.
    """
    joined = script.replace("\\\n", " ")
    return [
        line.strip()
        for line in joined.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def test_some_services_do_run_shell_scripts():
    # Guards the guard: if the shape changes and nothing matches, the check below would
    # pass by checking nothing.
    assert set(_shell_scripts()) == {"redpanda-init", "airflow-init"}


@pytest.mark.parametrize("name", sorted(_shell_scripts()))
def test_no_continuation_line_became_its_own_command(name):
    # YAML's folded scalar keeps the newlines on lines indented further than the first, so
    # a folded multi-line command arrives at bash as several commands and each
    # continuation flag becomes a command name. It fails as "--brokers: command not found"
    # and, behind a `|| true`, not visibly at all. This cost two silent breakages: topics
    # that were never created and an Airflow user that never existed.
    for command in _effective_commands(_shell_scripts()[name]):
        assert not command.startswith("-"), (
            f"{name} would run {command!r} as a command, which means a continuation line "
            f"was split off from the line above it"
        )


@pytest.mark.parametrize("name", sorted(_shell_scripts()))
def test_every_init_script_stops_on_the_first_error(name):
    assert "set -euo pipefail" in _shell_scripts()[name]


def test_creating_the_airflow_user_is_not_allowed_to_fail_silently():
    # The REST API and the evaluation harness both authenticate as this user, so a
    # swallowed failure here surfaces much later as an unexplained 401.
    commands = _effective_commands(_shell_scripts()["airflow-init"])

    assert any("airflow users create" in command for command in commands)
    assert not any("|| true" in command for command in commands)
