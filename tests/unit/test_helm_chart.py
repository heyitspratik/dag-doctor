"""Structural checks on the Helm chart.

Rendering the chart needs Helm, which is not a Python dependency, so CI does that with
`helm lint` and `kubeconform`. These cover what is cheap to check here and expensive to
notice later: a values file that quietly starts committing a credential, or a probe path
that no longer exists in the API.
"""

import re
from pathlib import Path

import pytest
import yaml

CHART_DIR = Path(__file__).parents[2] / "deploy" / "helm" / "dag-doctor"
TEMPLATES = CHART_DIR / "templates"

CHART = yaml.safe_load((CHART_DIR / "Chart.yaml").read_text())
VALUES = yaml.safe_load((CHART_DIR / "values.yaml").read_text())
LOCAL = yaml.safe_load((CHART_DIR / "values-local.yaml").read_text())
PROD = yaml.safe_load((CHART_DIR / "values-prod.yaml").read_text())

EXPECTED_TEMPLATES = {
    "_helpers.tpl",
    "NOTES.txt",
    "api-deployment.yaml",
    "api-hpa.yaml",
    "api-service.yaml",
    "configmap.yaml",
    "migrate-job.yaml",
    "poddisruptionbudget.yaml",
    "secret.yaml",
    "serviceaccount.yaml",
    "servicemonitor.yaml",
    "worker-deployment.yaml",
    "worker-hpa.yaml",
}


def test_the_chart_ships_every_template():
    assert {path.name for path in TEMPLATES.iterdir()} == EXPECTED_TEMPLATES


def test_the_chart_declares_a_kubernetes_floor():
    # The chart uses autoscaling/v2 and policy/v1, neither of which exists on old
    # clusters, so failing at install time with a clear message beats failing at apply.
    assert CHART["kubeVersion"].startswith(">=1.")
    assert CHART["apiVersion"] == "v2"


def test_the_chart_version_and_the_app_version_are_separate():
    # A values change is not a new agent, and conflating them makes the chart version
    # useless as a signal.
    assert "version" in CHART
    assert "appVersion" in CHART


@pytest.mark.parametrize("component", ["api", "worker"])
def test_both_components_request_and_limit_resources(component):
    resources = VALUES[component]["resources"]

    assert resources["requests"]["cpu"]
    assert resources["requests"]["memory"]
    assert resources["limits"]["memory"]


def test_the_worker_grace_period_outlasts_an_investigation():
    # The worker drains on SIGTERM. A grace period shorter than one investigation would
    # kill investigations mid-flight on every deploy.
    assert VALUES["worker"]["terminationGracePeriodSeconds"] >= 600


def test_the_worker_has_no_prestop_hook():
    # preStop runs before SIGTERM and inside the same grace period, so one here would
    # delay the drain rather than enable it. See the chart README.
    assert "lifecycle:" not in (TEMPLATES / "worker-deployment.yaml").read_text()


def test_the_api_has_a_prestop_hook():
    # Where it is the right tool: letting the endpoints controller withdraw the pod
    # before it stops serving.
    assert "preStop" in (TEMPLATES / "api-deployment.yaml").read_text()


def test_the_probes_point_at_endpoints_the_api_actually_serves():
    from dag_doctor.api.main import create_app
    from dag_doctor.core.settings import Settings

    deployment = (TEMPLATES / "api-deployment.yaml").read_text()
    served = set(create_app(Settings()).openapi()["paths"])

    assert "/health/live" in served
    assert "/health/ready" in served
    assert "path: /health/live" in deployment
    assert "path: /health/ready" in deployment


def test_liveness_does_not_probe_readiness():
    # Pointing liveness at the dependency check turns a brief database blip into a
    # restart loop.
    deployment = (TEMPLATES / "api-deployment.yaml").read_text()
    liveness = deployment.split("livenessProbe:")[1].split("readinessProbe:")[0]

    assert "/health/live" in liveness
    assert "/health/ready" not in liveness


def test_autoscaling_is_off_until_someone_provides_the_metric():
    # An HPA referencing a metric nobody publishes does not fail loudly, it simply never
    # scales, which is worse than not having one.
    assert VALUES["worker"]["autoscaling"]["enabled"] is False
    assert VALUES["api"]["autoscaling"]["enabled"] is False


def test_the_worker_scales_on_consumer_lag_rather_than_cpu():
    # The worker waits on a model, so its CPU utilisation says almost nothing about
    # whether incidents are piling up.
    lag = VALUES["worker"]["autoscaling"]["consumerLag"]

    assert lag["enabled"] is True
    assert lag["metricName"] == "kafka_consumergroup_lag"
    assert VALUES["worker"]["autoscaling"]["targetCPUUtilizationPercentage"] == 0


def test_production_values_do_not_commit_credentials():
    # The check that matters most in this file. Values files end up in git, and git
    # history is forever.
    assert PROD["secrets"]["create"] is False
    assert PROD["secrets"]["existingSecret"]
    assert "postgresDsn" not in PROD["secrets"]


def test_production_pins_the_image_tag():
    # A floating tag makes a rollback a guess about what was running.
    assert PROD["image"]["tag"]
    assert PROD["image"]["tag"] != "latest"


def test_the_local_values_disable_disruption_budgets():
    # One replica plus a budget blocks node drains, so on a single-node kind cluster
    # nothing could ever be evicted.
    assert LOCAL["api"]["podDisruptionBudget"]["enabled"] is False
    assert LOCAL["worker"]["podDisruptionBudget"]["enabled"] is False


def test_the_local_values_point_at_the_kind_dependencies():
    dependencies = yaml.safe_load_all(
        (CHART_DIR.parents[1] / "kind" / "dependencies.yaml").read_text()
    )
    service_names = {
        document["metadata"]["name"]
        for document in dependencies
        if document and document["kind"] == "Service"
    }

    for url in (LOCAL["config"]["ollamaBaseUrl"], LOCAL["config"]["kafkaBootstrapServers"]):
        host = url.removeprefix("http://").split(":")[0]
        assert host in service_names


#: Environment variable names are the only all-caps keys in these templates, which is
#: what lets them be picked out without rendering the chart.
_ENV_KEY = re.compile(r"^\s{2,}([A-Z][A-Z0-9_]*):")


def _template_env_keys(name: str) -> set[str]:
    """The environment variable names a template sets."""
    return {
        match.group(1)
        for line in (TEMPLATES / name).read_text().splitlines()
        if (match := _ENV_KEY.match(line))
    }


@pytest.mark.parametrize("template", ["configmap.yaml", "secret.yaml"])
def test_every_key_the_chart_sets_is_a_variable_the_settings_read(template):
    # The chart is the only place these names appear outside the settings module, so a
    # renamed variable would silently stop being configurable and nothing would say so.
    from tests.unit.core.test_settings import _known_variables

    keys = _template_env_keys(template)

    assert keys
    assert keys <= _known_variables()


def test_the_chart_configures_the_things_that_matter_at_deploy_time():
    # A chart that parses but forgets the budgets or the broker is a chart nobody can
    # actually operate.
    keys = _template_env_keys("configmap.yaml") | _template_env_keys("secret.yaml")

    assert {
        "LLM_PROVIDER",
        "MAX_ITERATIONS",
        "MAX_TOOL_CALLS",
        "KAFKA_BOOTSTRAP_SERVERS",
        "POSTGRES_DSN",
        "AIRFLOW_DSN",
        "WAREHOUSE_DSN",
        "METRICS_PORT",
    } <= keys
