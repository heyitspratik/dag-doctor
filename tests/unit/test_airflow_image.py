"""What the Airflow image has to install for the failure callback to import.

The DAG files import the callback, so a package missing from that image is not a
degraded feature: every DAG fails to import and Airflow has no DAGs at all. This computes
the callback's actual import closure and checks it against the two places that install it.
"""

import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SRC = ROOT / "src"
DOCKERFILE = (ROOT / "docker" / "airflow.Dockerfile").read_text()
CI = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
PARSE_SCRIPT = (ROOT / "docker" / "parse-dags.sh").read_text()

CALLBACK = "dag_doctor.messaging.airflow_callback"

#: Airflow already depends on these, so installing them again would only risk moving a
#: version Airflow pinned for its own reasons.
AIRFLOW_PROVIDES = {"pydantic"}


def _third_party_closure(entry: str) -> set[str]:
    """Every third-party package reachable by importing a module and what it imports."""
    seen: set[str] = set()
    packages: set[str] = set()

    def walk(module: str) -> None:
        if module in seen:
            return
        seen.add(module)
        path = SRC / (module.replace(".", "/") + ".py")
        if not path.exists():
            return
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            else:
                continue
            for name in names:
                if name.startswith("dag_doctor"):
                    walk(name)
                elif name.split(".")[0] not in sys.stdlib_module_names:
                    packages.add(name.split(".")[0])

    walk(entry)
    return packages


REQUIRED = sorted(_third_party_closure(CALLBACK) - AIRFLOW_PROVIDES)


def test_the_callback_closure_is_small_enough_to_be_worth_listing():
    # If this grows, the callback has started depending on the agent's own machinery and
    # the right fix is to trim the import, not to fatten the Airflow image with langgraph.
    assert REQUIRED
    assert len(REQUIRED) <= 5, REQUIRED


@pytest.mark.parametrize("package", REQUIRED)
def test_the_airflow_image_installs_it(package):
    # A missing package here is not a degraded feature. The DAG files import the callback,
    # so every DAG fails to import and Airflow ends up with none at all.
    assert package.replace("_", "-") in DOCKERFILE or package in DOCKERFILE


def test_the_dag_parsing_ci_job_builds_the_image_rather_than_restating_it():
    # CI used to repeat this dependency list, which is a second place for it to drift
    # from. Building the real image instead makes the Dockerfile the only source of
    # truth, and parses the DAGs against what the stack actually runs.
    assert "docker/airflow.Dockerfile" in CI
    assert "docker/parse-dags.sh" in CI


def test_the_parse_script_is_a_file_rather_than_embedded_in_the_workflow():
    # It was a Python heredoc inside a bash -c inside a YAML block scalar. The
    # indentation YAML requires stopped bash finding the terminator, so Python received
    # the whole script indented. A file has one level of quoting and can be run locally
    # exactly as CI runs it.
    assert "DagBag" not in CI
    assert "DagBag" in PARSE_SCRIPT


def test_an_empty_dagbag_is_not_treated_as_a_pass():
    # Nothing found is what a mounting mistake looks like, and it would otherwise report
    # success while checking nothing at all.
    assert "no DAGs were found" in PARSE_SCRIPT
    assert "set -euo pipefail" in PARSE_SCRIPT


def test_the_callback_does_not_drag_in_the_agent_itself():
    # The callback runs inside a task process. Importing the graph or the tools there
    # would put langgraph and sqlalchemy in the Airflow image for no reason.
    for forbidden in ("langgraph", "sqlalchemy", "fastapi"):
        assert forbidden not in REQUIRED
