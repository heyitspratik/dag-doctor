"""Structural checks on the seeded DAG files.

These parse the DAG files as source rather than importing them, so they run without
Airflow installed. They catch the mistake that matters most as scenarios are added: a new
broken DAG that never attaches the failure callback would fail in Airflow, publish
nothing, and look exactly like an agent that could not diagnose it.

Actually importing the DAGs against a real Airflow is a separate CI job.
"""

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).parents[2] / "airflow" / "dags"
DAG_FILES = sorted(DAGS_DIR.glob("*.py"))

CALLBACK_MODULE = "dag_doctor.messaging.airflow_callback"
CALLBACK_NAME = "on_task_failure"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    return next((kw.value for kw in call.keywords if kw.arg == name), None)


def _dag_calls(tree: ast.Module) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "DAG"
    ]


def test_the_dags_directory_is_not_empty():
    assert DAG_FILES, f"no DAG files found under {DAGS_DIR}"


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.stem)
def test_each_dag_file_is_syntactically_valid(path):
    _tree(path)


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.stem)
def test_each_file_declares_exactly_one_dag(path):
    assert len(_dag_calls(_tree(path))) == 1


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.stem)
def test_the_dag_id_matches_the_file_name(path):
    # The evaluation runner addresses scenarios by dag_id and the scorer reads the file
    # name, so a mismatch would silently score the wrong scenario.
    dag_id = _keyword(_dag_calls(_tree(path))[0], "dag_id")

    assert isinstance(dag_id, ast.Constant)
    assert dag_id.value == path.stem


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.stem)
def test_each_dag_imports_the_failure_callback(path):
    imported = {
        alias.name
        for node in ast.walk(_tree(path))
        if isinstance(node, ast.ImportFrom) and node.module == CALLBACK_MODULE
        for alias in node.names
    }

    assert CALLBACK_NAME in imported


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.stem)
def test_each_dag_attaches_the_failure_callback_to_every_task(path):
    # Attached through default_args rather than per task, so a task added later inherits
    # it. A scenario DAG without this publishes nothing and looks like an agent failure.
    default_args = _keyword(_dag_calls(_tree(path))[0], "default_args")
    assert isinstance(default_args, ast.Name), "default_args should be a module-level constant"

    assignment = next(
        node
        for node in ast.walk(_tree(path))
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == default_args.id for t in node.targets)
    )
    assert isinstance(assignment.value, ast.Dict)

    callbacks = {
        value.id
        for key, value in zip(assignment.value.keys, assignment.value.values, strict=True)
        if isinstance(key, ast.Constant)
        and key.value == "on_failure_callback"
        and isinstance(value, ast.Name)
    }
    assert callbacks == {CALLBACK_NAME}


@pytest.mark.parametrize("path", DAG_FILES, ids=lambda p: p.stem)
def test_each_dag_is_triggered_by_hand_rather_than_on_a_schedule(path):
    # The seeded scenarios exist to be triggered by make seed-failures and by the
    # evaluation runner. A schedule would fill the agent's database with noise.
    schedule = _keyword(_dag_calls(_tree(path))[0], "schedule")

    assert isinstance(schedule, ast.Constant)
    assert schedule.value is None


def test_every_seeded_dag_has_an_entry_in_the_answer_key():
    # The accuracy number means nothing if a DAG can be added without anybody deciding
    # what the right answer for it is.
    from dag_doctor.evaluation.scenarios import SCENARIOS

    assert {path.stem for path in DAG_FILES} == {scenario.dag_id for scenario in SCENARIOS}


@pytest.mark.parametrize(
    "scenario",
    __import__("dag_doctor.evaluation.scenarios", fromlist=["SCENARIOS"]).SCENARIOS,
    ids=lambda s: s.dag_id,
)
def test_the_task_the_key_scores_actually_exists_in_the_dag(scenario):
    # Scoring a task that is not in the DAG would silently score nothing at all.
    source = (DAGS_DIR / f"{scenario.dag_id}.py").read_text()
    task_ids = _task_ids(ast.parse(source))

    assert scenario.failing_task in task_ids


@pytest.mark.parametrize(
    "scenario",
    [
        s
        for s in __import__("dag_doctor.evaluation.scenarios", fromlist=["SCENARIOS"]).SCENARIOS
        if s.expected_responsible_task
    ],
    ids=lambda s: s.dag_id,
)
def test_the_task_the_key_blames_actually_exists_in_the_dag(scenario):
    source = (DAGS_DIR / f"{scenario.dag_id}.py").read_text()
    task_ids = _task_ids(ast.parse(source))

    assert scenario.expected_responsible_task in task_ids
    assert scenario.expected_responsible_task != scenario.failing_task


def _task_ids(tree: ast.Module) -> set[str]:
    """Every task_id declared in a DAG file."""
    return {
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant)
    }


def test_only_the_transient_scenario_retries():
    # Retrying a deterministic failure only delays the diagnosis. The transient one is
    # the single case where retries belong, and its diagnosis should say so.
    retrying = {path.stem for path in DAG_FILES if '"retries": 0' not in path.read_text()}

    assert retrying == {"connection_timeout_api"}
