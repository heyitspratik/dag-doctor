"""Tools that read Airflow's own metadata database.

Airflow owns this schema; the projection below names only the columns these tools depend
on, so an Airflow upgrade that adds columns changes nothing here and one that moves a
column we read fails loudly in one place rather than diffusely at query time.

Everything is read through a SELECT-only role. The agent is an observer of Airflow, not a
participant in it.
"""

import json
import statistics
from datetime import datetime
from typing import ClassVar

from sqlalchemy import Column, Integer, MetaData, String, Table, Text, select
from sqlalchemy import DateTime as SADateTime
from sqlalchemy import Float as SAFloat

from dag_doctor.tools.base import BaseTool, TargetNotFoundError, ToolInput, ToolOutput
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.sql import ReadOnlyExecutor

#: A read-only projection of the Airflow tables these tools query. Not Airflow's models:
#: importing those would drag the whole scheduler into the agent's process.
AIRFLOW = MetaData()

TASK_INSTANCE = Table(
    "task_instance",
    AIRFLOW,
    Column("dag_id", String),
    Column("task_id", String),
    Column("run_id", String),
    Column("map_index", Integer),
    Column("start_date", SADateTime(timezone=True)),
    Column("end_date", SADateTime(timezone=True)),
    Column("duration", SAFloat),
    Column("state", String),
    Column("try_number", Integer),
    Column("operator", String),
)

DAG_TABLE = Table(
    "dag",
    AIRFLOW,
    Column("dag_id", String),
    Column("fileloc", String),
    Column("is_paused", String),
)

DAG_CODE = Table(
    "dag_code",
    AIRFLOW,
    Column("fileloc", String),
    Column("source_code", Text),
    Column("last_updated", SADateTime(timezone=True)),
)

SERIALIZED_DAG = Table(
    "serialized_dag",
    AIRFLOW,
    Column("dag_id", String),
    Column("data", Text),
    Column("last_updated", SADateTime(timezone=True)),
)

#: Airflow task states that mean the task did not produce its output.
UNSUCCESSFUL_STATES = frozenset({"failed", "upstream_failed", "skipped", "removed"})


class RunOutcome(ToolOutput):
    """One historical attempt at a task."""

    run_id: str
    state: str | None
    try_number: int
    duration_s: float | None
    start_date: datetime | None
    end_date: datetime | None

    def summarise(self) -> str:
        """One line describing this attempt."""
        return f"{self.run_id}: {self.state} after {self.duration_s or 0:.1f}s"


class DagRunHistory(ToolOutput):
    """How this task has behaved recently.

    ``is_new_failure`` is the field the graph actually routes on. A task that has failed
    every night for a fortnight is a different problem from one that broke this morning,
    and the fix for each is different.
    """

    dag_id: str
    task_id: str
    runs: list[RunOutcome]
    failure_rate: float
    consecutive_failures: int
    is_new_failure: bool
    median_duration_s: float | None

    def summarise(self) -> str:
        """One line describing the task's recent record."""
        if not self.runs:
            return f"{self.dag_id}.{self.task_id} has no recorded run history"
        kind = "newly failing" if self.is_new_failure else "chronically failing"
        return (
            f"{self.dag_id}.{self.task_id} is {kind}: {self.failure_rate:.0%} of the last "
            f"{len(self.runs)} runs failed, {self.consecutive_failures} in a row"
        )


class DagRunHistoryInput(ToolInput):
    """Arguments for :class:`GetDagRunHistory`."""

    dag_id: str
    task_id: str
    limit: int = 20


class GetDagRunHistory(BaseTool[DagRunHistoryInput, DagRunHistory]):
    """Is this failure new or chronic?"""

    name: ClassVar[str] = "get_dag_run_history"
    description: ClassVar[str] = (
        "Recent outcomes, durations and failure rate for one task. Answers whether this "
        "failure is new or long-standing, which decides whether to look for a recent "
        "change at all."
    )
    input_model = DagRunHistoryInput

    def __init__(self, connections: ConnectionRegistry, timeout_s: float = 30.0) -> None:
        """Initialise the tool.

        Args:
            connections: Where the Airflow metadata connection comes from.
            timeout_s: Per-call timeout.
        """
        super().__init__(timeout_s)
        self._connections = connections

    async def execute(self, tool_input: DagRunHistoryInput) -> DagRunHistory:
        """Read the task's recent attempts."""
        executor = ReadOnlyExecutor(self._connections.engine("airflow"))
        statement = (
            select(
                TASK_INSTANCE.c.run_id,
                TASK_INSTANCE.c.state,
                TASK_INSTANCE.c.try_number,
                TASK_INSTANCE.c.duration,
                TASK_INSTANCE.c.start_date,
                TASK_INSTANCE.c.end_date,
            )
            .where(
                TASK_INSTANCE.c.dag_id == tool_input.dag_id,
                TASK_INSTANCE.c.task_id == tool_input.task_id,
            )
            .order_by(TASK_INSTANCE.c.start_date.desc())
            .limit(min(tool_input.limit, 100))
        )
        rows = await executor.fetch_all(statement)
        runs = [
            RunOutcome(
                run_id=str(row["run_id"]),
                state=_optional_str(row["state"]),
                try_number=_optional_int(row["try_number"]) or 1,
                duration_s=_optional_float(row["duration"]),
                start_date=_optional_datetime(row["start_date"]),
                end_date=_optional_datetime(row["end_date"]),
            )
            for row in rows
        ]
        return _summarise_history(tool_input.dag_id, tool_input.task_id, runs)


class TaskState(ToolOutput):
    """The state of one upstream task in this run."""

    task_id: str
    state: str | None
    duration_s: float | None
    end_date: datetime | None

    def summarise(self) -> str:
        """One line describing this task's state."""
        return f"{self.task_id}: {self.state}"


class UpstreamState(ToolOutput):
    """What the failing task's dependencies did in this run.

    ``root_failure`` is the point of the tool. When a task fails because two tasks upstream
    failed, attributing the incident to the visible symptom is the single most common way
    a pipeline diagnosis goes wrong.
    """

    dag_id: str
    task_id: str
    run_id: str
    upstream: list[TaskState]
    failed_upstream: list[str]
    root_failure: str | None
    all_upstream_succeeded: bool

    def summarise(self) -> str:
        """One line describing the upstream picture."""
        if not self.upstream:
            return f"{self.task_id} has no upstream tasks"
        if self.all_upstream_succeeded:
            return f"all {len(self.upstream)} upstream tasks of {self.task_id} succeeded"
        return (
            f"{len(self.failed_upstream)} upstream task(s) of {self.task_id} did not "
            f"succeed: {', '.join(self.failed_upstream)}; earliest is {self.root_failure}"
        )


class UpstreamStateInput(ToolInput):
    """Arguments for :class:`GetUpstreamTaskState`."""

    dag_id: str
    task_id: str
    run_id: str


class GetUpstreamTaskState(BaseTool[UpstreamStateInput, UpstreamState]):
    """Did a dependency fail, and if so which one actually broke first?"""

    name: ClassVar[str] = "get_upstream_task_state"
    description: ClassVar[str] = (
        "States of every task upstream of a failing task in the same run, transitively, "
        "with the earliest genuine failure identified. Use this before concluding that "
        "the failing task is itself at fault."
    )
    input_model = UpstreamStateInput

    def __init__(self, connections: ConnectionRegistry, timeout_s: float = 30.0) -> None:
        """Initialise the tool.

        Args:
            connections: Where the Airflow metadata connection comes from.
            timeout_s: Per-call timeout.
        """
        super().__init__(timeout_s)
        self._connections = connections

    async def execute(self, tool_input: UpstreamStateInput) -> UpstreamState:
        """Walk the dependency graph upward and read each task's state."""
        executor = ReadOnlyExecutor(self._connections.engine("airflow"))
        upstream_ids = await self._upstream_of(executor, tool_input.dag_id, tool_input.task_id)

        rows = await executor.fetch_all(
            select(
                TASK_INSTANCE.c.task_id,
                TASK_INSTANCE.c.state,
                TASK_INSTANCE.c.duration,
                TASK_INSTANCE.c.end_date,
            ).where(
                TASK_INSTANCE.c.dag_id == tool_input.dag_id,
                TASK_INSTANCE.c.run_id == tool_input.run_id,
            )
        )
        states = {
            str(row["task_id"]): TaskState(
                task_id=str(row["task_id"]),
                state=_optional_str(row["state"]),
                duration_s=_optional_float(row["duration"]),
                end_date=_optional_datetime(row["end_date"]),
            )
            for row in rows
            if str(row["task_id"]) in upstream_ids
        }

        upstream = [states[task_id] for task_id in sorted(states)]
        failed = [task.task_id for task in upstream if task.state in UNSUCCESSFUL_STATES]
        return UpstreamState(
            dag_id=tool_input.dag_id,
            task_id=tool_input.task_id,
            run_id=tool_input.run_id,
            upstream=upstream,
            failed_upstream=failed,
            root_failure=_earliest_genuine_failure(upstream),
            all_upstream_succeeded=not failed,
        )

    async def _upstream_of(self, executor: ReadOnlyExecutor, dag_id: str, task_id: str) -> set[str]:
        """Every task the given task depends on, directly or transitively.

        Airflow stores edges as ``downstream_task_ids`` on the serialised DAG, so the
        graph is inverted here rather than queried upward.

        Raises:
            TargetNotFoundError: If the DAG has no serialised form, which means the scheduler has
                not parsed it. That is a finding about the DAG, not a tool failure.
        """
        row = await executor.fetch_one(
            select(SERIALIZED_DAG.c.data).where(SERIALIZED_DAG.c.dag_id == dag_id)
        )
        if row is None or row["data"] is None:
            raise TargetNotFoundError(f"No serialised DAG stored for {dag_id!r}")

        downstream = _downstream_edges(row["data"])
        parents: dict[str, set[str]] = {}
        for parent, children in downstream.items():
            for child in children:
                parents.setdefault(child, set()).add(parent)

        seen: set[str] = set()
        frontier = list(parents.get(task_id, set()))
        while frontier:
            current = frontier.pop()
            if current in seen:
                continue
            seen.add(current)
            frontier.extend(parents.get(current, set()))
        return seen


def _downstream_edges(raw: object) -> dict[str, list[str]]:
    """Pull the task graph out of a serialised DAG, tolerating shape drift."""
    payload = json.loads(raw) if isinstance(raw, str | bytes) else raw
    if not isinstance(payload, dict):
        return {}
    dag = payload.get("dag")
    tasks = dag.get("tasks") if isinstance(dag, dict) else None
    if not isinstance(tasks, list):
        return {}

    edges: dict[str, list[str]] = {}
    for task in tasks:
        if not isinstance(task, dict):
            continue
        # Airflow has nested the task body under __var in some serialisation versions.
        nested = task.get("__var")
        body: dict[str, object] = nested if isinstance(nested, dict) else task
        task_id = body.get("task_id")
        children = body.get("downstream_task_ids")
        if isinstance(task_id, str) and isinstance(children, list):
            edges[task_id] = [child for child in children if isinstance(child, str)]
    return edges


def _summarise_history(dag_id: str, task_id: str, runs: list[RunOutcome]) -> DagRunHistory:
    """Turn a list of attempts into the shape the graph reasons about."""
    failures = [run for run in runs if run.state == "failed"]
    consecutive = 0
    for run in runs:
        if run.state != "failed":
            break
        consecutive += 1

    durations = [run.duration_s for run in runs if run.duration_s is not None]
    return DagRunHistory(
        dag_id=dag_id,
        task_id=task_id,
        runs=runs,
        failure_rate=len(failures) / len(runs) if runs else 0.0,
        consecutive_failures=consecutive,
        # One or two failures against a long clean record is a change; a task that fails
        # most of the time was already broken and the recent change is not the cause.
        is_new_failure=bool(runs) and consecutive <= 2 and len(failures) <= max(2, len(runs) // 4),
        median_duration_s=statistics.median(durations) if durations else None,
    )


def _earliest_genuine_failure(upstream: list[TaskState]) -> str | None:
    """The first task that actually failed, ignoring ones marked upstream_failed.

    ``upstream_failed`` is Airflow reporting a consequence. Following the consequences to
    their source is the whole job here.
    """
    genuine = [task for task in upstream if task.state == "failed"]
    if not genuine:
        return None
    dated = [task for task in genuine if task.end_date is not None]
    if not dated:
        return genuine[0].task_id
    return min(dated, key=lambda task: task.end_date or datetime.max).task_id


def _optional_int(value: object) -> int | None:
    """Render a nullable integer column as an int."""
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_str(value: object) -> str | None:
    """Render a nullable column as a string."""
    return str(value) if value is not None else None


def _optional_float(value: object) -> float | None:
    """Render a nullable numeric column as a float."""
    return float(value) if isinstance(value, int | float) else None


def _optional_datetime(value: object) -> datetime | None:
    """Pass a datetime through and ignore anything else."""
    return value if isinstance(value, datetime) else None
