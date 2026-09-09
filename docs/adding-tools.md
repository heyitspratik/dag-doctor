# Adding a tool

Tools are the part of this agent most worth extending, and the contract is small enough
that a useful one is about twenty lines. Everything that must hold for every tool,
validation, the timeout, and turning a fault into a typed result, happens in the base class
so a new tool cannot forget it.

## A worked example

Say you want the agent to check whether a DAG is paused, because a paused DAG explains a
missing upstream partition better than anything the data can tell you.

```python
# src/dag_doctor/tools/dag_state.py
from typing import ClassVar

from sqlalchemy import select

from dag_doctor.tools.airflow_metadata import DAG_TABLE
from dag_doctor.tools.base import BaseTool, TargetNotFoundError, ToolInput, ToolOutput
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.sql import ReadOnlyExecutor


class DagStateInput(ToolInput):
    """Arguments for :class:`GetDagState`."""

    dag_id: str


class DagState(ToolOutput):
    """Whether Airflow would run this DAG at all."""

    dag_id: str
    is_paused: bool

    def summarise(self) -> str:
        """One line describing the DAG's state."""
        return f"{self.dag_id} is {'paused' if self.is_paused else 'active'}"


class GetDagState(BaseTool[DagStateInput, DagState]):
    """Is this DAG paused?"""

    name: ClassVar[str] = "get_dag_state"
    description: ClassVar[str] = (
        "Whether a DAG is paused. A paused upstream DAG explains missing data better "
        "than any amount of profiling will."
    )
    input_model = DagStateInput

    def __init__(self, connections: ConnectionRegistry, timeout_s: float = 30.0) -> None:
        """Initialise the tool."""
        super().__init__(timeout_s)
        self._connections = connections

    async def execute(self, tool_input: DagStateInput) -> DagState:
        """Read the DAG's paused flag.

        Raises:
            TargetNotFoundError: If Airflow knows no such DAG.
        """
        executor = ReadOnlyExecutor(self._connections.engine("airflow"))
        row = await executor.fetch_one(
            select(DAG_TABLE.c.is_paused).where(DAG_TABLE.c.dag_id == tool_input.dag_id)
        )
        if row is None:
            raise TargetNotFoundError(f"Airflow has no DAG named {tool_input.dag_id!r}")
        return DagState(dag_id=tool_input.dag_id, is_paused=bool(row["is_paused"]))
```

Register it in [`tools/factory.py`](../src/dag_doctor/tools/factory.py) and it appears in
the catalogue the model chooses from, with its argument schema, automatically.

## The rules, and why each exists

**Inputs are a Pydantic model with `extra="forbid"`.** The arguments come from a language
model. Silently accepting a field the tool does not read turns a hallucinated parameter
into a query that answers a different question than the one asked.

**Outputs are a typed model with a `summarise()`.** No tool returns a raw string blob. The
graph reasons over fields, and the one-line summary is what goes on the evidence record so
a later node can see what a tool found without unpacking its payload.

**Never raise into the graph.** Return a typed failure instead. A tool that cannot answer
is evidence the investigation should weigh, not a reason to abandon an incident that other
tools could still have explained. `BaseTool.run` handles this: it catches everything and
maps it to a status. Use `TargetNotFoundError` when the thing asked about does not exist,
since absence is usually a finding rather than a fault.

**Read-only, and enforced in code.** Take a connection *name*, never a DSN, so the model
cannot point a tool at a host of its choosing. Build queries from reflected columns or
validated identifiers rather than interpolating strings. `ReadOnlyExecutor` opens the
transaction read-only and applies a statement timeout on Postgres, so even a bug in your
tool cannot write.

**Write only to the agent's own database.** Tools that need history, like
`compare_schema_snapshot` and `profile_table`, record their own baselines there. That is
the one thing the agent writes, and it is what turns "this column is 90% null" into "this
column was 2% null and is now 90%".

## Testing it

Every database-backed tool is tested against a fixture database built from the same table
projection the tools query, so a column the tool reads but the fixture lacks fails loudly.
See [`tests/unit/tools/conftest.py`](../tests/unit/tools/conftest.py). No test may need
Docker, an API key, or a running Ollama.

Worth covering: the happy path, a missing target, a refused connection, and an argument
that is an injection attempt.

## Writing the description

The description is a prompt. It is what a model reads when deciding whether your tool is
worth a call from a budget of twenty. Say what question it answers and when it is worth
asking, not what it queries:

> Recent outcomes, durations and failure rate for one task. Answers whether this failure is
> new or long-standing, which decides whether to look for a recent change at all.

rather than "queries the task_instance table".
