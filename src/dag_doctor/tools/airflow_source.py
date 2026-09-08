"""Reading the DAG file Airflow actually parsed.

Reading the file from disk would read whatever is there now. Airflow stores the source it
parsed, which is the version that produced this failure, and those differ exactly when it
matters most: right after someone deployed a change.
"""

from typing import ClassVar

from sqlalchemy import select

from dag_doctor.tools.airflow_metadata import DAG_CODE, DAG_TABLE
from dag_doctor.tools.base import BaseTool, TargetNotFoundError, ToolInput, ToolOutput
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.sql import ReadOnlyExecutor

#: A DAG file longer than this is truncated before it reaches a prompt. Whole files are
#: rarely needed and a long one crowds out the evidence.
MAX_SOURCE_CHARS = 20_000


class DagSource(ToolOutput):
    """The parsed source of one DAG file."""

    dag_id: str
    fileloc: str
    source: str
    line_count: int
    truncated: bool

    def summarise(self) -> str:
        """One line describing the source that was read."""
        suffix = " (truncated)" if self.truncated else ""
        return f"{self.fileloc}: {self.line_count} lines{suffix}"


class DagSourceInput(ToolInput):
    """Arguments for :class:`GetDagSource`."""

    dag_id: str


class GetDagSource(BaseTool[DagSourceInput, DagSource]):
    """What does the failing DAG actually say?"""

    name: ClassVar[str] = "get_dag_source"
    description: ClassVar[str] = (
        "The source of the DAG file as Airflow parsed it, which is the version that "
        "produced this failure. Use it to read the failing operator's real query or "
        "arguments instead of guessing at them."
    )
    input_model = DagSourceInput

    def __init__(self, connections: ConnectionRegistry, timeout_s: float = 30.0) -> None:
        """Initialise the tool.

        Args:
            connections: Where the Airflow metadata connection comes from.
            timeout_s: Per-call timeout.
        """
        super().__init__(timeout_s)
        self._connections = connections

    async def execute(self, tool_input: DagSourceInput) -> DagSource:
        """Fetch the stored source for a DAG.

        Raises:
            TargetNotFoundError: If Airflow knows no such DAG, or has stored no source for it.
        """
        executor = ReadOnlyExecutor(self._connections.engine("airflow"))
        dag_row = await executor.fetch_one(
            select(DAG_TABLE.c.fileloc).where(DAG_TABLE.c.dag_id == tool_input.dag_id)
        )
        if dag_row is None or dag_row["fileloc"] is None:
            raise TargetNotFoundError(f"Airflow has no DAG named {tool_input.dag_id!r}")

        fileloc = str(dag_row["fileloc"])
        code_row = await executor.fetch_one(
            select(DAG_CODE.c.source_code).where(DAG_CODE.c.fileloc == fileloc)
        )
        if code_row is None or code_row["source_code"] is None:
            raise TargetNotFoundError(f"No stored source for {fileloc}")

        source = str(code_row["source_code"])
        truncated = len(source) > MAX_SOURCE_CHARS
        return DagSource(
            dag_id=tool_input.dag_id,
            fileloc=fileloc,
            source=source[:MAX_SOURCE_CHARS],
            line_count=source.count("\n") + 1,
            truncated=truncated,
        )
