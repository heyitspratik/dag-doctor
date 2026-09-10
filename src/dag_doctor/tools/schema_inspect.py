"""Reading a table's shape, and diffing it against what it used to be.

Reflection is used rather than hand-written information_schema queries, so these tools
work against any backend the warehouse might be and remain testable without one.

The diff is where the value is. A column list on its own says nothing; the same list next
to last week's says whether a contract changed, and a removed column beside an added one
of the same type is how a rename is recognised for what it is.
"""

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import ClassVar

from sqlalchemy import Connection, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dag_doctor.core.logging import get_logger
from dag_doctor.db.models import SchemaSnapshot
from dag_doctor.tools.base import BaseTool, TargetNotFoundError, ToolInput, ToolOutput
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.sql import TableRef

logger = get_logger(__name__)


class ColumnSpec(ToolOutput):
    """One column, as the database currently describes it."""

    name: str
    type: str
    nullable: bool

    def summarise(self) -> str:
        """One line describing this column."""
        return f"{self.name} {self.type}{'' if self.nullable else ' NOT NULL'}"


class TableSchema(ToolOutput):
    """A table's current columns."""

    connection: str
    table: str
    columns: list[ColumnSpec]

    def summarise(self) -> str:
        """One line describing the table's shape."""
        names = ", ".join(column.name for column in self.columns)
        return f"{self.table} has {len(self.columns)} columns: {names}"


class TableSchemaInput(ToolInput):
    """Arguments for :class:`InspectTableSchema`."""

    connection: str
    table: str


async def _reflect(
    connections: ConnectionRegistry, connection_name: str, table: TableRef
) -> list[ColumnSpec]:
    """Read a table's columns, raising if it is not there.

    Raises:
        TargetNotFoundError: If the table does not exist, which is often itself the finding.
    """
    engine = connections.engine(connection_name)

    def read(sync_connection: Connection) -> list[ColumnSpec]:
        inspector = inspect(sync_connection)
        if not inspector.has_table(table.name, schema=table.schema):
            raise TargetNotFoundError(f"{table.qualified} does not exist on {connection_name!r}")
        return [
            ColumnSpec(
                name=str(column["name"]),
                type=str(column["type"]),
                nullable=bool(column["nullable"]),
            )
            for column in inspector.get_columns(table.name, schema=table.schema)
        ]

    async with engine.connect() as connection:
        return await connection.run_sync(read)


class InspectTableSchema(BaseTool[TableSchemaInput, TableSchema]):
    """What columns does this table have right now?"""

    name: ClassVar[str] = "inspect_table_schema"
    description: ClassVar[str] = (
        "The current columns and types of a table. Use it to check whether a column a "
        "query selects actually exists."
    )
    input_model = TableSchemaInput

    def __init__(self, connections: ConnectionRegistry, timeout_s: float = 30.0) -> None:
        """Initialise the tool.

        Args:
            connections: The connections a tool may name.
            timeout_s: Per-call timeout.
        """
        super().__init__(timeout_s)
        self._connections = connections

    async def execute(self, tool_input: TableSchemaInput) -> TableSchema:
        """Reflect the table's columns."""
        table = TableRef.parse(tool_input.table)
        columns = await _reflect(self._connections, tool_input.connection, table)
        return TableSchema(connection=tool_input.connection, table=table.qualified, columns=columns)


class RenamedColumn(ToolOutput):
    """A column that looks renamed rather than dropped and replaced."""

    likely_from: str
    likely_to: str
    type: str

    def summarise(self) -> str:
        """One line describing the suspected rename."""
        return f"{self.likely_from} appears to have been renamed to {self.likely_to}"


class ChangedColumn(ToolOutput):
    """A column whose type or nullability moved."""

    name: str
    was: str
    now: str

    def summarise(self) -> str:
        """One line describing the change."""
        return f"{self.name} changed from {self.was} to {self.now}"


class SchemaDiff(ToolOutput):
    """How a table's shape has changed since it was last seen."""

    connection: str
    table: str
    baseline_captured_at: datetime | None
    added: list[str]
    removed: list[str]
    changed: list[ChangedColumn]
    likely_renames: list[RenamedColumn]
    has_drift: bool
    baseline_created: bool
    #: Snapshots exist for this table, but none from on or before the requested moment.
    baseline_too_recent: bool = False

    def summarise(self) -> str:
        """One line describing the drift, or its absence."""
        if self.baseline_created:
            return f"no prior snapshot of {self.table}; recorded one for next time"
        if self.baseline_too_recent:
            return (
                f"{self.table} has snapshots, but none from that far back; "
                f"nothing to compare against without widening as_of"
            )
        if not self.has_drift:
            return f"{self.table} is unchanged since {self.baseline_captured_at}"
        if self.likely_renames:
            renames = ", ".join(item.summarise() for item in self.likely_renames)
            return f"{self.table} drifted: {renames}"
        return (
            f"{self.table} drifted: {len(self.added)} added, {len(self.removed)} removed, "
            f"{len(self.changed)} changed"
        )


class SchemaSnapshotInput(ToolInput):
    """Arguments for :class:`CompareSchemaSnapshot`."""

    connection: str
    table: str
    as_of: datetime | None = None


class CompareSchemaSnapshot(BaseTool[SchemaSnapshotInput, SchemaDiff]):
    """Has this table's shape changed since we last looked?"""

    name: ClassVar[str] = "compare_schema_snapshot"
    description: ClassVar[str] = (
        "Diff a table's current columns against a stored snapshot, naming added, removed "
        "and retyped columns and identifying likely renames. This is what turns 'a column "
        "is missing' into 'the upstream contract changed'."
    )
    input_model = SchemaSnapshotInput

    def __init__(
        self,
        connections: ConnectionRegistry,
        session_factory: async_sessionmaker[AsyncSession],
        timeout_s: float = 30.0,
    ) -> None:
        """Initialise the tool.

        Args:
            connections: The connections a tool may name.
            session_factory: The agent's own database, where snapshots live. The agent is
                read-only against the warehouse; its own memory is what it writes.
            timeout_s: Per-call timeout.
        """
        super().__init__(timeout_s)
        self._connections = connections
        self._session_factory = session_factory

    async def execute(self, tool_input: SchemaSnapshotInput) -> SchemaDiff:
        """Compare the live table against the most recent stored snapshot."""
        table = TableRef.parse(tool_input.table)
        current = await _reflect(self._connections, tool_input.connection, table)

        async with self._session_factory() as session:
            baseline = await self._latest_snapshot(
                session, tool_input.connection, table.qualified, tool_input.as_of
            )
            if baseline is None:
                # Only bootstrap when the table has never been seen. If snapshots exist
                # and as_of simply excludes them, recording the current shape would
                # overwrite the very baseline the caller was asking to compare against,
                # and every later comparison would report no drift. That is how a real
                # rename went undetected for a whole afternoon.
                if await self._has_any_snapshot(session, tool_input.connection, table.qualified):
                    return _no_baseline(tool_input.connection, table.qualified)

                await self._record(session, tool_input.connection, table.qualified, current)
                await session.commit()
                return SchemaDiff(
                    connection=tool_input.connection,
                    table=table.qualified,
                    baseline_captured_at=None,
                    added=[],
                    removed=[],
                    changed=[],
                    likely_renames=[],
                    has_drift=False,
                    baseline_created=True,
                )

        return _diff(tool_input.connection, table.qualified, baseline, current)

    async def _latest_snapshot(
        self,
        session: AsyncSession,
        connection: str,
        table: str,
        as_of: datetime | None,
    ) -> SchemaSnapshot | None:
        """The most recent snapshot at or before a moment."""
        statement = (
            select(SchemaSnapshot)
            .where(SchemaSnapshot.connection == connection, SchemaSnapshot.table_name == table)
            .order_by(SchemaSnapshot.captured_at.desc())
            .limit(1)
        )
        if as_of is not None:
            statement = statement.where(SchemaSnapshot.captured_at <= as_of)
        return (await session.execute(statement)).scalar_one_or_none()

    async def _has_any_snapshot(self, session: AsyncSession, connection: str, table: str) -> bool:
        """Whether this table has ever been snapshotted, ignoring any as_of window."""
        statement = (
            select(SchemaSnapshot.id)
            .where(SchemaSnapshot.connection == connection, SchemaSnapshot.table_name == table)
            .limit(1)
        )
        return (await session.execute(statement)).scalar_one_or_none() is not None

    async def _record(
        self, session: AsyncSession, connection: str, table: str, columns: list[ColumnSpec]
    ) -> None:
        """Store the current shape so the next comparison has something to compare to."""
        record_snapshot(session, connection, table, columns)


def record_snapshot(
    session: AsyncSession, connection: str, table: str, columns: list[ColumnSpec]
) -> None:
    """Write a snapshot of a table's current shape.

    Separate from the tool because establishing a baseline and diffing against one are
    different jobs. The tool only records when nothing is there, which is right for a diff
    and wrong for a baseline: a stale snapshot would silently survive and every later
    comparison would be against the wrong reference.

    Args:
        session: A session on the agent's own database.
        connection: The named connection the table lives on.
        table: The qualified table name.
        columns: Its columns as they are now.
    """
    session.add(
        SchemaSnapshot(
            connection=connection,
            table_name=table,
            columns=[column.model_dump() for column in columns],
            captured_at=datetime.now(UTC),
        )
    )
    logger.info("schema.snapshot_recorded", connection=connection, table=table)


async def reflect_columns(
    connections: ConnectionRegistry, connection: str, table: str
) -> list[ColumnSpec]:
    """Read a table's current columns.

    Args:
        connections: The connections a tool may name.
        connection: Which one to read.
        table: The table, as ``schema.table`` or ``table``.

    Returns:
        Its columns.
    """
    return await _reflect(connections, connection, TableRef.parse(table))


def _no_baseline(connection: str, table: str) -> SchemaDiff:
    """Report that no snapshot is old enough, without writing a misleading one."""
    return SchemaDiff(
        connection=connection,
        table=table,
        baseline_captured_at=None,
        added=[],
        removed=[],
        changed=[],
        likely_renames=[],
        has_drift=False,
        baseline_created=False,
        baseline_too_recent=True,
    )


def _diff(
    connection: str, table: str, baseline: SchemaSnapshot, current: list[ColumnSpec]
) -> SchemaDiff:
    """Compare two column lists and interpret the difference."""
    was = {
        str(column["name"]): column
        for column in baseline.columns
        if isinstance(column, dict) and isinstance(column.get("name"), str)
    }
    now = {column.name: column for column in current}

    added = sorted(set(now) - set(was))
    removed = sorted(set(was) - set(now))
    changed = [
        ChangedColumn(name=name, was=str(was[name].get("type")), now=now[name].type)
        for name in sorted(set(was) & set(now))
        if str(was[name].get("type")) != now[name].type
    ]
    return SchemaDiff(
        connection=connection,
        table=table,
        baseline_captured_at=baseline.captured_at,
        added=added,
        removed=removed,
        changed=changed,
        likely_renames=_likely_renames(was, now, added, removed),
        has_drift=bool(added or removed or changed),
        baseline_created=False,
    )


def _likely_renames(
    was: Mapping[str, object],
    now: Mapping[str, ColumnSpec],
    added: list[str],
    removed: list[str],
) -> list[RenamedColumn]:
    """Pair a removed column with an added one of the same type.

    Not proof, and named as a likelihood rather than a fact. It is the difference between
    telling someone a column vanished and telling them what to look for.
    """
    renames: list[RenamedColumn] = []
    available = list(added)
    for gone in removed:
        old = was[gone]
        old_type = str(old.get("type")) if isinstance(old, dict) else ""
        match = next((name for name in available if now[name].type == old_type), None)
        if match is not None:
            available.remove(match)
            renames.append(RenamedColumn(likely_from=gone, likely_to=match, type=old_type))
    return renames
