"""Profiling a table, and comparing it against what it used to look like.

A profile on its own is weak evidence. "This column is 90% null" could be normal. "This
column was 2% null a week ago and is 90% null now" is a finding, and it is the difference
between blaming the code that broke and the data that changed under it.

Profiles are sampled rather than scanned. A full scan of a fact table would time out, and
the null fraction of a random sample is close enough to route an investigation.
"""

from datetime import UTC, datetime
from typing import Any, ClassVar

from sqlalchemy import (
    Connection,
    Float,
    MetaData,
    Table,
    case,
    cast,
    func,
    inspect,
    literal,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql import ColumnElement

from dag_doctor.core.logging import get_logger
from dag_doctor.db.models import TableProfile as TableProfileRow
from dag_doctor.tools.base import BaseTool, TargetNotFoundError, ToolInput, ToolOutput
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.sql import ReadOnlyExecutor, TableRef, validate_identifier

logger = get_logger(__name__)

#: Rows drawn for the per-column statistics. Large enough for a null fraction to mean
#: something, small enough to finish inside a tool timeout on a large table.
DEFAULT_SAMPLE_ROWS = 5000
MAX_SAMPLE_ROWS = 50_000

#: A null fraction that moves by more than this against the baseline is called out. Set
#: from what a regression actually looks like: a column that quietly goes from a few
#: percent to most of its rows, rather than a sampling wobble.
NULL_FRACTION_ALERT = 0.2


class ColumnProfile(ToolOutput):
    """One column's shape in the sample."""

    name: str
    null_fraction: float
    distinct_count: int
    min_value: str | None = None
    max_value: str | None = None
    baseline_null_fraction: float | None = None

    @property
    def null_fraction_delta(self) -> float | None:
        """How far the null fraction has moved since the baseline."""
        if self.baseline_null_fraction is None:
            return None
        return self.null_fraction - self.baseline_null_fraction

    def summarise(self) -> str:
        """One line describing this column."""
        delta = self.null_fraction_delta
        movement = f", was {self.baseline_null_fraction:.0%}" if delta is not None else ""
        return (
            f"{self.name}: {self.null_fraction:.0%} null{movement}, {self.distinct_count} distinct"
        )


class TableProfile(ToolOutput):
    """A table's row count and per-column statistics, against its own history."""

    connection: str
    table: str
    row_count: int
    sampled_rows: int
    columns: list[ColumnProfile]
    baseline_captured_at: datetime | None
    baseline_created: bool
    regressed_columns: list[str]

    def summarise(self) -> str:
        """One line describing the profile, leading with any regression."""
        if self.regressed_columns:
            worst = ", ".join(self.regressed_columns)
            return f"{self.table} has {self.row_count} rows; null fractions jumped on: {worst}"
        if self.baseline_created:
            return (
                f"{self.table} has {self.row_count} rows; no prior profile, recorded one "
                f"for next time"
            )
        return f"{self.table} has {self.row_count} rows and no null fraction regressions"


class ProfileTableInput(ToolInput):
    """Arguments for :class:`ProfileTable`."""

    connection: str
    table: str
    columns: list[str] | None = None
    sample_rows: int = DEFAULT_SAMPLE_ROWS


class ProfileTable(BaseTool[ProfileTableInput, TableProfile]):
    """Has the data changed shape, even though the schema has not?"""

    name: ClassVar[str] = "profile_table"
    description: ClassVar[str] = (
        "Row count, null fractions, distinct counts and ranges for a table's columns, "
        "compared against the last profile taken. Use it when a query runs but the data "
        "looks wrong, or when a not-null constraint started failing."
    )
    input_model = ProfileTableInput

    def __init__(
        self,
        connections: ConnectionRegistry,
        session_factory: async_sessionmaker[AsyncSession],
        timeout_s: float = 30.0,
    ) -> None:
        """Initialise the tool.

        Args:
            connections: The connections a tool may name.
            session_factory: The agent's own database, where baselines live.
            timeout_s: Per-call timeout.
        """
        super().__init__(timeout_s)
        self._connections = connections
        self._session_factory = session_factory

    async def execute(self, tool_input: ProfileTableInput) -> TableProfile:
        """Profile the table and compare it against the stored baseline."""
        table = TableRef.parse(tool_input.table)
        reflected = await self._reflect(tool_input.connection, table)
        wanted = self._requested_columns(tool_input, reflected)

        executor = ReadOnlyExecutor(self._connections.engine(tool_input.connection))
        row_count = await self._row_count(executor, reflected)
        sample_size = min(max(tool_input.sample_rows, 1), MAX_SAMPLE_ROWS)
        columns = await self._profile_columns(executor, reflected, wanted, sample_size)

        async with self._session_factory() as session:
            baseline = await self._latest_baseline(session, tool_input.connection, table.qualified)
            _apply_baseline(columns, baseline)
            await self._record(
                session, tool_input.connection, table.qualified, row_count, sample_size, columns
            )
            await session.commit()

        return TableProfile(
            connection=tool_input.connection,
            table=table.qualified,
            row_count=row_count,
            sampled_rows=min(sample_size, row_count),
            columns=columns,
            baseline_captured_at=baseline.captured_at if baseline else None,
            baseline_created=baseline is None,
            regressed_columns=[
                column.name
                for column in columns
                if (column.null_fraction_delta or 0.0) > NULL_FRACTION_ALERT
            ],
        )

    async def _reflect(self, connection_name: str, table: TableRef) -> Table:
        """Reflect the table so queries are built from real columns, never from strings.

        Reflecting rather than interpolating is the security property here: a column name
        that is not on the table cannot reach a query at all.

        Raises:
            TargetNotFoundError: If the table does not exist.
        """
        metadata = MetaData()

        def read(sync_connection: Connection) -> Table:
            inspector = inspect(sync_connection)
            if not inspector.has_table(table.name, schema=table.schema):
                raise TargetNotFoundError(f"{table.qualified} does not exist")
            return Table(table.name, metadata, schema=table.schema, autoload_with=sync_connection)

        async with self._connections.engine(connection_name).connect() as connection:
            return await connection.run_sync(read)

    def _requested_columns(self, tool_input: ProfileTableInput, table: Table) -> list[str]:
        """Decide which columns to profile, refusing names that are not columns.

        Raises:
            TargetNotFoundError: If a requested column is not on the table.
        """
        present = {column.name for column in table.columns}
        if tool_input.columns is None:
            return sorted(present)
        wanted = [validate_identifier(name, kind="column") for name in tool_input.columns]
        missing = [name for name in wanted if name not in present]
        if missing:
            raise TargetNotFoundError(f"{table.name} has no column(s) {', '.join(missing)}")
        return wanted

    async def _row_count(self, executor: ReadOnlyExecutor, table: Table) -> int:
        """Count the table's rows."""
        row = await executor.fetch_one(select(func.count()).select_from(table))
        return _as_int(next(iter(row.values()))) if row else 0

    async def _profile_columns(
        self, executor: ReadOnlyExecutor, table: Table, wanted: list[str], sample_rows: int
    ) -> list[ColumnProfile]:
        """Compute per-column statistics over one bounded sample."""
        # One query over one sample, so every column's statistics describe the same rows.
        # Profiling column by column would re-sample each time and the numbers would not
        # be comparable with each other.
        sample = select(table).limit(sample_rows).subquery("sample")
        aggregates: list[ColumnElement[Any]] = [func.count().label("sampled")]
        for name in wanted:
            column = sample.c[name]
            aggregates.extend(
                [
                    func.sum(_null_indicator(column)).label(f"nulls__{name}"),
                    func.count(column.distinct()).label(f"distinct__{name}"),
                    func.min(column).label(f"min__{name}"),
                    func.max(column).label(f"max__{name}"),
                ]
            )
        row = await executor.fetch_one(select(*aggregates).select_from(sample))
        if row is None:
            return []

        sampled = _as_int(row["sampled"])
        return [
            ColumnProfile(
                name=name,
                null_fraction=(_as_float(row[f"nulls__{name}"]) / sampled) if sampled else 0.0,
                distinct_count=_as_int(row[f"distinct__{name}"]),
                min_value=_render(row[f"min__{name}"]),
                max_value=_render(row[f"max__{name}"]),
            )
            for name in wanted
        ]

    async def _latest_baseline(
        self, session: AsyncSession, connection: str, table: str
    ) -> TableProfileRow | None:
        """The most recent stored profile for this table."""
        statement = (
            select(TableProfileRow)
            .where(
                TableProfileRow.connection == connection,
                TableProfileRow.table_name == table,
            )
            .order_by(TableProfileRow.captured_at.desc())
            .limit(1)
        )
        return (await session.execute(statement)).scalar_one_or_none()

    async def _record(
        self,
        session: AsyncSession,
        connection: str,
        table: str,
        row_count: int,
        sampled: int,
        columns: list[ColumnProfile],
    ) -> None:
        """Store this profile so the next call has a baseline."""
        session.add(
            TableProfileRow(
                connection=connection,
                table_name=table,
                row_count=row_count,
                sampled_rows=sampled,
                columns=[
                    column.model_dump(exclude={"baseline_null_fraction"}) for column in columns
                ],
                captured_at=datetime.now(UTC),
            )
        )
        logger.info("profile.recorded", connection=connection, table=table, rows=row_count)


def _null_indicator(column: ColumnElement[Any]) -> ColumnElement[float]:
    """One when the value is null, zero otherwise.

    Summing this is portable, unlike ``count(*) - count(column)`` arithmetic, which
    returns different types across dialects.
    """
    return cast(case((column.is_(None), literal(1)), else_=literal(0)), Float)


def _as_int(value: object) -> int:
    """Coerce a driver value to an int, treating anything unexpected as zero."""
    return int(value) if isinstance(value, int | float) and not isinstance(value, bool) else 0


def _as_float(value: object) -> float:
    """Coerce a driver value to a float, treating anything unexpected as zero."""
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else 0.0


def _apply_baseline(columns: list[ColumnProfile], baseline: TableProfileRow | None) -> None:
    """Attach each column's previous null fraction, where one was recorded."""
    if baseline is None:
        return
    previous = {
        str(entry["name"]): entry
        for entry in baseline.columns
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
    }
    for column in columns:
        entry = previous.get(column.name)
        if entry is not None and isinstance(entry.get("null_fraction"), int | float):
            column.baseline_null_fraction = float(entry["null_fraction"])


def _render(value: object) -> str | None:
    """Render a min or max value as a short string."""
    if value is None:
        return None
    return str(value)[:120]
