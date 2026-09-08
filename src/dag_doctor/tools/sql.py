"""Read-only query execution, enforced in code.

The database grants a SELECT-only role, which is the outer guarantee. This is the inner
one, and it exists because a demo repository gets cloned and reconfigured by people who
will not read the grant, and because a prompt injection in a log line should not be able
to turn a diagnosis into a write.

Three things are enforced here. Identifiers coming from a model are validated against a
strict pattern rather than interpolated, values are always bound parameters, and every
connection is opened in a read-only transaction with a statement timeout where the dialect
supports one.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self

from sqlalchemy import Executable, TextClause, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from dag_doctor.core.exceptions import ReadOnlyViolationError

#: Postgres identifiers are 63 bytes. Anything outside this is not a table name, it is an
#: attempt to put something else where a table name goes.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,62}$")

_READ_PREFIX = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)

#: Defence in depth for the few places a tool builds text SQL. Core constructs cannot
#: become DML by accident, but a hand-written string can.
_WRITE_KEYWORD = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|merge"
    r"|vacuum|reindex|refresh|lock|call|do|commit|rollback|savepoint)\b",
    re.IGNORECASE,
)

#: Ceiling on any single query, applied on top of each tool's own limit. A profiling
#: query that forgot its LIMIT should be slow, not fatal.
MAX_ROWS = 1000


@dataclass(frozen=True)
class TableRef:
    """A validated ``schema.table`` reference.

    Constructed only through :meth:`parse`, so an unvalidated string cannot reach a
    query by being passed where a ``TableRef`` was expected.
    """

    schema: str | None
    name: str

    @classmethod
    def parse(cls, raw: str) -> Self:
        """Validate a table name supplied by a caller.

        Args:
            raw: Either ``table`` or ``schema.table``.

        Returns:
            The validated reference.

        Raises:
            ReadOnlyViolationError: If either part is not a plain identifier. Quoting
                would not be enough on its own: refusing is simpler to reason about and
                costs nothing, because real table names are plain identifiers.
        """
        parts = raw.strip().split(".")
        if len(parts) > 2 or not all(_IDENTIFIER.match(part) for part in parts):
            raise ReadOnlyViolationError(
                f"{raw!r} is not a valid table name",
                details={"table": raw},
            )
        if len(parts) == 2:
            return cls(schema=parts[0], name=parts[1])
        return cls(schema=None, name=parts[0])

    @property
    def qualified(self) -> str:
        """The reference as written in SQL."""
        return f"{self.schema}.{self.name}" if self.schema else self.name

    def __str__(self) -> str:
        """The qualified name."""
        return self.qualified


def validate_identifier(raw: str, *, kind: str = "identifier") -> str:
    """Check that a caller-supplied name is a plain identifier.

    Args:
        raw: The name, usually a column, chosen by a model.
        kind: What it names, for the error message.

    Returns:
        The name unchanged.

    Raises:
        ReadOnlyViolationError: If it is not a plain identifier.
    """
    if not _IDENTIFIER.match(raw.strip()):
        raise ReadOnlyViolationError(
            f"{raw!r} is not a valid {kind}",
            details={kind: raw},
        )
    return raw.strip()


def require_read_only(sql: str) -> str:
    """Reject anything that is not a single read statement.

    Args:
        sql: The statement about to be executed.

    Returns:
        The statement, stripped of trailing whitespace and a trailing semicolon.

    Raises:
        ReadOnlyViolationError: If it is not a single SELECT or WITH.
    """
    stripped = sql.strip().rstrip(";").strip()
    if ";" in stripped:
        raise ReadOnlyViolationError(
            "Multiple statements in one query", details={"sql": stripped[:200]}
        )
    if not _READ_PREFIX.match(stripped):
        raise ReadOnlyViolationError(
            "Only SELECT and WITH statements may be executed",
            details={"sql": stripped[:200]},
        )
    if _WRITE_KEYWORD.search(stripped):
        raise ReadOnlyViolationError(
            "Statement contains a write keyword", details={"sql": stripped[:200]}
        )
    return stripped


def read_only_text(sql: str) -> TextClause:
    """Build a text clause after checking it is a read.

    Args:
        sql: The statement.

    Returns:
        The clause, ready to execute.
    """
    return text(require_read_only(sql))


class ReadOnlyExecutor:
    """Runs queries against one named connection, read-only and bounded."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        statement_timeout_s: float = 15.0,
        max_rows: int = MAX_ROWS,
    ) -> None:
        """Initialise the executor.

        Args:
            engine: The engine for the named connection.
            statement_timeout_s: Passed to the database where the dialect supports it.
            max_rows: Hard ceiling on rows returned by any one query.
        """
        self._engine = engine
        self._statement_timeout_s = statement_timeout_s
        self._max_rows = min(max_rows, MAX_ROWS)

    async def fetch_all(
        self,
        statement: Executable,
        params: Mapping[str, object] | None = None,
    ) -> list[dict[str, object]]:
        """Run a query and return its rows as dictionaries.

        Args:
            statement: A Core construct or a clause built by :func:`read_only_text`.
            params: Bound parameters. Values are always bound, never interpolated.

        Returns:
            Up to ``max_rows`` rows.
        """
        async with self._engine.connect() as connection, connection.begin():
            await self._harden(connection)
            result = await connection.execute(statement, dict(params or {}))
            rows = result.mappings().fetchmany(self._max_rows)
        return [dict(row) for row in rows]

    async def fetch_one(
        self,
        statement: Executable,
        params: Mapping[str, object] | None = None,
    ) -> dict[str, object] | None:
        """Run a query expected to return at most one row.

        Args:
            statement: A Core construct or a clause built by :func:`read_only_text`.
            params: Bound parameters.

        Returns:
            The row, or ``None``.
        """
        rows = await self.fetch_all(statement, params)
        return rows[0] if rows else None

    async def _harden(self, connection: AsyncConnection) -> None:
        """Make the transaction itself refuse writes, where the dialect can.

        On Postgres this is a genuine guarantee rather than a convention: even a bug in
        this package cannot write through a connection the server has marked read-only.
        SQLite, used by the tests, has no equivalent, which is why the checks above are
        not left to the database alone.
        """
        if connection.dialect.name != "postgresql":
            return
        await connection.execute(text("SET TRANSACTION READ ONLY"))
        timeout_ms = int(self._statement_timeout_s * 1000)
        await connection.execute(text(f"SET LOCAL statement_timeout = {timeout_ms}"))
