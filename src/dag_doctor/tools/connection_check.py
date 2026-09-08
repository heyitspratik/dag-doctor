"""Is the database a pipeline depends on actually reachable?

Cheap to run and decisive when it fires: an unreachable dependency turns a puzzling
application error into transient infrastructure, which is a different diagnosis with a
different fix.
"""

import time
from typing import ClassVar

from dag_doctor.core.exceptions import ReadOnlyViolationError
from dag_doctor.tools.base import BaseTool, ToolInput, ToolOutput
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.sql import ReadOnlyExecutor, read_only_text


class ConnectionHealth(ToolOutput):
    """Whether a named connection answered, and how quickly."""

    connection: str
    reachable: bool
    latency_ms: int
    error: str | None = None

    def summarise(self) -> str:
        """One line describing the probe."""
        if self.reachable:
            return f"connection {self.connection!r} answered in {self.latency_ms}ms"
        return f"connection {self.connection!r} is unreachable: {self.error}"


class ConnectionHealthInput(ToolInput):
    """Arguments for :class:`CheckConnectionHealth`."""

    conn_id: str


class CheckConnectionHealth(BaseTool[ConnectionHealthInput, ConnectionHealth]):
    """Can the agent reach a configured connection at all?"""

    name: ClassVar[str] = "check_connection_health"
    description: ClassVar[str] = (
        "Probe a configured connection and report whether it answers and how quickly. "
        "Use it to separate a transient infrastructure problem from a code or data one."
    )
    input_model = ConnectionHealthInput

    def __init__(self, connections: ConnectionRegistry, timeout_s: float = 15.0) -> None:
        """Initialise the tool.

        Args:
            connections: The connections a tool may name.
            timeout_s: Per-call timeout.
        """
        super().__init__(timeout_s)
        self._connections = connections

    async def execute(self, tool_input: ConnectionHealthInput) -> ConnectionHealth:
        """Probe one connection.

        An unreachable connection is a successful tool call reporting a negative result,
        not a tool failure: the graph needs that answer as evidence.
        """
        started = time.perf_counter()
        try:
            engine = self._connections.engine(tool_input.conn_id)
        except ReadOnlyViolationError:
            # Naming an unregistered connection is a caller error, not a network fact, so
            # it stays an exception and surfaces as FORBIDDEN.
            raise

        try:
            await ReadOnlyExecutor(engine).fetch_one(read_only_text("SELECT 1 AS ok"))
        except Exception as exc:
            return ConnectionHealth(
                connection=tool_input.conn_id,
                reachable=False,
                latency_ms=int((time.perf_counter() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )
        return ConnectionHealth(
            connection=tool_input.conn_id,
            reachable=True,
            latency_ms=int((time.perf_counter() - started) * 1000),
        )
