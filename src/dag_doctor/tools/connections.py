"""The set of databases a tool is allowed to touch.

Tools take a connection *name*, never a DSN. The model can therefore ask for
``"warehouse"`` and be served, or ask for anything else and be refused; it cannot point a
tool at a host of its choosing. This is the reason the connection argument is a name in
every tool signature.
"""

from collections.abc import Mapping

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dag_doctor.core.exceptions import ReadOnlyViolationError
from dag_doctor.core.settings import Settings


class ConnectionRegistry:
    """Named, read-only connections, opened once and shared."""

    def __init__(self, dsns: Mapping[str, str]) -> None:
        """Initialise the registry.

        Args:
            dsns: Connection name to DSN. Every DSN must name a SELECT-only role.
        """
        self._dsns = dict(dsns)
        self._engines: dict[str, AsyncEngine] = {}

    @classmethod
    def from_settings(cls, settings: Settings) -> "ConnectionRegistry":
        """Build the registry the application actually uses.

        Args:
            settings: Application settings.

        Returns:
            A registry over Airflow's metadata database and the warehouse.
        """
        return cls({"airflow": settings.airflow.dsn, "warehouse": settings.warehouse_dsn})

    def names(self) -> list[str]:
        """Every connection a tool may name."""
        return sorted(self._dsns)

    def engine(self, name: str) -> AsyncEngine:
        """Return the engine for a named connection.

        Args:
            name: The connection name a tool was given.

        Returns:
            The engine, created on first use.

        Raises:
            ReadOnlyViolationError: If the name is not registered. Refusing here is what
                stops a tool argument from becoming a connection string.
        """
        if name not in self._dsns:
            # The alternatives go in the message, not only in the details: this refusal
            # is read by a model, and one that is not told the valid names will guess
            # again rather than pick one.
            raise ReadOnlyViolationError(
                f"Connection {name!r} is not one this agent may read. "
                f"Allowed: {', '.join(self.names())}",
                details={"connection": name, "allowed": self.names()},
            )
        if name not in self._engines:
            self._engines[name] = create_async_engine(
                self._dsns[name], **_pool_options(name and self._dsns[name])
            )
        return self._engines[name]

    async def dispose(self) -> None:
        """Close every engine this registry opened."""
        for engine in self._engines.values():
            await engine.dispose()
        self._engines.clear()


def _pool_options(dsn: str) -> dict[str, object]:
    """Engine options appropriate to the backend.

    Pool sizing is a server-database concern. SQLite, which the tool tests run against,
    does not use a queue pool and rejects these arguments outright.
    """
    options: dict[str, object] = {"pool_pre_ping": True}
    if make_url(dsn).get_backend_name() != "sqlite":
        # Small on purpose: these connections serve short read queries, and a large pool
        # per named connection would multiply across every worker replica.
        options |= {"pool_size": 2, "max_overflow": 2}
    return options
