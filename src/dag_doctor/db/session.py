"""Async engine and session factory for the agent's own database.

One engine per process, one session per unit of work. The worker's unit of work is a
single Kafka message, which is deliberate: the offset is committed only after the
transaction commits, so a crash between the two replays the message rather than losing it.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dag_doctor.core.settings import DatabaseSettings


def build_engine(settings: DatabaseSettings) -> AsyncEngine:
    """Create the async engine.

    Args:
        settings: DSN, pool size, and echo flag.

    Returns:
        An engine the caller is responsible for disposing.
    """
    return create_async_engine(
        settings.dsn,
        echo=settings.echo,
        pool_size=settings.pool_size,
        # A connection that has been idle since before a database restart fails on first
        # use rather than on checkout, which surfaces as a spurious diagnosis failure.
        pool_pre_ping=True,
    )


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create the session factory bound to an engine.

    Args:
        engine: The engine sessions should use.

    Returns:
        A session factory.
    """
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@asynccontextmanager
async def session_scope(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Run a unit of work in one transaction, committing on success.

    Args:
        session_factory: Where to get the session from.

    Yields:
        The session for the unit of work.
    """
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except BaseException:
            await session.rollback()
            raise
