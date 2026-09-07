"""A real Postgres, for the claims SQLite cannot check.

The unit suite runs the same models and the same migration on SQLite, which covers the
logic. What it cannot cover is JSONB, native enum types, and genuine write concurrency.
Those are what these fixtures exist for. Every test here needs Docker and is excluded from
the default run.
"""

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.postgres import PostgresContainer

ROOT = Path(__file__).parents[2]

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine", driver="psycopg") as container:
        yield container.get_connection_url()


@pytest.fixture
async def migrated_session_factory(
    postgres_dsn: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", postgres_dsn)
    command.downgrade(config, "base")
    command.upgrade(config, "head")

    engine = create_async_engine(postgres_dsn)
    yield async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    await engine.dispose()
