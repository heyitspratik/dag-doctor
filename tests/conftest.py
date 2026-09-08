"""Fixtures shared by the whole suite.

Nothing here may reach the network. No test in this repository is allowed to need an API
key or a running Ollama, so every LLM is scripted and every service is a fixture.
"""

import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dag_doctor.core import settings as settings_module
from dag_doctor.core.models import FailureEvent
from dag_doctor.core.settings import BudgetSettings
from dag_doctor.db.models import Base
from dag_doctor.graph.toolbox import Toolbox
from tests.fakes import make_tool

#: Environment variables the settings classes read. A developer's shell, or the .env file
#: sitting in the repository root, must never be able to make a test pass or fail.
_SETTINGS_PREFIXES = (
    "APP_ENV",
    "LOG_LEVEL",
    "API_KEY",
    "LLM_",
    "OLLAMA_",
    "ANTHROPIC_",
    "OPENAI_",
    "MAX_",
    "TOOL_",
    "POSTGRES_",
    "AIRFLOW_",
    "KAFKA_",
)


@pytest.fixture(autouse=True)
def hermetic_settings(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Isolate settings from the ambient environment and from the repository's .env.

    Changing directory is what handles the .env file: pydantic-settings resolves the
    relative env_file against the working directory, and the nested settings groups build
    themselves through default factories that would otherwise each reread it.
    """
    for name in list(os.environ):
        if name.startswith(_SETTINGS_PREFIXES):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    settings_module.get_settings.cache_clear()
    yield
    settings_module.get_settings.cache_clear()


@pytest.fixture
async def session_factory(tmp_path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A real database for the persistence tests, without needing a container.

    SQLite through aiosqlite, with the schema built from the same models Postgres uses.
    This exercises real SQL, real transactions, and real constraint enforcement on every
    commit. Postgres-specific behaviour is covered separately by the integration tests.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'test.db'}")

    # pysqlite emits its own BEGIN at the wrong moments, which breaks SAVEPOINT and so
    # breaks the repository's conflict handling. Taking transaction control away from the
    # driver is the documented fix, and it is what makes these tests represent Postgres.
    @event.listens_for(engine.sync_engine, "connect")
    def _disable_driver_transactions(dbapi_connection, _record):
        dbapi_connection.isolation_level = None

    @event.listens_for(engine.sync_engine, "begin")
    def _begin(connection):
        connection.exec_driver_sql("BEGIN")

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    yield async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

    await engine.dispose()


@pytest.fixture
def failure_event() -> FailureEvent:
    """The seeded schema-drift failure, used as the standard event across the suite."""
    return FailureEvent(
        dag_id="schema_drift_orders",
        task_id="build_orders_by_customer",
        run_id="manual__2026-09-07T10:00:00+00:00",
        try_number=1,
        exception_type="UndefinedColumn",
        exception_message='column "customer_id" does not exist',
    )


#: What the seeded schema-drift scenario's tools would report, used across the suites.
DRIFT_FINDING = "orders.customer_id appears to have been renamed to customer_uuid"


@pytest.fixture
def toolbox() -> Toolbox:
    """Stub tools covering a success, a refusal, and everything in between."""
    return Toolbox(
        [
            make_tool("fetch_task_logs", "UndefinedColumn: column customer_id does not exist"),
            make_tool("compare_schema_snapshot", DRIFT_FINDING),
            make_tool("get_dag_run_history", "newly failing: 1 of the last 11 runs failed"),
            make_tool("check_connection_health", "connection answered in 3ms"),
            make_tool("profile_table", "unavailable", fails=True),
        ]
    )


@pytest.fixture
def budgets() -> BudgetSettings:
    return BudgetSettings()
