"""Read-only enforcement, checked against the database that actually enforces it.

The unit suite runs these tools on SQLite, which has no read-only transaction mode. The
claim that a bug in this package could not write through a tool connection is therefore
only testable here.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, InternalError, ProgrammingError

from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.sql import ReadOnlyExecutor, read_only_text

pytestmark = pytest.mark.integration


@pytest.fixture
async def warehouse(postgres_dsn: str):
    registry = ConnectionRegistry({"warehouse": postgres_dsn})
    engine = registry.engine("warehouse")
    async with engine.begin() as connection:
        await connection.execute(text("CREATE TABLE IF NOT EXISTS probe (id int)"))
        await connection.execute(text("INSERT INTO probe VALUES (1)"))
    yield registry
    await registry.dispose()


async def test_a_read_goes_through(warehouse):
    executor = ReadOnlyExecutor(warehouse.engine("warehouse"))

    row = await executor.fetch_one(read_only_text("SELECT id FROM probe"))

    assert row == {"id": 1}


async def test_the_transaction_itself_refuses_a_write(warehouse):
    # Not the keyword scan, which is bypassed here on purpose: this is Postgres refusing
    # because the transaction was opened read-only. It is the guarantee that survives a
    # bug in this package.
    executor = ReadOnlyExecutor(warehouse.engine("warehouse"))

    with pytest.raises((ProgrammingError, InternalError, DBAPIError)) as excinfo:
        await executor.fetch_all(text("INSERT INTO probe VALUES (2)"))

    assert "read-only" in str(excinfo.value).lower()


async def test_a_statement_that_runs_too_long_is_cut_off(warehouse):
    # Without a statement timeout one hung query consumes the whole tool budget.
    executor = ReadOnlyExecutor(warehouse.engine("warehouse"), statement_timeout_s=0.1)

    with pytest.raises(DBAPIError) as excinfo:
        await executor.fetch_all(read_only_text("SELECT pg_sleep(5)"))

    assert "statement timeout" in str(excinfo.value).lower()


async def test_the_row_ceiling_is_applied(warehouse):
    executor = ReadOnlyExecutor(warehouse.engine("warehouse"), max_rows=10)

    rows = await executor.fetch_all(read_only_text("SELECT generate_series(1, 5000) AS n"))

    assert len(rows) == 10
