"""Fixture databases for the tool tests.

Every tool that reads a database is exercised against a real one here: a SQLite stand-in
for Airflow's metadata database and for the warehouse, built from the same table
projection the tools query, so a column the tools read but the fixture lacks fails loudly.

Nothing here needs Docker, a running Airflow, or a model.
"""

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, Text, insert
from sqlalchemy import DateTime as SADateTime
from sqlalchemy.ext.asyncio import create_async_engine

from dag_doctor.tools.airflow_metadata import (
    AIRFLOW,
    DAG_CODE,
    DAG_TABLE,
    SERIALIZED_DAG,
    TASK_INSTANCE,
)
from dag_doctor.tools.connections import ConnectionRegistry

WAREHOUSE = MetaData()

ORDERS = Table(
    "orders",
    WAREHOUSE,
    Column("order_id", String, primary_key=True),
    Column("customer_uuid", String),
    Column("order_ts", SADateTime(timezone=True)),
    Column("amount_cents", Integer),
    Column("status", Text),
)

DAG_FILE = '''"""The seeded schema drift scenario."""

BUILD = """
SELECT customer_id, count(*) FROM raw.orders GROUP BY customer_id
"""
'''

SERIALISED = json.dumps(
    {
        "dag": {
            "dag_id": "schema_drift_orders",
            "tasks": [
                {
                    "__var": {
                        "task_id": "extract_orders",
                        "downstream_task_ids": ["land_raw_orders"],
                    }
                },
                {
                    "__var": {
                        "task_id": "land_raw_orders",
                        "downstream_task_ids": ["build_orders_by_customer"],
                    }
                },
                {"task_id": "build_orders_by_customer", "downstream_task_ids": []},
                {"task_id": "unrelated_branch", "downstream_task_ids": []},
            ],
        }
    }
)

NOW = datetime(2026, 9, 7, 10, 0, tzinfo=UTC)


@pytest.fixture
async def airflow_dsn(tmp_path) -> AsyncIterator[str]:
    """A SQLite stand-in for Airflow's metadata database, populated."""
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'airflow.db'}"
    engine = create_async_engine(dsn)
    async with engine.begin() as connection:
        await connection.run_sync(AIRFLOW.create_all)
        await connection.execute(
            insert(DAG_TABLE),
            [{"dag_id": "schema_drift_orders", "fileloc": "/opt/airflow/dags/drift.py"}],
        )
        await connection.execute(
            insert(DAG_CODE),
            [
                {
                    "fileloc": "/opt/airflow/dags/drift.py",
                    "source_code": DAG_FILE,
                    "last_updated": NOW,
                }
            ],
        )
        await connection.execute(
            insert(SERIALIZED_DAG),
            [{"dag_id": "schema_drift_orders", "data": SERIALISED, "last_updated": NOW}],
        )
        await connection.execute(insert(TASK_INSTANCE), _task_instances())
    await engine.dispose()
    yield dsn


def _task_instances() -> list[dict[str, object]]:
    """One failing run, and a clean history before it."""
    rows: list[dict[str, object]] = []
    for day in range(1, 11):
        rows.append(
            {
                "dag_id": "schema_drift_orders",
                "task_id": "build_orders_by_customer",
                "run_id": f"scheduled__2026-08-{day:02d}",
                "map_index": -1,
                "state": "success",
                "try_number": 1,
                "duration": 12.0 + day,
                "start_date": NOW - timedelta(days=30 - day),
                "end_date": NOW - timedelta(days=30 - day),
                "operator": "SQLExecuteQueryOperator",
            }
        )
    failing_run = "manual__2026-09-07T10:00:00+00:00"
    rows.append(
        {
            "dag_id": "schema_drift_orders",
            "task_id": "build_orders_by_customer",
            "run_id": failing_run,
            "map_index": -1,
            "state": "failed",
            "try_number": 1,
            "duration": 2.0,
            "start_date": NOW,
            "end_date": NOW,
            "operator": "SQLExecuteQueryOperator",
        }
    )
    for task_id, state, minutes in (
        ("extract_orders", "success", 20),
        ("land_raw_orders", "success", 10),
        ("unrelated_branch", "failed", 5),
    ):
        rows.append(
            {
                "dag_id": "schema_drift_orders",
                "task_id": task_id,
                "run_id": failing_run,
                "map_index": -1,
                "state": state,
                "try_number": 1,
                "duration": 5.0,
                "start_date": NOW - timedelta(minutes=minutes),
                "end_date": NOW - timedelta(minutes=minutes),
                "operator": "SQLExecuteQueryOperator",
            }
        )
    return rows


@pytest.fixture
async def warehouse_dsn(tmp_path) -> AsyncIterator[str]:
    """A SQLite stand-in for the warehouse, already carrying the drifted column name."""
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'warehouse.db'}"
    engine = create_async_engine(dsn)
    async with engine.begin() as connection:
        await connection.run_sync(WAREHOUSE.create_all)
        await connection.execute(
            insert(ORDERS),
            [
                {
                    "order_id": f"ord-{index:05d}",
                    # Deliberately null on most rows, so a null fraction regression is
                    # something the profiling tests can actually measure.
                    "customer_uuid": f"cust-{index % 7}" if index % 10 == 0 else None,
                    "order_ts": NOW - timedelta(hours=index),
                    "amount_cents": 100 * index,
                    "status": "placed" if index % 2 else "shipped",
                }
                for index in range(1, 101)
            ],
        )
    await engine.dispose()
    yield dsn


@pytest.fixture
async def connections(airflow_dsn: str, warehouse_dsn: str) -> AsyncIterator[ConnectionRegistry]:
    registry = ConnectionRegistry({"airflow": airflow_dsn, "warehouse": warehouse_dsn})
    yield registry
    # The registry opens engines lazily and holds them, so a test that does not dispose
    # leaks a connection into the next one and surfaces there as an unrelated failure.
    await registry.dispose()
