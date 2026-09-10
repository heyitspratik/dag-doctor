from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from dag_doctor.db.models import SchemaSnapshot
from dag_doctor.tools.base import ToolStatus
from dag_doctor.tools.schema_inspect import CompareSchemaSnapshot, InspectTableSchema


async def _snapshots(session_factory) -> list[SchemaSnapshot]:
    async with session_factory() as session:
        return list((await session.execute(select(SchemaSnapshot))).scalars())


async def test_the_current_columns_are_reported(connections):
    result = await InspectTableSchema(connections).run(
        {"connection": "warehouse", "table": "orders"}
    )

    assert result.ok
    names = [column.name for column in result.data.columns]
    assert names == ["order_id", "customer_uuid", "order_ts", "amount_cents", "status"]


async def test_the_summary_lists_the_columns(connections):
    result = await InspectTableSchema(connections).run(
        {"connection": "warehouse", "table": "orders"}
    )

    assert "5 columns" in result.data.summarise()


async def test_a_table_that_does_not_exist_is_a_finding_not_a_fault(connections):
    # "The table is gone" is frequently the answer, so it must reach the graph as data.
    result = await InspectTableSchema(connections).run(
        {"connection": "warehouse", "table": "orders_by_customer"}
    )

    assert result.status is ToolStatus.NOT_FOUND


async def test_a_table_name_that_is_an_injection_attempt_is_refused(connections):
    result = await InspectTableSchema(connections).run(
        {"connection": "warehouse", "table": "orders; DROP TABLE orders"}
    )

    assert result.status is ToolStatus.FORBIDDEN


async def test_a_connection_the_agent_may_not_read_is_refused(connections):
    result = await InspectTableSchema(connections).run(
        {"connection": "production_primary", "table": "orders"}
    )

    assert result.status is ToolStatus.FORBIDDEN
    assert "production_primary" in (result.error or "")


async def test_the_first_comparison_records_a_baseline_rather_than_claiming_drift(
    connections, session_factory
):
    result = await CompareSchemaSnapshot(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders"}
    )

    assert result.ok
    assert result.data.baseline_created is True
    assert result.data.has_drift is False
    assert "recorded one for next time" in result.data.summarise()
    assert len(await _snapshots(session_factory)) == 1


async def test_an_unchanged_table_reports_no_drift(connections, session_factory):
    tool = CompareSchemaSnapshot(connections, session_factory)
    await tool.run({"connection": "warehouse", "table": "orders"})

    result = await tool.run({"connection": "warehouse", "table": "orders"})

    assert result.data.has_drift is False
    assert result.data.baseline_created is False
    assert "unchanged" in result.data.summarise()


async def test_a_renamed_column_is_recognised_as_a_rename(connections, session_factory):
    # The headline case. A column vanishing and another of the same type appearing is
    # an upstream contract change, and saying so is the difference between "a column is
    # missing" and "here is what to look for".
    async with session_factory() as session:
        session.add(
            SchemaSnapshot(
                connection="warehouse",
                table_name="orders",
                columns=[
                    {"name": "order_id", "type": "VARCHAR", "nullable": False},
                    {"name": "customer_id", "type": "VARCHAR", "nullable": True},
                    {"name": "order_ts", "type": "DATETIME", "nullable": True},
                    {"name": "amount_cents", "type": "INTEGER", "nullable": True},
                    {"name": "status", "type": "TEXT", "nullable": True},
                ],
                captured_at=datetime.now(UTC) - timedelta(days=1),
            )
        )
        await session.commit()

    result = await CompareSchemaSnapshot(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders"}
    )

    assert result.data.has_drift is True
    assert result.data.removed == ["customer_id"]
    assert result.data.added == ["customer_uuid"]
    rename = result.data.likely_renames[0]
    assert rename.likely_from == "customer_id"
    assert rename.likely_to == "customer_uuid"
    assert "renamed to customer_uuid" in result.data.summarise()


async def test_a_retyped_column_is_reported_as_changed(connections, session_factory):
    async with session_factory() as session:
        session.add(
            SchemaSnapshot(
                connection="warehouse",
                table_name="orders",
                columns=[
                    {"name": "order_id", "type": "VARCHAR", "nullable": False},
                    {"name": "customer_uuid", "type": "VARCHAR", "nullable": True},
                    {"name": "order_ts", "type": "DATETIME", "nullable": True},
                    {"name": "amount_cents", "type": "VARCHAR", "nullable": True},
                    {"name": "status", "type": "TEXT", "nullable": True},
                ],
                captured_at=datetime.now(UTC) - timedelta(days=1),
            )
        )
        await session.commit()

    result = await CompareSchemaSnapshot(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders"}
    )

    assert [change.name for change in result.data.changed] == ["amount_cents"]
    assert result.data.changed[0].was == "VARCHAR"
    assert result.data.changed[0].now == "INTEGER"


async def test_a_snapshot_taken_after_the_as_of_moment_is_ignored(connections, session_factory):
    # Comparing against a snapshot taken after the failure would compare the failure
    # against itself.
    async with session_factory() as session:
        for offset, columns in ((2, ["order_id"]), (0, ["order_id", "customer_uuid"])):
            session.add(
                SchemaSnapshot(
                    connection="warehouse",
                    table_name="orders",
                    columns=[
                        {"name": name, "type": "VARCHAR", "nullable": True} for name in columns
                    ],
                    captured_at=datetime.now(UTC) - timedelta(days=offset),
                )
            )
        await session.commit()

    result = await CompareSchemaSnapshot(connections, session_factory).run(
        {
            "connection": "warehouse",
            "table": "orders",
            "as_of": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
        }
    )

    assert "customer_uuid" in result.data.added


async def test_an_as_of_before_every_snapshot_does_not_overwrite_the_baseline(
    connections, session_factory
):
    # The defect this guards: when as_of excluded every snapshot, the tool treated that as
    # "never seen" and recorded the current shape as the new baseline. On a drifted table
    # that overwrites the very reference the caller was comparing against, and every later
    # comparison reports no drift. A real rename went undetected for an afternoon.
    tool = CompareSchemaSnapshot(connections, session_factory)
    await tool.run({"connection": "warehouse", "table": "orders"})
    before = await _snapshots(session_factory)

    result = await tool.run(
        {
            "connection": "warehouse",
            "table": "orders",
            "as_of": (datetime.now(UTC) - timedelta(days=365)).isoformat(),
        }
    )

    assert result.ok
    assert result.data.baseline_too_recent is True
    assert result.data.baseline_created is False
    assert len(await _snapshots(session_factory)) == len(before), "it wrote another snapshot"
    assert "nothing to compare against" in result.data.summarise()


async def test_a_table_never_seen_before_still_records_a_first_baseline(
    connections, session_factory
):
    # Bootstrapping is still right when the table genuinely has no history.
    result = await CompareSchemaSnapshot(connections, session_factory).run(
        {
            "connection": "warehouse",
            "table": "orders",
            "as_of": (datetime.now(UTC) - timedelta(days=365)).isoformat(),
        }
    )

    assert result.data.baseline_created is True
    assert len(await _snapshots(session_factory)) == 1
