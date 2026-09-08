from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from dag_doctor.db.models import TableProfile as TableProfileRow
from dag_doctor.tools.base import ToolStatus
from dag_doctor.tools.data_profile import ProfileTable


async def _profiles(session_factory) -> list[TableProfileRow]:
    async with session_factory() as session:
        return list((await session.execute(select(TableProfileRow))).scalars())


async def test_the_table_is_counted_and_every_column_profiled(connections, session_factory):
    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders"}
    )

    assert result.ok
    assert result.data.row_count == 100
    assert {column.name for column in result.data.columns} == {
        "order_id",
        "customer_uuid",
        "order_ts",
        "amount_cents",
        "status",
    }


async def test_null_fractions_are_measured(connections, session_factory):
    # The fixture leaves customer_uuid populated on one row in ten.
    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders", "columns": ["customer_uuid"]}
    )

    profile = result.data.columns[0]
    assert profile.null_fraction == 0.9
    assert profile.distinct_count == 7


async def test_ranges_are_reported_so_an_impossible_value_is_visible(connections, session_factory):
    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders", "columns": ["amount_cents"]}
    )

    assert result.data.columns[0].min_value == "100"
    assert result.data.columns[0].max_value == "10000"


async def test_the_first_profile_records_a_baseline_rather_than_claiming_a_regression(
    connections, session_factory
):
    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders"}
    )

    assert result.data.baseline_created is True
    assert result.data.regressed_columns == []
    assert len(await _profiles(session_factory)) == 1


async def test_a_null_fraction_that_jumps_is_called_a_regression(connections, session_factory):
    # A column that is 90% null could be normal. A column that was 2% null and is now
    # 90% null is a finding, and it is the difference between blaming the code that
    # broke and the data that changed under it.
    async with session_factory() as session:
        session.add(
            TableProfileRow(
                connection="warehouse",
                table_name="orders",
                row_count=100,
                sampled_rows=100,
                columns=[{"name": "customer_uuid", "null_fraction": 0.02, "distinct_count": 90}],
                captured_at=datetime.now(UTC) - timedelta(days=1),
            )
        )
        await session.commit()

    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders", "columns": ["customer_uuid"]}
    )

    assert result.data.regressed_columns == ["customer_uuid"]
    assert result.data.columns[0].baseline_null_fraction == 0.02
    assert result.data.columns[0].null_fraction_delta == 0.88
    assert "null fractions jumped on: customer_uuid" in result.data.summarise()


async def test_a_stable_column_is_not_called_a_regression(connections, session_factory):
    tool = ProfileTable(connections, session_factory)
    await tool.run({"connection": "warehouse", "table": "orders", "columns": ["customer_uuid"]})

    result = await tool.run(
        {"connection": "warehouse", "table": "orders", "columns": ["customer_uuid"]}
    )

    assert result.data.regressed_columns == []
    assert "no null fraction regressions" in result.data.summarise()


async def test_every_profile_is_recorded_so_the_next_call_has_a_baseline(
    connections, session_factory
):
    tool = ProfileTable(connections, session_factory)
    await tool.run({"connection": "warehouse", "table": "orders"})
    await tool.run({"connection": "warehouse", "table": "orders"})

    assert len(await _profiles(session_factory)) == 2


async def test_a_column_that_is_not_on_the_table_is_refused(connections, session_factory):
    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders", "columns": ["customer_id"]}
    )

    assert result.status is ToolStatus.NOT_FOUND
    assert "customer_id" in (result.error or "")


async def test_a_column_name_that_is_an_expression_is_refused(connections, session_factory):
    # Columns are named by a model, so this is the argument that must never be
    # interpolated into a query.
    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders", "columns": ["1; DROP TABLE orders"]}
    )

    assert result.status is ToolStatus.FORBIDDEN


async def test_a_missing_table_is_reported_as_not_found(connections, session_factory):
    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "nowhere"}
    )

    assert result.status is ToolStatus.NOT_FOUND


async def test_profiling_samples_rather_than_scanning(connections, session_factory):
    # A full scan of a fact table would time out. The sample is bounded and the sampled
    # count is reported, so nobody mistakes a sampled null fraction for an exact one.
    result = await ProfileTable(connections, session_factory).run(
        {"connection": "warehouse", "table": "orders", "sample_rows": 20}
    )

    assert result.data.row_count == 100
    assert result.data.sampled_rows == 20
