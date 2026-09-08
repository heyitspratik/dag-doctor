from sqlalchemy import insert
from sqlalchemy.ext.asyncio import create_async_engine

from dag_doctor.tools.airflow_metadata import DAG_TABLE
from dag_doctor.tools.airflow_source import MAX_SOURCE_CHARS, GetDagSource
from dag_doctor.tools.base import ToolStatus


async def test_the_source_airflow_parsed_is_returned(connections):
    result = await GetDagSource(connections).run({"dag_id": "schema_drift_orders"})

    assert result.ok
    assert "customer_id" in result.data.source
    assert result.data.fileloc == "/opt/airflow/dags/drift.py"
    assert result.data.truncated is False


async def test_the_summary_names_the_file_that_was_read(connections):
    result = await GetDagSource(connections).run({"dag_id": "schema_drift_orders"})

    assert "/opt/airflow/dags/drift.py" in result.data.summarise()


async def test_a_dag_airflow_does_not_know_is_reported_as_not_found(connections):
    result = await GetDagSource(connections).run({"dag_id": "no_such_dag"})

    assert result.status is ToolStatus.NOT_FOUND
    assert "no_such_dag" in (result.error or "")


async def test_a_dag_with_no_stored_source_is_reported_as_not_found(connections, airflow_dsn):
    engine = create_async_engine(airflow_dsn)
    async with engine.begin() as connection:
        await connection.execute(
            insert(DAG_TABLE), [{"dag_id": "parsed_but_uncached", "fileloc": "/dags/x.py"}]
        )
    await engine.dispose()

    result = await GetDagSource(connections).run({"dag_id": "parsed_but_uncached"})

    assert result.status is ToolStatus.NOT_FOUND


async def test_a_very_long_dag_file_is_truncated_before_it_reaches_a_prompt(
    connections, airflow_dsn
):
    # A whole file is rarely needed and a long one crowds the evidence out of the context.
    from dag_doctor.tools.airflow_metadata import DAG_CODE

    engine = create_async_engine(airflow_dsn)
    async with engine.begin() as connection:
        await connection.execute(
            insert(DAG_TABLE), [{"dag_id": "enormous", "fileloc": "/dags/enormous.py"}]
        )
        await connection.execute(
            insert(DAG_CODE),
            [{"fileloc": "/dags/enormous.py", "source_code": "x = 1\n" * 20_000}],
        )
    await engine.dispose()

    result = await GetDagSource(connections).run({"dag_id": "enormous"})

    assert result.data.truncated is True
    assert len(result.data.source) == MAX_SOURCE_CHARS
    assert "truncated" in result.data.summarise()
