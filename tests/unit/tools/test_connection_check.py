import pytest

from dag_doctor.tools.base import ToolStatus
from dag_doctor.tools.connection_check import CheckConnectionHealth
from dag_doctor.tools.connections import ConnectionRegistry


async def test_a_reachable_connection_answers(connections):
    result = await CheckConnectionHealth(connections).run({"conn_id": "warehouse"})

    assert result.ok
    assert result.data.reachable is True
    assert result.data.latency_ms >= 0
    assert "answered in" in result.data.summarise()


async def test_an_unreachable_connection_is_a_successful_call_with_a_negative_answer():
    # "The database is down" is exactly the evidence that turns a puzzling application
    # error into transient infrastructure, so it must arrive as data, not as a fault.
    registry = ConnectionRegistry(
        {"warehouse": "postgresql+psycopg://nobody:nobody@127.0.0.1:1/none"}
    )
    try:
        result = await CheckConnectionHealth(registry, timeout_s=5.0).run({"conn_id": "warehouse"})
    finally:
        await registry.dispose()

    assert result.ok
    assert result.data.reachable is False
    assert result.data.error
    assert "unreachable" in result.data.summarise()


async def test_a_connection_the_agent_may_not_read_is_refused(connections):
    result = await CheckConnectionHealth(connections).run({"conn_id": "production_primary"})

    assert result.status is ToolStatus.FORBIDDEN


async def test_the_allowed_connections_are_named_in_the_refusal(connections):
    result = await CheckConnectionHealth(connections).run({"conn_id": "anything_else"})

    assert "airflow" in (result.error or "")
    assert "warehouse" in (result.error or "")


@pytest.mark.parametrize("raw_input", [{}, {"conn_id": 1}, {"conn_id": "x", "extra": True}])
async def test_arguments_that_do_not_fit_the_schema_are_refused(connections, raw_input):
    result = await CheckConnectionHealth(connections).run(raw_input)

    assert result.status is ToolStatus.INVALID_INPUT


def test_only_registered_connections_are_reachable_at_all(connections):
    # Tools take a connection name, never a DSN, so a model cannot point one at a host
    # of its choosing.
    assert connections.names() == ["airflow", "warehouse"]
