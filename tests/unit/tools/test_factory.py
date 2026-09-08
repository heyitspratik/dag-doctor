"""The tool set as the agent actually receives it."""

import pytest

from dag_doctor.core.settings import Settings
from dag_doctor.tools import registry
from dag_doctor.tools.factory import build_tools, register_default_tools

#: The nine tools the build spec calls for. Named here rather than derived from the
#: registry, so dropping one is a test failure rather than a silently smaller agent.
EXPECTED_TOOLS = {
    "fetch_task_logs",
    "get_dag_run_history",
    "get_upstream_task_state",
    "inspect_table_schema",
    "compare_schema_snapshot",
    "profile_table",
    "get_dag_source",
    "check_connection_health",
    "search_similar_incidents",
}


@pytest.fixture(autouse=True)
def _empty_registry():
    registry.clear()
    yield
    registry.clear()


def test_every_tool_is_built(session_factory, connections):
    tools = build_tools(Settings(), session_factory, connections)

    assert {tool.name for tool in tools} == EXPECTED_TOOLS


def test_registering_makes_every_tool_addressable_by_name(session_factory, connections):
    register_default_tools(Settings(), session_factory, connections)

    assert set(registry.names()) == EXPECTED_TOOLS


def test_every_tool_describes_itself_for_the_prompt(session_factory, connections):
    register_default_tools(Settings(), session_factory, connections)

    for name, description in registry.catalogue():
        assert len(description) > 40, f"{name} needs a description a model can act on"


def test_every_tool_publishes_an_argument_schema(session_factory, connections):
    tools = build_tools(Settings(), session_factory, connections)

    for tool in tools:
        schema = tool.input_schema()
        assert schema["type"] == "object"
        assert "properties" in schema


def test_every_tool_inherits_the_configured_timeout(session_factory, connections):
    settings = Settings()
    settings.budgets.tool_timeout_s = 7.5

    tools = build_tools(settings, session_factory, connections)

    assert {tool.timeout_s for tool in tools} == {7.5}


def test_no_tool_opens_a_connection_when_it_is_merely_built(session_factory, connections):
    # The Airflow DAG parser imports this package. A tool that connected on construction
    # would make every DAG parse depend on a reachable database.
    build_tools(Settings(), session_factory, connections)

    assert connections._engines == {}
