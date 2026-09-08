"""Building the tool set the agent actually runs with.

Kept apart from the registry so that constructing tools, which needs settings and database
connections, does not happen at import time. Importing a tool module must stay free: the
Airflow DAG parser imports this package, and a module that opened a connection on import
would make every DAG parse depend on a reachable database.
"""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dag_doctor.core.settings import Settings
from dag_doctor.tools import registry
from dag_doctor.tools.airflow_logs import FetchTaskLogs
from dag_doctor.tools.airflow_metadata import GetDagRunHistory, GetUpstreamTaskState
from dag_doctor.tools.airflow_source import GetDagSource
from dag_doctor.tools.base import RunnableTool
from dag_doctor.tools.connection_check import CheckConnectionHealth
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.data_profile import ProfileTable
from dag_doctor.tools.schema_inspect import CompareSchemaSnapshot, InspectTableSchema
from dag_doctor.tools.similar_incidents import SearchSimilarIncidents


def build_tools(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    connections: ConnectionRegistry | None = None,
) -> list[RunnableTool]:
    """Construct every tool, wired to its data source.

    Args:
        settings: Application settings, which carry the tool timeout.
        session_factory: The agent's own database, which is the only thing it writes.
        connections: Injected in tests; built from settings otherwise.

    Returns:
        The tools, in the order a sensible investigation tends to use them.
    """
    timeout = settings.budgets.tool_timeout_s
    connections = connections or ConnectionRegistry.from_settings(settings)
    return [
        FetchTaskLogs(settings.airflow, timeout),
        SearchSimilarIncidents(session_factory, timeout),
        GetDagRunHistory(connections, timeout),
        GetUpstreamTaskState(connections, timeout),
        GetDagSource(connections, timeout),
        InspectTableSchema(connections, timeout),
        CompareSchemaSnapshot(connections, session_factory, timeout),
        ProfileTable(connections, session_factory, timeout),
        CheckConnectionHealth(connections, timeout),
    ]


def register_default_tools(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    connections: ConnectionRegistry | None = None,
) -> list[RunnableTool]:
    """Build every tool and put it in the registry.

    Args:
        settings: Application settings.
        session_factory: The agent's own database.
        connections: Injected in tests; built from settings otherwise.

    Returns:
        The registered tools.
    """
    tools = build_tools(settings, session_factory, connections)
    for tool in tools:
        registry.register(tool)
    return tools
