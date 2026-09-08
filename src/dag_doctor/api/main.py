"""The HTTP application.

Wiring lives on ``app.state``, set up by the lifespan and torn down after it, so nothing
here is a module-level singleton a test would have to reach around. The application can be
built without a model provider: everything except replay works, and replay says plainly
that it cannot rather than failing obscurely.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol
from uuid import UUID

from fastapi import APIRouter, FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from dag_doctor import __version__
from dag_doctor.api.errors import install_error_handlers
from dag_doctor.api.middleware import install_request_context
from dag_doctor.api.v1.routes import diagnoses, health, incidents, metrics
from dag_doctor.core.logging import configure_logging, get_logger
from dag_doctor.core.models import FailureEvent
from dag_doctor.core.settings import Settings, get_settings
from dag_doctor.db.session import build_engine, build_session_factory
from dag_doctor.graph.model import LangChainCaller
from dag_doctor.graph.toolbox import Toolbox
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.factory import build_tools
from dag_doctor.worker.investigator import Investigator

logger = get_logger(__name__)

API_PREFIX = "/api/v1"

DESCRIPTION = """
Diagnoses failing Airflow pipelines.

An Airflow task failure becomes an incident, a LangGraph state machine investigates it
with read-only tools, and the result is a structured diagnosis with an evidence chain and
a confidence score computed in code.

The endpoint worth looking at first is `/api/v1/incidents/{id}/investigation`, which
returns everything the agent did: what it looked at, what it believed, what it tried to
disprove, and what it concluded.
"""


class ReplayRunner(Protocol):
    """Re-runs one investigation in the background."""

    async def __call__(self, incident_id: UUID, event: FailureEvent, model: str | None) -> None:
        """Investigate an incident again, optionally with a different model."""
        ...


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    replay: ReplayRunner | None = None,
) -> FastAPI:
    """Build the application.

    Args:
        settings: Application settings; read from the environment when absent.
        session_factory: Injected by tests. When absent the lifespan opens its own engine
            and closes it on shutdown.
        replay: What the replay endpoint calls. Absent means replay is unavailable, which
            it says rather than pretending otherwise.

    Returns:
        The application, ready to serve.
    """
    resolved = settings or get_settings()
    configure_logging(resolved.app_env, resolved.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine: AsyncEngine | None = None
        if session_factory is None:
            engine = build_engine(resolved.db)
            app.state.session_factory = build_session_factory(engine)

        connections: ConnectionRegistry | None = None
        if replay is None:
            connections = ConnectionRegistry.from_settings(resolved)
            app.state.replay = _build_replay(resolved, app.state.session_factory, connections)

        logger.info("api.started", version=__version__, provider=resolved.llm.provider)
        try:
            yield
        finally:
            if connections is not None:
                await connections.dispose()
            if engine is not None:
                await engine.dispose()
            logger.info("api.stopped")

    app = FastAPI(
        title="dag-doctor",
        description=DESCRIPTION,
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        openapi_url="/openapi.json",
        contact={"name": "dag-doctor", "url": "https://github.com/pratikahir/dag-doctor"},
        license_info={"name": "MIT"},
    )
    app.state.settings = resolved
    # The lifespan builds one when none was injected, so replay works in a real
    # deployment without the caller having to assemble the graph itself.
    app.state.replay = replay
    if session_factory is not None:
        app.state.session_factory = session_factory

    install_request_context(app)
    install_error_handlers(app)

    versioned = APIRouter(prefix=API_PREFIX)
    versioned.include_router(incidents.router)
    versioned.include_router(diagnoses.router)
    app.include_router(versioned)
    # Health and metrics stay outside the version prefix: a probe or a scraper should not
    # have to be reconfigured when the API version changes.
    app.include_router(health.router)
    app.include_router(metrics.router)
    return app


def _build_replay(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    connections: ConnectionRegistry,
) -> ReplayRunner:
    """Build the function the replay endpoint calls.

    A caller is constructed per replay rather than shared, because a replay may ask for a
    different model and reusing one would apply that choice to everything afterwards.
    """

    async def run(incident_id: UUID, event: FailureEvent, model: str | None) -> None:
        llm = settings.llm.with_model(model) if model else settings.llm
        run_settings = settings.model_copy(update={"llm": llm})
        toolbox = Toolbox(build_tools(run_settings, session_factory, connections))
        async with LangChainCaller(llm) as caller:
            investigator = Investigator(run_settings, session_factory, caller, toolbox)
            async with investigator:
                await investigator.investigate(incident_id, event)

    return run
