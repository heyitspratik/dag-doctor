"""Running one incident from failure event to persisted diagnosis.

The ordering here is the part worth reading. The investigation runs first, then everything
it produced is written in a single transaction, and only then is the diagnosis announced.
A crash at any point leaves either no investigation or a complete one, never a diagnosis
whose evidence is missing, and never an announcement of something the database does not
have.
"""

from types import TracebackType
from typing import Self
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dag_doctor.core.exceptions import DagDoctorError, ResourceNotFoundError
from dag_doctor.core.logging import get_logger, incident_context
from dag_doctor.core.metrics import (
    CONFIDENCE,
    DIAGNOSIS_LATENCY,
    INCIDENTS_RECEIVED,
    INVESTIGATIONS_COMPLETED,
    TOKENS_SPENT,
    TOOL_CALLS,
    TOOL_DURATION,
)
from dag_doctor.core.models import (
    Diagnosis,
    FailureEvent,
    HaltReason,
    IncidentStatus,
    RootCauseCategory,
)
from dag_doctor.core.settings import Settings
from dag_doctor.db.repositories import IncidentRepository, InvestigationRepository
from dag_doctor.db.session import session_scope
from dag_doctor.graph.builder import build_graph, initial_state
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.state import InvestigationState
from dag_doctor.graph.toolbox import Toolbox
from dag_doctor.messaging.producer import DiagnosisPublisher
from dag_doctor.messaging.schemas import DiagnosisCompletedMessage

logger = get_logger(__name__)


class Investigator:
    """Turns a failure event into a persisted, announced diagnosis."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        caller: ModelCaller,
        toolbox: Toolbox,
        diagnoses: DiagnosisPublisher | None = None,
    ) -> None:
        """Initialise the investigator.

        Args:
            settings: Budgets, thresholds, and broker configuration.
            session_factory: The agent's own database.
            caller: How the graph's nodes reach a model.
            toolbox: The tools the investigation may use.
            diagnoses: Where finished investigations are announced. Optional so a test, or
                a run with no broker, still investigates and persists.
        """
        self._settings = settings
        self._session_factory = session_factory
        self._toolbox = toolbox
        self._diagnoses = diagnoses
        self._graph = build_graph(caller, toolbox, settings.budgets)

    async def __aenter__(self) -> Self:
        """Connect the diagnosis publisher, if there is one."""
        if self._diagnoses is not None:
            await self._diagnoses.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Disconnect the diagnosis publisher."""
        if self._diagnoses is not None:
            await self._diagnoses.stop()

    async def handle(self, event: FailureEvent) -> Diagnosis | None:
        """Record the failure as an incident and investigate it.

        Args:
            event: The failure delivered on the topic.

        Returns:
            The diagnosis, or ``None`` if the incident was already diagnosed.
        """
        INCIDENTS_RECEIVED.labels(dag_id=event.dag_id).inc()
        async with session_scope(self._session_factory) as session:
            incident, created = await IncidentRepository(session).get_or_create(event)
            incident_id = incident.id
            already_done = not created and incident.status in _TERMINAL

        if already_done:
            # A redelivery of a failure that was already diagnosed. Investigating again
            # would spend a model twice to reach the same answer.
            logger.info(
                "investigation.skipped", incident_id=str(incident_id), reason="already_diagnosed"
            )
            return None

        return await self.investigate(incident_id, event)

    async def investigate(self, incident_id: UUID, event: FailureEvent) -> Diagnosis | None:
        """Run the graph for one incident and persist everything it produced.

        Args:
            incident_id: The incident being investigated.
            event: The failure that opened it.

        Returns:
            The diagnosis, or ``None`` if the graph produced none at all.
        """
        with incident_context(incident_id, dag_id=event.dag_id, task_id=event.task_id):
            async with session_scope(self._session_factory) as session:
                repository = IncidentRepository(session)
                incident = await repository.get(incident_id)
                if incident is None:
                    raise ResourceNotFoundError(
                        f"No incident {incident_id}", details={"incident_id": str(incident_id)}
                    )
                await repository.set_status(incident, IncidentStatus.INVESTIGATING)
                attempt = await InvestigationRepository(session).next_attempt(incident_id)

            logger.info("investigation.started", attempt=attempt)
            with DIAGNOSIS_LATENCY.time():
                state = await self._run_graph(incident_id, event)

            async with session_scope(self._session_factory) as session:
                await InvestigationRepository(session).save(state, attempt)
                incident = await IncidentRepository(session).get(incident_id)
                if incident is not None:
                    incident.status = _status_for(state)

            await self._announce(state, event, attempt)
            _record_metrics(state)
            _log_outcome(state)
            return state.diagnosis

    async def _run_graph(self, incident_id: UUID, event: FailureEvent) -> InvestigationState:
        """Run the graph, turning any escape into a recorded halt.

        The nodes already convert their own faults into halts. This is the outer net for
        anything the graph machinery itself raises, so an incident never stays stuck in
        ``investigating`` with nothing written against it.
        """
        start = initial_state(incident_id, event, self._settings.budgets)
        try:
            result = await self._graph.ainvoke(
                start, config={"configurable": {"thread_id": str(incident_id)}}
            )
        except DagDoctorError as exc:
            logger.error("investigation.failed", error=exc.message, code=exc.code)
            return _halted(start, exc.message)
        return InvestigationState.model_validate(result)

    async def _announce(self, state: InvestigationState, event: FailureEvent, attempt: int) -> None:
        """Announce the finished investigation, if there is anywhere to announce it."""
        if self._diagnoses is None or state.diagnosis is None:
            return
        await self._diagnoses.publish(
            DiagnosisCompletedMessage.from_diagnosis(
                state.diagnosis,
                dag_id=event.dag_id,
                task_id=event.task_id,
                run_id=event.run_id,
                attempt=attempt,
            )
        )


#: Statuses that mean the incident has been through the graph already.
_TERMINAL = frozenset(
    {IncidentStatus.DIAGNOSED, IncidentStatus.INCONCLUSIVE, IncidentStatus.FAILED}
)


def _status_for(state: InvestigationState) -> IncidentStatus:
    """Where an incident ends up, given how its investigation went.

    ``failed`` and ``inconclusive`` are kept apart on purpose. An investigation that broke
    is an operational problem someone should fix; one that ran properly and could not
    decide is a finding about the failure. Collapsing them would hide the first inside
    the second, and the agent would look merely unhelpful rather than broken.
    """
    diagnosis = state.diagnosis
    if diagnosis is None or state.halt_reason is HaltReason.INVESTIGATION_ERROR:
        return IncidentStatus.FAILED
    return IncidentStatus.DIAGNOSED if diagnosis.is_conclusive else IncidentStatus.INCONCLUSIVE


def _halted(state: InvestigationState, error: str) -> InvestigationState:
    """Turn a graph-level failure into an inconclusive investigation with a reason."""
    return state.model_copy(
        update={
            "halt_reason": HaltReason.INVESTIGATION_ERROR,
            "unknowns": [f"The investigation could not run: {error}"],
            "diagnosis": Diagnosis(
                incident_id=state.incident_id,
                root_cause_category=RootCauseCategory.UNKNOWN,
                summary=f"The investigation could not run: {error}",
                confidence=0.0,
                halt_reason=HaltReason.INVESTIGATION_ERROR,
                unknowns=[error],
            ),
        }
    )


def _record_metrics(state: InvestigationState) -> None:
    """Publish what this investigation cost and concluded."""
    for item in state.evidence:
        TOOL_CALLS.labels(tool=item.tool_name, status="ok" if item.succeeded else "failed").inc()
        TOOL_DURATION.labels(tool=item.tool_name).observe(item.duration_ms / 1000)
    for step in state.steps:
        TOKENS_SPENT.labels(node=step.node, direction="prompt").inc(step.prompt_tokens)
        TOKENS_SPENT.labels(node=step.node, direction="completion").inc(step.completion_tokens)

    diagnosis = state.diagnosis
    if diagnosis is None:
        return
    CONFIDENCE.observe(diagnosis.confidence)
    INVESTIGATIONS_COMPLETED.labels(
        root_cause_category=diagnosis.root_cause_category.value,
        conclusive=str(diagnosis.is_conclusive).lower(),
        halt_reason=diagnosis.halt_reason.value,
    ).inc()


def _log_outcome(state: InvestigationState) -> None:
    """One line per finished investigation, carrying what a reader would want."""
    diagnosis = state.diagnosis
    logger.info(
        "investigation.finished",
        category=diagnosis.root_cause_category.value if diagnosis else "none",
        confidence=diagnosis.confidence if diagnosis else 0.0,
        conclusive=diagnosis.is_conclusive if diagnosis else False,
        halt_reason=state.halt_reason.value if state.halt_reason else None,
        iterations=state.iteration,
        tool_calls=state.tool_calls_made,
        steps=len(state.steps),
    )
