"""Incident routes, including the one worth demonstrating.

``/investigation`` matters more than it looks. It is the difference between an agent that
asserts a conclusion and one that can be checked: what it looked at, what it believed,
what it tried to disprove, and what it concluded.
"""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Query, Request, status
from sqlalchemy import Select, select

from dag_doctor.api.dependencies import (
    ApiKeyDep,
    SessionDep,
    decode_cursor,
    encode_cursor,
)
from dag_doctor.api.errors import COMMON_RESPONSES
from dag_doctor.api.schemas import (
    DiagnosisOut,
    EvidenceOut,
    HypothesisOut,
    IncidentDetail,
    IncidentSummary,
    InvestigationTrace,
    Page,
    ReplayAccepted,
    ReplayIn,
    StepOut,
)
from dag_doctor.core.exceptions import ConfigValidationError, ResourceNotFoundError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import FailureEvent, IncidentStatus
from dag_doctor.db.models import DiagnosisRecord, Incident
from dag_doctor.db.repositories import IncidentRepository, InvestigationRepository

logger = get_logger(__name__)

router = APIRouter(prefix="/incidents", tags=["incidents"], responses=COMMON_RESPONSES)

MAX_PAGE_SIZE = 200


@router.get("", summary="List incidents", response_model=Page[IncidentSummary])
async def list_incidents(
    session: SessionDep,
    _auth: ApiKeyDep,
    dag_id: Annotated[str | None, Query(description="Only this DAG")] = None,
    task_id: Annotated[str | None, Query(description="Only this task")] = None,
    incident_status: Annotated[
        IncidentStatus | None, Query(alias="status", description="Only this status")
    ] = None,
    since: Annotated[datetime | None, Query(description="Received at or after")] = None,
    until: Annotated[datetime | None, Query(description="Received before")] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = 50,
    cursor: Annotated[str | None, Query(description="From a previous page")] = None,
) -> Page[IncidentSummary]:
    """Return a page of incidents, newest first."""
    statement = select(Incident).order_by(Incident.received_at.desc(), Incident.id.desc())
    statement = _filtered(statement, dag_id, task_id, incident_status, since, until)

    if cursor is not None:
        received_at, identifier = decode_cursor(cursor)
        # Tie-broken by id, so incidents received in the same instant cannot be skipped
        # or repeated as the reader pages through them.
        statement = statement.where(
            (Incident.received_at < received_at)
            | ((Incident.received_at == received_at) & (Incident.id < UUID(identifier)))
        )

    # One more than asked for, which is how the next cursor is known to exist without a
    # second count query.
    rows = list((await session.execute(statement.limit(limit + 1))).scalars())
    has_more = len(rows) > limit
    page = rows[:limit]
    return Page(
        items=[IncidentSummary.of(row) for row in page],
        next_cursor=(
            encode_cursor(page[-1].received_at, str(page[-1].id)) if has_more and page else None
        ),
    )


@router.get("/{incident_id}", summary="One incident, with its diagnosis")
async def get_incident(incident_id: UUID, session: SessionDep, _auth: ApiKeyDep) -> IncidentDetail:
    """Return an incident and the most recent diagnosis of it."""
    incident = await _require(incident_id, session)
    diagnosis = await InvestigationRepository(session).latest_diagnosis(incident_id)
    return IncidentDetail.of_incident(incident, diagnosis)


@router.get("/{incident_id}/investigation", summary="The full step-by-step trace")
async def get_investigation(
    incident_id: UUID,
    session: SessionDep,
    _auth: ApiKeyDep,
    attempt: Annotated[int | None, Query(ge=1, description="One attempt, or all")] = None,
) -> InvestigationTrace:
    """Return everything the agent did, in order.

    Refuted hypotheses are included deliberately. What was ruled out, and what ruled it
    out, is half of why the conclusion should be believed.
    """
    await _require(incident_id, session)
    repository = InvestigationRepository(session)
    steps = await repository.steps(incident_id, attempt)
    return InvestigationTrace(
        incident_id=incident_id,
        attempts=max((step.attempt for step in steps), default=0),
        steps=[StepOut.of(step) for step in steps],
        evidence=[EvidenceOut.of(item) for item in await repository.evidence(incident_id, attempt)],
        hypotheses=[
            HypothesisOut.of(item) for item in await repository.hypotheses(incident_id, attempt)
        ],
        diagnosis=_maybe(await repository.latest_diagnosis(incident_id)),
    )


@router.post(
    "/{incident_id}/replay",
    summary="Investigate this incident again",
    status_code=status.HTTP_202_ACCEPTED,
)
async def replay_incident(
    incident_id: UUID,
    body: ReplayIn,
    request: Request,
    session: SessionDep,
    background: BackgroundTasks,
    _auth: ApiKeyDep,
) -> ReplayAccepted:
    """Re-run the investigation, optionally against a different model.

    Returns immediately: an investigation takes minutes with a local model, and holding a
    request open for it would be a worse interface than a status to poll. The new attempt
    is written beside the old one rather than over it, which is the entire point.

    Raises:
        ConfigValidationError: If this deployment has no investigator wired in, which is
            the case for an API running without a model provider.
    """
    incident = await _require(incident_id, session)
    replay = getattr(request.app.state, "replay", None)
    if replay is None:
        raise ConfigValidationError(
            "This deployment cannot replay investigations because no model provider is "
            "configured for the API process"
        )

    attempt = await InvestigationRepository(session).next_attempt(incident_id)
    background.add_task(replay, incident_id, _event_of(incident), body.model)
    logger.info("replay.accepted", incident_id=str(incident_id), attempt=attempt, model=body.model)
    return ReplayAccepted(incident_id=incident_id, attempt=attempt)


def _filtered(
    statement: Select[tuple[Incident]],
    dag_id: str | None,
    task_id: str | None,
    incident_status: IncidentStatus | None,
    since: datetime | None,
    until: datetime | None,
) -> Select[tuple[Incident]]:
    """Apply the optional filters a caller supplied."""
    if dag_id is not None:
        statement = statement.where(Incident.dag_id == dag_id)
    if task_id is not None:
        statement = statement.where(Incident.task_id == task_id)
    if incident_status is not None:
        statement = statement.where(Incident.status == incident_status)
    if since is not None:
        statement = statement.where(Incident.received_at >= since)
    if until is not None:
        statement = statement.where(Incident.received_at < until)
    return statement


async def _require(incident_id: UUID, session: SessionDep) -> Incident:
    """Fetch an incident or say plainly that there is none.

    Raises:
        ResourceNotFoundError: If no such incident exists.
    """
    incident = await IncidentRepository(session).get(incident_id)
    if incident is None:
        raise ResourceNotFoundError(
            f"No incident {incident_id}", details={"incident_id": str(incident_id)}
        )
    return incident


def _maybe(record: DiagnosisRecord | None) -> DiagnosisOut | None:
    """Render a diagnosis if there is one."""
    return DiagnosisOut.of(record) if record is not None else None


def _event_of(incident: Incident) -> FailureEvent:
    """Rebuild the failure event an incident was opened from."""
    return FailureEvent(
        dag_id=incident.dag_id,
        task_id=incident.task_id,
        run_id=incident.run_id,
        try_number=incident.try_number,
        map_index=incident.map_index,
        logical_date=incident.logical_date,
        failed_at=incident.failed_at,
        log_url=incident.log_url,
        exception_type=incident.exception_type,
        exception_message=incident.exception_message,
    )
