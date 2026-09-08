"""Diagnosis routes, including the one that closes the loop.

Feedback is what turns a demo into something that improves. A human verdict makes the
accuracy number in the README measured rather than asserted, and it is what
``search_similar_incidents`` weights when a similar failure comes round again.
"""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query
from sqlalchemy import Select, select

from dag_doctor.api.dependencies import ApiKeyDep, SessionDep, decode_cursor, encode_cursor
from dag_doctor.api.errors import COMMON_RESPONSES
from dag_doctor.api.schemas import DiagnosisOut, FeedbackIn, Page
from dag_doctor.core.exceptions import ResourceNotFoundError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import RootCauseCategory
from dag_doctor.db.models import DiagnosisRecord

logger = get_logger(__name__)

router = APIRouter(prefix="/diagnoses", tags=["diagnoses"], responses=COMMON_RESPONSES)

MAX_PAGE_SIZE = 200


@router.get("", summary="List diagnoses", response_model=Page[DiagnosisOut])
async def list_diagnoses(
    session: SessionDep,
    _auth: ApiKeyDep,
    root_cause_category: Annotated[
        RootCauseCategory | None, Query(description="Only this category")
    ] = None,
    dag_id: Annotated[str | None, Query(description="Only this DAG")] = None,
    min_confidence: Annotated[float | None, Query(ge=0.0, le=1.0)] = None,
    reviewed: Annotated[
        bool | None, Query(description="Only those a human has judged, or only those not")
    ] = None,
    since: Annotated[datetime | None, Query(description="Created at or after")] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = 50,
    cursor: Annotated[str | None, Query(description="From a previous page")] = None,
) -> Page[DiagnosisOut]:
    """Return a page of diagnoses, newest first."""
    statement = select(DiagnosisRecord).order_by(
        DiagnosisRecord.created_at.desc(), DiagnosisRecord.id.desc()
    )
    statement = _filtered(statement, root_cause_category, dag_id, min_confidence, reviewed, since)

    if cursor is not None:
        created_at, identifier = decode_cursor(cursor)
        statement = statement.where(
            (DiagnosisRecord.created_at < created_at)
            | ((DiagnosisRecord.created_at == created_at) & (DiagnosisRecord.id < UUID(identifier)))
        )

    rows = list((await session.execute(statement.limit(limit + 1))).scalars())
    has_more = len(rows) > limit
    page = rows[:limit]
    return Page(
        items=[DiagnosisOut.of(row) for row in page],
        next_cursor=(
            encode_cursor(page[-1].created_at, str(page[-1].id)) if has_more and page else None
        ),
    )


@router.post("/{diagnosis_id}/feedback", summary="Record a human verdict")
async def record_feedback(
    diagnosis_id: UUID, body: FeedbackIn, session: SessionDep, _auth: ApiKeyDep
) -> DiagnosisOut:
    """Mark a diagnosis correct or incorrect.

    A verdict can be changed. People revise their view once they have looked properly, and
    refusing the correction would leave the accuracy number wrong on purpose.

    Raises:
        ResourceNotFoundError: If there is no such diagnosis.
    """
    record = await session.get(DiagnosisRecord, diagnosis_id)
    if record is None:
        raise ResourceNotFoundError(
            f"No diagnosis {diagnosis_id}", details={"diagnosis_id": str(diagnosis_id)}
        )

    record.human_verdict = body.correct
    record.human_note = body.note
    await session.commit()
    logger.info(
        "diagnosis.reviewed",
        diagnosis_id=str(diagnosis_id),
        incident_id=str(record.incident_id),
        correct=body.correct,
    )
    return DiagnosisOut.of(record)


def _filtered(
    statement: Select[tuple[DiagnosisRecord]],
    root_cause_category: RootCauseCategory | None,
    dag_id: str | None,
    min_confidence: float | None,
    reviewed: bool | None,
    since: datetime | None,
) -> Select[tuple[DiagnosisRecord]]:
    """Apply the optional filters a caller supplied."""
    if root_cause_category is not None:
        statement = statement.where(DiagnosisRecord.root_cause_category == root_cause_category)
    if dag_id is not None:
        statement = statement.where(DiagnosisRecord.responsible_dag_id == dag_id)
    if min_confidence is not None:
        statement = statement.where(DiagnosisRecord.confidence >= min_confidence)
    if reviewed is not None:
        # Unjudged is not the same as judged wrong, so the filter distinguishes them.
        statement = statement.where(
            DiagnosisRecord.human_verdict.is_not(None)
            if reviewed
            else DiagnosisRecord.human_verdict.is_(None)
        )
    if since is not None:
        statement = statement.where(DiagnosisRecord.created_at >= since)
    return statement
