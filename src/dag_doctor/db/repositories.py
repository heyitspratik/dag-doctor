"""Repositories: the only place this package writes SQL.

Keeping queries here rather than in the consumer or the graph means the storage model can
change without the investigation logic noticing, and it gives the tests one seam to drive.
"""

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dag_doctor.core.exceptions import PersistenceError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import Diagnosis, FailureEvent, IncidentStatus
from dag_doctor.db.models import (
    DiagnosisRecord,
    EvidenceRecord,
    HypothesisRecord,
    Incident,
    InvestigationStep,
)
from dag_doctor.graph.state import InvestigationState

logger = get_logger(__name__)


class IncidentRepository:
    """Reads and writes incidents."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialise the repository.

        Args:
            session: The session for the current unit of work.
        """
        self._session = session

    async def get_or_create(self, event: FailureEvent) -> tuple[Incident, bool]:
        """Return the incident for a failure, creating it only if it is new.

        Idempotency is enforced by the unique constraint rather than by a prior SELECT.
        Checking first and inserting second leaves a window in which two workers both see
        no row and both insert; catching the constraint violation closes it, because the
        database arbitrates rather than the application.

        Args:
            event: The failure delivered on the topic.

        Returns:
            The incident, and whether this call created it.

        Raises:
            PersistenceError: If the row could neither be inserted nor found afterwards,
                which means the conflict came from something other than a redelivery.
        """
        existing = await self._find(event)
        if existing is not None:
            return await self._record_redelivery(existing), False

        incident = Incident(
            dag_id=event.dag_id,
            task_id=event.task_id,
            run_id=event.run_id,
            try_number=event.try_number,
            map_index=event.map_index,
            status=IncidentStatus.RECEIVED,
            logical_date=event.logical_date,
            failed_at=event.failed_at,
            log_url=event.log_url,
            exception_type=event.exception_type,
            exception_message=event.exception_message,
        )
        try:
            # A savepoint rather than the whole transaction, so losing this race does not
            # discard work the caller already did in the same unit of work.
            async with self._session.begin_nested():
                self._session.add(incident)
                await self._session.flush()
        except IntegrityError:
            # Another worker inserted the same failure between the SELECT and this INSERT.
            concurrent = await self._find(event)
            if concurrent is None:
                raise PersistenceError(
                    "Incident insert conflicted but no matching incident exists",
                    details={"idempotency_key": event.idempotency_key},
                ) from None
            return await self._record_redelivery(concurrent), False

        logger.info(
            "incident.created",
            incident_id=str(incident.id),
            dag_id=event.dag_id,
            task_id=event.task_id,
            run_id=event.run_id,
            try_number=event.try_number,
        )
        return incident, True

    async def get(self, incident_id: UUID) -> Incident | None:
        """Fetch one incident by id.

        Args:
            incident_id: The incident to fetch.

        Returns:
            The incident, or ``None`` if there is no such row.
        """
        return await self._session.get(Incident, incident_id)

    async def set_status(self, incident: Incident, status: IncidentStatus) -> Incident:
        """Move an incident to a new lifecycle state.

        Args:
            incident: The incident to update.
            status: Its new status.

        Returns:
            The updated incident.
        """
        incident.status = status
        await self._session.flush()
        return incident

    async def _find(self, event: FailureEvent) -> Incident | None:
        """Look an incident up by the tuple that identifies a task failure."""
        statement = select(Incident).where(
            Incident.dag_id == event.dag_id,
            Incident.task_id == event.task_id,
            Incident.run_id == event.run_id,
            Incident.try_number == event.try_number,
            Incident.map_index == event.map_index,
        )
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def _record_redelivery(self, incident: Incident) -> Incident:
        """Note that Kafka delivered this failure again.

        A redelivery is not an error. Counting them is what distinguishes ordinary
        at-least-once delivery from a worker crash-looping on one message.
        """
        incident.delivery_count += 1
        await self._session.flush()
        logger.info(
            "incident.redelivered",
            incident_id=str(incident.id),
            delivery_count=incident.delivery_count,
            dag_id=incident.dag_id,
            task_id=incident.task_id,
        )
        return incident


class InvestigationRepository:
    """Writes and reads a whole investigation: its steps, evidence, hypotheses and result.

    One repository rather than four, because these rows are only ever meaningful together.
    Persisting them in a single unit of work means a crash halfway through leaves no
    investigation at all rather than a diagnosis whose evidence is missing.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialise the repository.

        Args:
            session: The session for the current unit of work.
        """
        self._session = session

    async def next_attempt(self, incident_id: UUID) -> int:
        """The attempt number a new investigation of this incident should carry.

        Args:
            incident_id: The incident about to be investigated.

        Returns:
            One more than the highest attempt already recorded, or one.
        """
        highest = (
            await self._session.execute(
                select(func.max(DiagnosisRecord.attempt)).where(
                    DiagnosisRecord.incident_id == incident_id
                )
            )
        ).scalar_one_or_none()
        return (highest or 0) + 1

    async def save(self, state: InvestigationState, attempt: int) -> DiagnosisRecord | None:
        """Persist a finished investigation.

        Previous attempts are left alone. A replay is only useful next to the run it is
        being compared with, so it is written beside the original rather than over it.

        Args:
            state: The investigation, at the point it reached a terminal node.
            attempt: Which attempt this is for the incident.

        Returns:
            The stored diagnosis, or ``None`` if the investigation produced none.
        """
        offset = await self._sequence_offset(state.incident_id)
        for step in state.steps:
            self._session.add(
                InvestigationStep(
                    incident_id=state.incident_id,
                    attempt=attempt,
                    node=step.node,
                    # Sequence stays unique per incident, so a replay continues the
                    # numbering rather than colliding with the run before it.
                    sequence=offset + step.sequence,
                    input=dict(step.input),
                    output=dict(step.output),
                    duration_ms=step.duration_ms,
                    prompt_tokens=step.prompt_tokens,
                    completion_tokens=step.completion_tokens,
                    model_used=step.model_used,
                    started_at=step.started_at,
                )
            )

        for item in state.evidence:
            self._session.add(
                EvidenceRecord(
                    id=item.id,
                    incident_id=state.incident_id,
                    attempt=attempt,
                    tool_name=item.tool_name,
                    tool_input=dict(item.tool_input),
                    result=dict(item.result),
                    summary=item.summary,
                    succeeded=item.succeeded,
                    duration_ms=item.duration_ms,
                    collected_at=item.collected_at,
                )
            )

        for hypothesis in state.hypotheses:
            self._session.add(
                HypothesisRecord(
                    id=hypothesis.id,
                    incident_id=state.incident_id,
                    attempt=attempt,
                    statement=hypothesis.statement,
                    root_cause_category=hypothesis.root_cause_category,
                    proposed_test=hypothesis.proposed_test,
                    test_call={
                        "tool": hypothesis.test_tool,
                        "arguments": dict(hypothesis.test_arguments),
                    },
                    outcome=hypothesis.outcome,
                    rank=hypothesis.rank,
                    test_notes=hypothesis.test_notes,
                    supporting_evidence_ids=[
                        str(item) for item in hypothesis.supporting_evidence_ids
                    ],
                    responsible_dag_id=hypothesis.responsible_dag_id,
                    responsible_task_id=hypothesis.responsible_task_id,
                    created_at=hypothesis.created_at,
                )
            )

        record = None
        if state.diagnosis is not None:
            record = _to_record(state.diagnosis, attempt)
            self._session.add(record)

        await self._session.flush()
        logger.info(
            "investigation.persisted",
            incident_id=str(state.incident_id),
            attempt=attempt,
            steps=len(state.steps),
            evidence=len(state.evidence),
            hypotheses=len(state.hypotheses),
            diagnosed=record is not None,
        )
        return record

    async def steps(self, incident_id: UUID, attempt: int | None = None) -> list[InvestigationStep]:
        """The step trace, in order.

        Args:
            incident_id: The incident.
            attempt: One attempt, or every attempt when omitted.

        Returns:
            The steps, ordered as they ran.
        """
        statement = (
            select(InvestigationStep)
            .where(InvestigationStep.incident_id == incident_id)
            .order_by(InvestigationStep.attempt, InvestigationStep.sequence)
        )
        if attempt is not None:
            statement = statement.where(InvestigationStep.attempt == attempt)
        return list((await self._session.execute(statement)).scalars())

    async def evidence(self, incident_id: UUID, attempt: int | None = None) -> list[EvidenceRecord]:
        """Every piece of evidence gathered.

        Args:
            incident_id: The incident.
            attempt: One attempt, or every attempt when omitted.

        Returns:
            The evidence, oldest first.
        """
        statement = (
            select(EvidenceRecord)
            .where(EvidenceRecord.incident_id == incident_id)
            .order_by(EvidenceRecord.attempt, EvidenceRecord.collected_at)
        )
        if attempt is not None:
            statement = statement.where(EvidenceRecord.attempt == attempt)
        return list((await self._session.execute(statement)).scalars())

    async def hypotheses(
        self, incident_id: UUID, attempt: int | None = None
    ) -> list[HypothesisRecord]:
        """Every hypothesis considered, best-ranked first.

        Args:
            incident_id: The incident.
            attempt: One attempt, or every attempt when omitted.

        Returns:
            The hypotheses, including the ones that were refuted.
        """
        statement = (
            select(HypothesisRecord)
            .where(HypothesisRecord.incident_id == incident_id)
            .order_by(HypothesisRecord.attempt, HypothesisRecord.rank)
        )
        if attempt is not None:
            statement = statement.where(HypothesisRecord.attempt == attempt)
        return list((await self._session.execute(statement)).scalars())

    async def latest_diagnosis(self, incident_id: UUID) -> DiagnosisRecord | None:
        """The most recent attempt's diagnosis.

        Args:
            incident_id: The incident.

        Returns:
            The diagnosis, or ``None`` if the incident has never been diagnosed.
        """
        statement = (
            select(DiagnosisRecord)
            .where(DiagnosisRecord.incident_id == incident_id)
            .order_by(DiagnosisRecord.attempt.desc())
            .limit(1)
        )
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def _sequence_offset(self, incident_id: UUID) -> int:
        """The highest step sequence already recorded for an incident."""
        highest = (
            await self._session.execute(
                select(func.max(InvestigationStep.sequence)).where(
                    InvestigationStep.incident_id == incident_id
                )
            )
        ).scalar_one_or_none()
        return highest or 0


def _to_record(diagnosis: Diagnosis, attempt: int) -> DiagnosisRecord:
    """Turn the domain diagnosis into its row."""
    return DiagnosisRecord(
        incident_id=diagnosis.incident_id,
        attempt=attempt,
        root_cause_category=diagnosis.root_cause_category,
        summary=diagnosis.summary,
        confidence=diagnosis.confidence,
        halt_reason=diagnosis.halt_reason.value,
        evidence_chain=[str(item) for item in diagnosis.evidence_chain],
        proposed_fix=diagnosis.proposed_fix,
        responsible_dag_id=diagnosis.responsible_dag_id,
        responsible_task_id=diagnosis.responsible_task_id,
        unknowns=list(diagnosis.unknowns),
        model_used=diagnosis.model_used,
        created_at=diagnosis.created_at,
    )
