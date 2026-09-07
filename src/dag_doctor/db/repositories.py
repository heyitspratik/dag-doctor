"""Repositories: the only place this package writes SQL.

Keeping queries here rather than in the consumer or the graph means the storage model can
change without the investigation logic noticing, and it gives the tests one seam to drive.
"""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dag_doctor.core.exceptions import PersistenceError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import FailureEvent, IncidentStatus
from dag_doctor.db.models import Incident

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
