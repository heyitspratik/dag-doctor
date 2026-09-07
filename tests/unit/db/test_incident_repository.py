import pytest
from sqlalchemy import func, select

from dag_doctor.core.exceptions import PersistenceError
from dag_doctor.core.models import IncidentStatus
from dag_doctor.db.models import Incident
from dag_doctor.db.repositories import IncidentRepository
from dag_doctor.db.session import session_scope


async def _count(session_factory) -> int:
    async with session_factory() as session:
        return (await session.execute(select(func.count()).select_from(Incident))).scalar_one()


async def test_a_new_failure_opens_an_incident(session_factory, failure_event):
    async with session_scope(session_factory) as session:
        incident, created = await IncidentRepository(session).get_or_create(failure_event)

    assert created is True
    assert incident.status is IncidentStatus.RECEIVED
    assert incident.exception_type == "UndefinedColumn"
    assert incident.delivery_count == 1


async def test_a_redelivered_message_creates_exactly_one_incident(session_factory, failure_event):
    # Kafka delivers at least once, so this is ordinary behaviour rather than an edge
    # case. Two incidents for one task failure would mean two investigations and two
    # diagnoses for the same event.
    ids = []
    for _ in range(3):
        async with session_scope(session_factory) as session:
            incident, created = await IncidentRepository(session).get_or_create(failure_event)
            ids.append((incident.id, created))

    assert await _count(session_factory) == 1
    assert [created for _id, created in ids] == [True, False, False]
    assert len({incident_id for incident_id, _ in ids}) == 1


async def test_redeliveries_are_counted_rather_than_hidden(session_factory, failure_event):
    for _ in range(3):
        async with session_scope(session_factory) as session:
            incident, _created = await IncidentRepository(session).get_or_create(failure_event)

    assert incident.delivery_count == 3


async def test_the_database_arbitrates_when_a_worker_races_and_loses(
    session_factory, failure_event, monkeypatch
):
    # The repository does not trust its own prior SELECT, because two workers can both
    # see nothing and both insert. Here the SELECT is forced to miss so the constraint is
    # what decides, which is exactly what happens under real concurrency. SQLite
    # serialises writers, so the race itself is reproduced in the integration suite; this
    # verifies the losing worker's code path, including the savepoint rollback.
    async with session_scope(session_factory) as session:
        await IncidentRepository(session).get_or_create(failure_event)

    async with session_scope(session_factory) as session:
        repository = IncidentRepository(session)
        real_find = repository._find
        calls = {"n": 0}

        async def find_nothing_the_first_time(event):
            calls["n"] += 1
            return None if calls["n"] == 1 else await real_find(event)

        monkeypatch.setattr(repository, "_find", find_nothing_the_first_time)
        incident, created = await repository.get_or_create(failure_event)

    assert created is False
    assert incident.delivery_count == 2
    assert await _count(session_factory) == 1


async def test_a_conflict_that_is_not_a_redelivery_is_reported_rather_than_swallowed(
    session_factory, failure_event, monkeypatch
):
    # If the insert conflicts but no matching row exists, the constraint that fired was
    # not the idempotency one. Returning a wrong incident would be worse than failing.
    async with session_scope(session_factory) as session:
        await IncidentRepository(session).get_or_create(failure_event)

    with pytest.raises(PersistenceError):
        async with session_scope(session_factory) as session:
            repository = IncidentRepository(session)

            async def never_finds(_event):
                return None

            monkeypatch.setattr(repository, "_find", never_finds)
            await repository.get_or_create(failure_event)


@pytest.mark.parametrize(
    ("field", "value"),
    [("try_number", 2), ("map_index", 0), ("task_id", "land_raw_orders"), ("run_id", "other")],
)
async def test_a_genuinely_different_failure_opens_its_own_incident(
    session_factory, failure_event, field, value
):
    other = failure_event.model_copy(update={field: value})

    for event in (failure_event, other):
        async with session_scope(session_factory) as session:
            await IncidentRepository(session).get_or_create(event)

    assert await _count(session_factory) == 2


async def test_an_incident_can_be_moved_through_its_lifecycle(session_factory, failure_event):
    async with session_scope(session_factory) as session:
        repository = IncidentRepository(session)
        incident, _created = await repository.get_or_create(failure_event)
        await repository.set_status(incident, IncidentStatus.INVESTIGATING)

    async with session_scope(session_factory) as session:
        reloaded = await IncidentRepository(session).get(incident.id)

    assert reloaded is not None
    assert reloaded.status is IncidentStatus.INVESTIGATING


async def test_fetching_an_unknown_incident_returns_nothing(session_factory, failure_event):
    from uuid import uuid4

    async with session_scope(session_factory) as session:
        assert await IncidentRepository(session).get(uuid4()) is None


async def test_a_failed_unit_of_work_leaves_no_incident_behind(session_factory, failure_event):
    with pytest.raises(RuntimeError):
        async with session_scope(session_factory) as session:
            await IncidentRepository(session).get_or_create(failure_event)
            raise RuntimeError("the investigation blew up")

    assert await _count(session_factory) == 0
