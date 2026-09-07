import asyncio
from pathlib import Path

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, func, select, text

from dag_doctor.core.models import RootCauseCategory
from dag_doctor.db.models import Base, Incident
from dag_doctor.db.repositories import IncidentRepository
from dag_doctor.db.session import session_scope

ROOT = Path(__file__).parents[2]

pytestmark = pytest.mark.integration


async def _count(session_factory) -> int:
    async with session_factory() as session:
        return (await session.execute(select(func.count()).select_from(Incident))).scalar_one()


async def test_the_migration_matches_the_models_on_postgres(migrated_session_factory, postgres_dsn):
    # The unit suite checks this on SQLite. Here it covers what only Postgres renders:
    # JSONB columns, native enum types, and the native UUID type.
    engine = create_engine(postgres_dsn)
    try:
        with engine.connect() as connection:
            differences = compare_metadata(MigrationContext.configure(connection), Base.metadata)
    finally:
        engine.dispose()

    assert differences == []


async def test_root_cause_is_a_native_database_enum(migrated_session_factory, postgres_dsn):
    # A free-text column would make accuracy a judgement call rather than a comparison.
    engine = create_engine(postgres_dsn)
    try:
        with engine.connect() as connection:
            labels = connection.execute(
                text(
                    "SELECT enumlabel FROM pg_enum e "
                    "JOIN pg_type t ON t.oid = e.enumtypid "
                    "WHERE t.typname = 'root_cause_category'"
                )
            ).scalars()
            stored = set(labels)
    finally:
        engine.dispose()

    assert stored == {member.value for member in RootCauseCategory}


async def test_two_workers_racing_produce_exactly_one_incident(
    migrated_session_factory, failure_event
):
    # The unit suite forces this code path deterministically because SQLite serialises
    # writers. This is the real thing: two concurrent transactions, one constraint.
    async def consume() -> bool:
        async with session_scope(migrated_session_factory) as session:
            _incident, created = await IncidentRepository(session).get_or_create(failure_event)
            return created

    results = await asyncio.gather(*(consume() for _ in range(5)), return_exceptions=True)
    created_flags = [result for result in results if isinstance(result, bool)]

    assert await _count(migrated_session_factory) == 1
    assert created_flags.count(True) == 1


async def test_a_redelivered_message_creates_exactly_one_incident(
    migrated_session_factory, failure_event
):
    for _ in range(3):
        async with session_scope(migrated_session_factory) as session:
            incident, _created = await IncidentRepository(session).get_or_create(failure_event)

    assert await _count(migrated_session_factory) == 1
    assert incident.delivery_count == 3
