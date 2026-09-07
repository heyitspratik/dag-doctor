"""The migration and the models must describe the same schema.

Postgres is the target, but this runs on SQLite so that it runs on every commit rather
than only when a container is available. Anything genuinely Postgres-specific, JSONB and
the native enum types, is covered by the integration tests.
"""

import io
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, inspect

from dag_doctor.core.models import RootCauseCategory
from dag_doctor.db.models import Base

ROOT = Path(__file__).parents[3]


def _alembic_config(url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", url)
    return config


@pytest.fixture
def migrated_url(tmp_path) -> str:
    url = f"sqlite:///{tmp_path / 'dag_doctor.db'}"
    command.upgrade(_alembic_config(url), "head")
    return url


def test_the_migration_creates_every_table_the_models_declare(migrated_url):
    engine = create_engine(migrated_url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    assert set(Base.metadata.tables) <= tables


def test_the_migration_and_the_models_do_not_disagree(migrated_url):
    # This is the test that catches a column added to a model and never migrated, which
    # otherwise surfaces as a runtime error against a real database.
    engine = create_engine(migrated_url)
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(connection)
            differences = compare_metadata(context, Base.metadata)
    finally:
        engine.dispose()

    assert differences == []


def test_the_migration_can_be_rolled_back(tmp_path):
    url = f"sqlite:///{tmp_path / 'rollback.db'}"
    config = _alembic_config(url)

    command.upgrade(config, "head")
    command.downgrade(config, "base")

    engine = create_engine(url)
    try:
        remaining = set(inspect(engine).get_table_names()) - {"alembic_version"}
    finally:
        engine.dispose()

    assert remaining == set()


def _postgres_ddl() -> str:
    """Render the migration as Postgres DDL without connecting to anything."""
    buffer = io.StringIO()
    config = Config(str(ROOT / "alembic.ini"), output_buffer=buffer, stdout=buffer)
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", "postgresql+psycopg://u:p@localhost/db")
    command.upgrade(config, "head", sql=True)
    return buffer.getvalue()


def test_the_root_cause_enum_is_created_once_with_the_values_the_code_uses():
    # Two tables use this type. Without create_type=False the second would try to create
    # it again and the migration would fail on Postgres but pass everywhere else.
    ddl = _postgres_ddl()
    creations = [line for line in ddl.splitlines() if "CREATE TYPE root_cause_category" in line]

    assert len(creations) == 1
    for member in RootCauseCategory:
        assert f"'{member.value}'" in creations[0]


def test_postgres_gets_jsonb_rather_than_a_text_column():
    # The SQLite variant used by the unit tests renders JSON, so this is the only check
    # that the target dialect gets the type the queries will rely on.
    assert "JSONB NOT NULL" in _postgres_ddl()


def test_the_idempotency_constraint_reaches_postgres():
    assert (
        "CONSTRAINT uq_incidents_identity UNIQUE "
        "(dag_id, task_id, run_id, try_number, map_index)" in _postgres_ddl()
    )


def test_confidence_is_bounded_by_the_database_not_only_by_pydantic():
    assert "CHECK (confidence >= 0.0 AND confidence <= 1.0)" in _postgres_ddl()
