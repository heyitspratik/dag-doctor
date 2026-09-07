"""Initial schema: incidents, investigation steps, evidence, hypotheses, diagnoses.

Revision ID: 0001
Revises:
Create Date: 2026-09-07

Column types are declared with dialect variants so this migration applies to Postgres,
which is the target, and to SQLite, which is what lets the repository tests run real SQL
against real constraints without a container. The types are spelled out here rather than
imported from the models, so that a later change to the models cannot retroactively change
what this migration did.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_COLUMN = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
UUID_COLUMN = sa.Uuid(as_uuid=True)
TIMESTAMP_COLUMN = sa.DateTime(timezone=True)

INCIDENT_STATUS = ("received", "investigating", "diagnosed", "inconclusive", "failed")
HYPOTHESIS_OUTCOME = ("untested", "confirmed", "refuted", "inconclusive")
ROOT_CAUSE_CATEGORY = (
    "schema_drift",
    "data_quality_regression",
    "upstream_dependency_failure",
    "missing_upstream_data",
    "transient_infrastructure",
    "query_defect",
    "type_mismatch",
    "resource_exhaustion",
    "configuration_error",
    "unknown",
)


def _enum(name: str, values: Sequence[str]) -> sa.Enum:
    """Reference an enum type without emitting CREATE TYPE.

    The type is created once at the top of the upgrade. Without ``create_type=False`` the
    second table to use ``root_cause_category`` would try to create it again and fail on
    Postgres, while doing nothing at all on a dialect with no native enums.
    """
    return postgresql.ENUM(*values, name=name, create_type=False)


def upgrade() -> None:
    bind = op.get_bind()
    for name, values in (
        ("incident_status", INCIDENT_STATUS),
        ("hypothesis_outcome", HYPOTHESIS_OUTCOME),
        ("root_cause_category", ROOT_CAUSE_CATEGORY),
    ):
        sa.Enum(*values, name=name).create(bind, checkfirst=True)

    op.create_table(
        "incidents",
        sa.Column("id", UUID_COLUMN, primary_key=True),
        sa.Column("dag_id", sa.String(250), nullable=False),
        sa.Column("task_id", sa.String(250), nullable=False),
        sa.Column("run_id", sa.String(250), nullable=False),
        sa.Column("try_number", sa.Integer(), nullable=False),
        sa.Column("map_index", sa.Integer(), nullable=False),
        sa.Column("status", _enum("incident_status", INCIDENT_STATUS), nullable=False),
        sa.Column("logical_date", TIMESTAMP_COLUMN, nullable=True),
        sa.Column("failed_at", TIMESTAMP_COLUMN, nullable=False),
        sa.Column("received_at", TIMESTAMP_COLUMN, nullable=False),
        sa.Column("log_url", sa.Text(), nullable=True),
        sa.Column("exception_type", sa.String(250), nullable=True),
        sa.Column("exception_message", sa.Text(), nullable=True),
        sa.Column("delivery_count", sa.Integer(), nullable=False),
        # The idempotency guarantee. Kafka delivers at least once, so this constraint is
        # what makes a redelivered failure update a row rather than open a second
        # investigation, and it holds even when two workers race.
        sa.UniqueConstraint(
            "dag_id", "task_id", "run_id", "try_number", "map_index", name="uq_incidents_identity"
        ),
    )
    op.create_index("ix_incidents_received_at", "incidents", ["received_at"])
    op.create_index("ix_incidents_dag_status", "incidents", ["dag_id", "status"])

    op.create_table(
        "investigation_steps",
        sa.Column("id", UUID_COLUMN, primary_key=True),
        sa.Column(
            "incident_id",
            UUID_COLUMN,
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("node", sa.String(50), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("input", JSON_COLUMN, nullable=False),
        sa.Column("output", JSON_COLUMN, nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("prompt_tokens", sa.Integer(), nullable=False),
        sa.Column("completion_tokens", sa.Integer(), nullable=False),
        sa.Column("model_used", sa.String(120), nullable=True),
        sa.Column("started_at", TIMESTAMP_COLUMN, nullable=False),
        sa.UniqueConstraint("incident_id", "sequence", name="uq_step_sequence"),
    )
    op.create_index("ix_steps_incident", "investigation_steps", ["incident_id"])

    op.create_table(
        "evidence",
        sa.Column("id", UUID_COLUMN, primary_key=True),
        sa.Column(
            "incident_id",
            UUID_COLUMN,
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("tool_name", sa.String(80), nullable=False),
        sa.Column("tool_input", JSON_COLUMN, nullable=False),
        sa.Column("result", JSON_COLUMN, nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("succeeded", sa.Boolean(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("collected_at", TIMESTAMP_COLUMN, nullable=False),
    )
    op.create_index("ix_evidence_incident", "evidence", ["incident_id"])

    op.create_table(
        "hypotheses",
        sa.Column("id", UUID_COLUMN, primary_key=True),
        sa.Column(
            "incident_id",
            UUID_COLUMN,
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("statement", sa.Text(), nullable=False),
        sa.Column(
            "root_cause_category",
            _enum("root_cause_category", ROOT_CAUSE_CATEGORY),
            nullable=False,
        ),
        sa.Column("proposed_test", sa.Text(), nullable=False),
        sa.Column("outcome", _enum("hypothesis_outcome", HYPOTHESIS_OUTCOME), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("test_notes", sa.Text(), nullable=True),
        sa.Column("supporting_evidence_ids", JSON_COLUMN, nullable=False),
        sa.Column("responsible_dag_id", sa.String(250), nullable=True),
        sa.Column("responsible_task_id", sa.String(250), nullable=True),
        sa.Column("created_at", TIMESTAMP_COLUMN, nullable=False),
    )
    op.create_index("ix_hypotheses_incident", "hypotheses", ["incident_id"])

    op.create_table(
        "diagnoses",
        sa.Column("id", UUID_COLUMN, primary_key=True),
        sa.Column(
            "incident_id",
            UUID_COLUMN,
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "root_cause_category",
            _enum("root_cause_category", ROOT_CAUSE_CATEGORY),
            nullable=False,
        ),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("halt_reason", sa.String(50), nullable=False),
        sa.Column("evidence_chain", JSON_COLUMN, nullable=False),
        sa.Column("proposed_fix", sa.Text(), nullable=True),
        sa.Column("responsible_dag_id", sa.String(250), nullable=True),
        sa.Column("responsible_task_id", sa.String(250), nullable=True),
        sa.Column("unknowns", JSON_COLUMN, nullable=False),
        sa.Column("model_used", sa.String(120), nullable=False),
        sa.Column("created_at", TIMESTAMP_COLUMN, nullable=False),
        sa.Column("human_verdict", sa.Boolean(), nullable=True),
        sa.Column("human_note", sa.Text(), nullable=True),
        sa.CheckConstraint("confidence >= 0.0 AND confidence <= 1.0", name="ck_confidence_range"),
    )
    op.create_index("ix_diagnoses_incident", "diagnoses", ["incident_id"])
    op.create_index("ix_diagnoses_category", "diagnoses", ["root_cause_category"])

    op.create_table(
        "schema_snapshots",
        sa.Column("id", UUID_COLUMN, primary_key=True),
        sa.Column("connection", sa.String(80), nullable=False),
        sa.Column("table_name", sa.String(250), nullable=False),
        sa.Column("columns", JSON_COLUMN, nullable=False),
        sa.Column(
            "captured_at", TIMESTAMP_COLUMN, nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "connection", "table_name", "captured_at", name="uq_snapshot_identity"
        ),
    )
    op.create_index(
        "ix_snapshots_lookup", "schema_snapshots", ["connection", "table_name", "captured_at"]
    )


def downgrade() -> None:
    op.drop_table("schema_snapshots")
    op.drop_table("diagnoses")
    op.drop_table("hypotheses")
    op.drop_table("evidence")
    op.drop_table("investigation_steps")
    op.drop_table("incidents")

    bind = op.get_bind()
    for name in ("root_cause_category", "hypothesis_outcome", "incident_status"):
        sa.Enum(name=name).drop(bind, checkfirst=True)
