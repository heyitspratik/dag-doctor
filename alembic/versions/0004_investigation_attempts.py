"""Number each investigation attempt, so a replay sits beside the original.

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-08

Replaying a past incident against a changed prompt or a different model is the main reason
this project puts an event log between Airflow and the agent. That is worth nothing if the
replay overwrites what it is meant to be compared against.

Step sequence numbers stay unique per incident rather than per attempt: a replay continues
the numbering, which keeps the existing constraint meaningful and avoids rebuilding the
table on backends that cannot alter a constraint in place.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = ("investigation_steps", "evidence", "hypotheses", "diagnoses")


def upgrade() -> None:
    for table in _TABLES:
        op.add_column(
            table,
            sa.Column("attempt", sa.Integer(), nullable=False, server_default=sa.text("1")),
        )
    op.create_index("ix_diagnoses_incident_attempt", "diagnoses", ["incident_id", "attempt"])


def downgrade() -> None:
    op.drop_index("ix_diagnoses_incident_attempt", table_name="diagnoses")
    for table in _TABLES:
        op.drop_column(table, "attempt")
