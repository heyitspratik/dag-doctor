"""Add table profiles, the baseline a data quality regression is measured against.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-07

The profiling tool writes here. The agent stays read-only against Airflow and the
warehouse; its own memory is the only thing it writes, which is what turns "this column is
90% null" into "this column was 2% null and is now 90% null".
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_COLUMN = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
UUID_COLUMN = sa.Uuid(as_uuid=True)
TIMESTAMP_COLUMN = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "table_profiles",
        sa.Column("id", UUID_COLUMN, primary_key=True),
        sa.Column("connection", sa.String(80), nullable=False),
        sa.Column("table_name", sa.String(250), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("sampled_rows", sa.Integer(), nullable=False),
        sa.Column("columns", JSON_COLUMN, nullable=False),
        sa.Column("captured_at", TIMESTAMP_COLUMN, nullable=False),
    )
    op.create_index(
        "ix_profiles_lookup", "table_profiles", ["connection", "table_name", "captured_at"]
    )


def downgrade() -> None:
    op.drop_table("table_profiles")
