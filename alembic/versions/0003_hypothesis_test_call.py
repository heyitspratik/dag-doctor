"""Record the tool call that tested each hypothesis.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-08

A hypothesis carries a falsifiable test, and the test is a tool call rather than a
sentence. Storing it makes a replayed investigation show not only what was claimed but
what was actually run to try to disprove it.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_COLUMN = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.add_column(
        "hypotheses",
        sa.Column("test_call", JSON_COLUMN, nullable=False, server_default=sa.text("'{}'")),
    )


def downgrade() -> None:
    op.drop_column("hypotheses", "test_call")
