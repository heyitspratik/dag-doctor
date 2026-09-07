"""SQLAlchemy models for the agent's own database.

Postgres is the target. The column types are declared through ``with_variant`` so the same
models also run on SQLite, which is what lets the repository tests exercise real SQL,
real constraints, and real transactions without a container. Anything genuinely
Postgres-specific is covered by the integration tests instead.

The root cause category is a database enum rather than free text. That is the decision
that makes diagnosis accuracy computable by comparison instead of by a human reading prose.
"""

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from dag_doctor.core.models import (
    HypothesisOutcome,
    IncidentStatus,
    RootCauseCategory,
)

if TYPE_CHECKING:
    from enum import Enum as PyEnum


def _enum_values(enum_type: "type[PyEnum]") -> list[str]:
    """Store an enum's values, not its member names.

    SQLAlchemy stores ``.name`` by default, which would put ``SCHEMA_DRIFT`` in a column
    whose type declares ``schema_drift``. Storing the value keeps the database, the wire
    format, and the API agreeing on one spelling.
    """
    return [member.value for member in enum_type]


#: JSONB on Postgres, plain JSON elsewhere. Tool results are stored whole rather than
#: shredded into columns: their shape is the tool's business, and the agent reads them back
#: as a unit.
JSONColumn = JSON().with_variant(JSONB(), "postgresql")

#: Native UUID on Postgres, CHAR(32) elsewhere, with UUID objects either way.
UUIDColumn = Uuid(as_uuid=True)

#: Timezone-aware everywhere. A naive timestamp in an incident record is a timestamp
#: nobody can correlate with an Airflow log.
TimestampColumn = DateTime(timezone=True)


def _utc_now() -> datetime:
    """Application-side default, so a row's time does not depend on the database's clock."""
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """Declarative base for every table in the agent's database."""


class Incident(Base):
    """One Airflow task failure the agent was told about.

    The unique constraint on the identifying tuple is the real idempotency guarantee.
    Kafka delivery is at-least-once, so the consumer will see the same failure more than
    once; the constraint is what makes a redelivery update a row rather than open a second
    investigation. Enforcing it in code alone would lose the race between two workers.
    """

    __tablename__ = "incidents"
    __table_args__ = (
        UniqueConstraint(
            "dag_id",
            "task_id",
            "run_id",
            "try_number",
            "map_index",
            name="uq_incidents_identity",
        ),
        Index("ix_incidents_received_at", "received_at"),
        Index("ix_incidents_dag_status", "dag_id", "status"),
    )

    id: Mapped[UUID] = mapped_column(UUIDColumn, primary_key=True, default=uuid4)
    dag_id: Mapped[str] = mapped_column(String(250))
    task_id: Mapped[str] = mapped_column(String(250))
    run_id: Mapped[str] = mapped_column(String(250))
    try_number: Mapped[int] = mapped_column(Integer)
    map_index: Mapped[int] = mapped_column(Integer, default=-1)

    status: Mapped[IncidentStatus] = mapped_column(
        Enum(
            IncidentStatus,
            name="incident_status",
            native_enum=True,
            values_callable=_enum_values,
        ),
        default=IncidentStatus.RECEIVED,
    )
    logical_date: Mapped[datetime | None] = mapped_column(TimestampColumn, default=None)
    failed_at: Mapped[datetime] = mapped_column(TimestampColumn)
    received_at: Mapped[datetime] = mapped_column(TimestampColumn, default=_utc_now)
    log_url: Mapped[str | None] = mapped_column(Text, default=None)
    exception_type: Mapped[str | None] = mapped_column(String(250), default=None)
    exception_message: Mapped[str | None] = mapped_column(Text, default=None)

    #: Counts redeliveries. A number above one is not an error, it is Kafka behaving as
    #: designed, and it is useful when a worker is crash-looping on one message.
    delivery_count: Mapped[int] = mapped_column(Integer, default=1)

    steps: Mapped[list["InvestigationStep"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )
    evidence: Mapped[list["EvidenceRecord"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )
    hypotheses: Mapped[list["HypothesisRecord"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )
    diagnoses: Mapped[list["DiagnosisRecord"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan"
    )


class InvestigationStep(Base):
    """One node execution, with its inputs, outputs, and cost.

    This is what turns the agent from a black box into something inspectable, and it is
    what the /investigation endpoint serves.
    """

    __tablename__ = "investigation_steps"
    __table_args__ = (
        UniqueConstraint("incident_id", "sequence", name="uq_step_sequence"),
        Index("ix_steps_incident", "incident_id"),
    )

    id: Mapped[UUID] = mapped_column(UUIDColumn, primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        UUIDColumn, ForeignKey("incidents.id", ondelete="CASCADE")
    )
    node: Mapped[str] = mapped_column(String(50))
    sequence: Mapped[int] = mapped_column(Integer)
    input: Mapped[dict[str, object]] = mapped_column(JSONColumn, default=dict)
    output: Mapped[dict[str, object]] = mapped_column(JSONColumn, default=dict)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0)
    model_used: Mapped[str | None] = mapped_column(String(120), default=None)
    started_at: Mapped[datetime] = mapped_column(TimestampColumn, default=_utc_now)

    incident: Mapped[Incident] = relationship(back_populates="steps")


class EvidenceRecord(Base):
    """One structured tool result, kept whole."""

    __tablename__ = "evidence"
    __table_args__ = (Index("ix_evidence_incident", "incident_id"),)

    id: Mapped[UUID] = mapped_column(UUIDColumn, primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        UUIDColumn, ForeignKey("incidents.id", ondelete="CASCADE")
    )
    tool_name: Mapped[str] = mapped_column(String(80))
    tool_input: Mapped[dict[str, object]] = mapped_column(JSONColumn, default=dict)
    result: Mapped[dict[str, object]] = mapped_column(JSONColumn, default=dict)
    summary: Mapped[str] = mapped_column(Text, default="")
    succeeded: Mapped[bool] = mapped_column(default=True)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    collected_at: Mapped[datetime] = mapped_column(TimestampColumn, default=_utc_now)

    incident: Mapped[Incident] = relationship(back_populates="evidence")


class HypothesisRecord(Base):
    """A candidate root cause and the outcome of the test that would refute it."""

    __tablename__ = "hypotheses"
    __table_args__ = (Index("ix_hypotheses_incident", "incident_id"),)

    id: Mapped[UUID] = mapped_column(UUIDColumn, primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        UUIDColumn, ForeignKey("incidents.id", ondelete="CASCADE")
    )
    statement: Mapped[str] = mapped_column(Text)
    root_cause_category: Mapped[RootCauseCategory] = mapped_column(
        Enum(
            RootCauseCategory,
            name="root_cause_category",
            native_enum=True,
            values_callable=_enum_values,
        )
    )
    proposed_test: Mapped[str] = mapped_column(Text)
    outcome: Mapped[HypothesisOutcome] = mapped_column(
        Enum(
            HypothesisOutcome,
            name="hypothesis_outcome",
            native_enum=True,
            values_callable=_enum_values,
        ),
        default=HypothesisOutcome.UNTESTED,
    )
    rank: Mapped[int] = mapped_column(Integer, default=0)
    test_notes: Mapped[str | None] = mapped_column(Text, default=None)
    supporting_evidence_ids: Mapped[list[object]] = mapped_column(JSONColumn, default=list)
    responsible_dag_id: Mapped[str | None] = mapped_column(String(250), default=None)
    responsible_task_id: Mapped[str | None] = mapped_column(String(250), default=None)
    created_at: Mapped[datetime] = mapped_column(TimestampColumn, default=_utc_now)

    incident: Mapped[Incident] = relationship(back_populates="hypotheses")


class DiagnosisRecord(Base):
    """The conclusion, its evidence chain, and the confidence computed for it."""

    __tablename__ = "diagnoses"
    __table_args__ = (
        CheckConstraint("confidence >= 0.0 AND confidence <= 1.0", name="ck_confidence_range"),
        Index("ix_diagnoses_incident", "incident_id"),
        Index("ix_diagnoses_category", "root_cause_category"),
    )

    id: Mapped[UUID] = mapped_column(UUIDColumn, primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        UUIDColumn, ForeignKey("incidents.id", ondelete="CASCADE")
    )
    root_cause_category: Mapped[RootCauseCategory] = mapped_column(
        Enum(
            RootCauseCategory,
            name="root_cause_category",
            native_enum=True,
            values_callable=_enum_values,
        )
    )
    summary: Mapped[str] = mapped_column(Text)
    confidence: Mapped[float] = mapped_column()
    halt_reason: Mapped[str] = mapped_column(String(50))
    evidence_chain: Mapped[list[object]] = mapped_column(JSONColumn, default=list)
    proposed_fix: Mapped[str | None] = mapped_column(Text, default=None)
    responsible_dag_id: Mapped[str | None] = mapped_column(String(250), default=None)
    responsible_task_id: Mapped[str | None] = mapped_column(String(250), default=None)
    unknowns: Mapped[list[object]] = mapped_column(JSONColumn, default=list)
    model_used: Mapped[str] = mapped_column(String(120), default="")
    created_at: Mapped[datetime] = mapped_column(TimestampColumn, default=_utc_now)

    #: Set by a human through the feedback endpoint. NULL means nobody has judged it,
    #: which is different from judged and wrong, and the accuracy metric respects that.
    human_verdict: Mapped[bool | None] = mapped_column(default=None)
    human_note: Mapped[str | None] = mapped_column(Text, default=None)

    incident: Mapped[Incident] = relationship(back_populates="diagnoses")


class SchemaSnapshot(Base):
    """A table's columns as they were, so drift can be diffed rather than guessed at."""

    __tablename__ = "schema_snapshots"
    __table_args__ = (
        UniqueConstraint("connection", "table_name", "captured_at", name="uq_snapshot_identity"),
        Index("ix_snapshots_lookup", "connection", "table_name", "captured_at"),
    )

    id: Mapped[UUID] = mapped_column(UUIDColumn, primary_key=True, default=uuid4)
    connection: Mapped[str] = mapped_column(String(80))
    table_name: Mapped[str] = mapped_column(String(250))
    columns: Mapped[list[object]] = mapped_column(JSONColumn, default=list)
    captured_at: Mapped[datetime] = mapped_column(
        TimestampColumn, default=_utc_now, server_default=func.now()
    )
