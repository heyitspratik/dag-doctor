"""Searching the agent's own history for failures that look like this one.

This is the tool that makes the agent improve as it runs. A pipeline that broke this way
before was probably diagnosed before, and the diagnosis that was accepted then is strong
evidence now.

Matching is lexical over a normalised error signature, not semantic over embeddings. That
is a deliberate choice rather than a shortcut. Error strings are not prose: the tokens that
identify a failure are literal identifiers such as a column name or an exception class,
which exact overlap captures and embeddings blur, and staying lexical keeps this tool free,
deterministic, and testable without a model running anywhere.
"""

import re
from datetime import datetime
from typing import ClassVar
from uuid import UUID

from sqlalchemy import Select, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dag_doctor.core.models import RootCauseCategory
from dag_doctor.db.models import DiagnosisRecord, Incident
from dag_doctor.tools.base import BaseTool, ToolInput, ToolOutput

#: Digits, quoted literals, UUIDs and hex blobs vary between two instances of the same
#: failure, so they are replaced rather than compared.
_VOLATILE = [
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I), " "),
    (re.compile(r"\b0x[0-9a-f]+\b", re.I), " "),
    # No trailing word boundary: requiring one makes the class separators backtrack, so
    # "30.5s" would strip to "5s" and two instances of one timeout would not match.
    (re.compile(r"\b\d[\d_.:+-]*"), " "),
]

_TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{2,}")

#: Words that appear in most tracebacks and so carry no signal about which failure this is.
_STOPWORDS = frozenset(
    {
        "the",
        "and",
        "for",
        "with",
        "from",
        "that",
        "this",
        "was",
        "were",
        "not",
        "none",
        "error",
        "exception",
        "traceback",
        "most",
        "recent",
        "call",
        "last",
        "line",
        "file",
        "self",
        "return",
        "raise",
        "during",
        "handling",
        "above",
        "another",
        "occurred",
        "airflow",
        "python",
        "site",
        "packages",
        "task",
        "run",
        "def",
        "cls",
    }
)

#: Below this, two failures have little more in common than being failures.
MIN_SIMILARITY = 0.25

#: Candidates pulled from the database before ranking. Bounded so a large incident history
#: cannot turn one tool call into a table scan.
CANDIDATE_LIMIT = 200


class SimilarIncident(ToolOutput):
    """One past incident that resembles this one, and how it was resolved."""

    incident_id: str
    dag_id: str
    task_id: str
    similarity: float
    exception_type: str | None
    root_cause_category: RootCauseCategory | None
    summary: str | None
    proposed_fix: str | None
    confidence: float | None
    human_verdict: bool | None
    occurred_at: datetime

    def summarise(self) -> str:
        """One line describing this match."""
        cause = self.root_cause_category.value if self.root_cause_category else "undiagnosed"
        return (
            f"{self.dag_id}.{self.task_id} on {self.occurred_at:%Y-%m-%d} "
            f"({self.similarity:.0%} similar): {cause}"
        )


class SimilarIncidents(ToolOutput):
    """Past incidents ranked by how closely they match the signature searched for."""

    signature: str
    matches: list[SimilarIncident]
    searched: int

    @property
    def confirmed_matches(self) -> list[SimilarIncident]:
        """Matches whose diagnosis a human confirmed.

        Weighted more heavily than the rest: a past diagnosis nobody checked is a guess
        the agent made, and treating it as evidence would let one early mistake compound.
        """
        return [match for match in self.matches if match.human_verdict is True]

    def summarise(self) -> str:
        """One line describing what history had to say."""
        if not self.matches:
            return f"no past incident resembles this signature (searched {self.searched})"
        best = self.matches[0]
        confirmed = len(self.confirmed_matches)
        return (
            f"{len(self.matches)} similar past incident(s), {confirmed} human-confirmed; "
            f"closest is {best.summarise()}"
        )


class SimilarIncidentsInput(ToolInput):
    """Arguments for :class:`SearchSimilarIncidents`."""

    error_signature: str
    exception_type: str | None = None
    limit: int = 5


class SearchSimilarIncidents(BaseTool[SimilarIncidentsInput, SimilarIncidents]):
    """Has this failure been seen, and diagnosed, before?"""

    name: ClassVar[str] = "search_similar_incidents"
    description: ClassVar[str] = (
        "Past incidents with a matching error signature, with the root cause each was "
        "diagnosed as and the fix proposed. Human-confirmed diagnoses are marked. Use it "
        "early: a failure seen before rarely needs investigating from scratch."
    )
    input_model = SimilarIncidentsInput

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        timeout_s: float = 30.0,
    ) -> None:
        """Initialise the tool.

        Args:
            session_factory: The agent's own database, which is its memory.
            timeout_s: Per-call timeout.
        """
        super().__init__(timeout_s)
        self._session_factory = session_factory

    async def execute(self, tool_input: SimilarIncidentsInput) -> SimilarIncidents:
        """Rank past incidents by signature overlap."""
        wanted = signature_tokens(tool_input.error_signature)
        async with self._session_factory() as session:
            incidents = list((await session.execute(self._candidates(tool_input))).scalars())
            diagnoses = await self._diagnoses_for(session, [incident.id for incident in incidents])

        matches = []
        for incident in incidents:
            candidate = signature_tokens(
                f"{incident.exception_type or ''} {incident.exception_message or ''}"
            )
            score = jaccard(wanted, candidate)
            if score < MIN_SIMILARITY:
                continue
            matches.append(_to_match(incident, diagnoses.get(incident.id), score))

        matches.sort(key=lambda match: (match.similarity, match.occurred_at), reverse=True)
        return SimilarIncidents(
            signature=" ".join(sorted(wanted)),
            matches=matches[: max(tool_input.limit, 1)],
            searched=len(incidents),
        )

    def _candidates(self, tool_input: SimilarIncidentsInput) -> Select[tuple[Incident]]:
        """Pull a bounded set of past incidents to rank.

        Narrowed by exception type when one is known, because that alone excludes most of
        the history and keeps the ranking work proportional to what could plausibly match.
        """
        conditions = (
            [
                or_(
                    Incident.exception_type == tool_input.exception_type,
                    Incident.exception_type.is_(None),
                )
            ]
            if tool_input.exception_type
            else []
        )
        return (
            select(Incident)
            .where(*conditions)
            .order_by(Incident.received_at.desc())
            .limit(CANDIDATE_LIMIT)
        )

    async def _diagnoses_for(
        self, session: AsyncSession, incident_ids: list[UUID]
    ) -> dict[UUID, DiagnosisRecord]:
        """The most recent diagnosis for each candidate incident.

        Fetched separately rather than as an outer join: most candidates are filtered out
        by similarity before their diagnosis is ever read, so joining would fetch rows
        nothing looks at.
        """
        if not incident_ids:
            return {}
        rows = (
            await session.execute(
                select(DiagnosisRecord)
                .where(DiagnosisRecord.incident_id.in_(incident_ids))
                .order_by(DiagnosisRecord.created_at.asc())
            )
        ).scalars()
        return {row.incident_id: row for row in rows}


def normalise_signature(raw: str) -> str:
    """Strip the parts of an error message that differ between two instances of it.

    Args:
        raw: An exception message, or a message and type together.

    Returns:
        The message with numbers, UUIDs and hex values removed.
    """
    text = raw
    for pattern, replacement in _VOLATILE:
        text = pattern.sub(replacement, text)
    return " ".join(text.split())


def signature_tokens(raw: str) -> frozenset[str]:
    """Reduce an error to the identifiers that distinguish it.

    Args:
        raw: An exception message, or a message and type together.

    Returns:
        The lowercased tokens worth matching on.
    """
    normalised = normalise_signature(raw)
    return frozenset(
        token.lower() for token in _TOKEN.findall(normalised) if token.lower() not in _STOPWORDS
    )


def jaccard(left: frozenset[str], right: frozenset[str]) -> float:
    """Overlap between two token sets, from zero to one.

    Args:
        left: The signature being searched for.
        right: A candidate signature.

    Returns:
        Intersection over union, or zero when either side is empty.
    """
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _to_match(
    incident: Incident, diagnosis: DiagnosisRecord | None, score: float
) -> SimilarIncident:
    """Build one ranked match from an incident and its diagnosis, if it has one."""
    return SimilarIncident(
        incident_id=str(incident.id),
        dag_id=incident.dag_id,
        task_id=incident.task_id,
        similarity=round(score, 3),
        exception_type=incident.exception_type,
        root_cause_category=diagnosis.root_cause_category if diagnosis else None,
        summary=diagnosis.summary if diagnosis else None,
        proposed_fix=diagnosis.proposed_fix if diagnosis else None,
        confidence=diagnosis.confidence if diagnosis else None,
        human_verdict=diagnosis.human_verdict if diagnosis else None,
        occurred_at=incident.received_at,
    )
