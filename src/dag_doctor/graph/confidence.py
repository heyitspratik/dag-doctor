"""Confidence, computed in code.

A model asked how confident it is will say ninety percent whether it is right or not, so
nothing here reads a self-reported number. Confidence is derived from the shape of the
investigation: whether the failure matched a signature we already recognise, whether a
hypothesis survived a real test, how many independent tools support the conclusion, and
how many hypotheses had to be discarded first.

Every term is bounded and the breakdown is returned alongside the total, so a reader can
see why a diagnosis scored what it did rather than being asked to trust it. The full
rationale is in docs/confidence.md.
"""

import re
from dataclasses import dataclass

from pydantic import BaseModel, Field

from dag_doctor.core.models import HypothesisOutcome, RootCauseCategory
from dag_doctor.graph.state import InvestigationState

#: Where every investigation starts. Deliberately low: having merely run is not evidence.
BASE_CONFIDENCE = 0.15

#: Ceilings for each contributing term. They sum with the base to 1.0, so a diagnosis can
#: only reach certainty by matching a known signature, surviving its test, and being
#: supported by several independent tools.
MAX_SIGNATURE = 0.20
MAX_HYPOTHESIS = 0.30
MAX_EVIDENCE = 0.35

#: Independent tools beyond which more evidence stops adding confidence. Four different
#: tools agreeing is a strong result; a fifth is usually the same story retold.
EVIDENCE_SATURATION = 4

#: Charged per discarded hypothesis. A cause found on the fourth attempt is worth less
#: than one found on the first, because the ones before it were also plausible.
REFUTATION_PENALTY = 0.05
MAX_REFUTATION_PENALTY = 0.15


@dataclass(frozen=True)
class Signature:
    """A failure pattern recognisable from the exception alone."""

    name: str
    pattern: re.Pattern[str]
    category: RootCauseCategory
    #: How decisive this pattern is by itself, from zero to one. Patterns that admit more
    #: than one explanation score lower, and that is what stops triage short-circuiting an
    #: investigation it should have run.
    strength: float


#: Ordered: the first match wins, so the more specific patterns come first.
KNOWN_SIGNATURES: tuple[Signature, ...] = (
    Signature(
        name="undefined_column",
        pattern=re.compile(r"column\s+\"?[\w.]+\"?\s+does not exist|UndefinedColumn", re.I),
        category=RootCauseCategory.SCHEMA_DRIFT,
        strength=1.0,
    ),
    Signature(
        name="not_null_violation",
        pattern=re.compile(r"null value in column|NotNullViolation|violates not-null", re.I),
        category=RootCauseCategory.DATA_QUALITY_REGRESSION,
        strength=0.9,
    ),
    Signature(
        name="type_mismatch",
        pattern=re.compile(
            r"invalid input syntax for type|could not convert|InvalidTextRepresentation"
            r"|unsupported operand type",
            re.I,
        ),
        category=RootCauseCategory.TYPE_MISMATCH,
        strength=0.9,
    ),
    Signature(
        name="query_timeout",
        # Ordered before connection_failure on purpose. A query cancelled by a statement
        # timeout is a runaway query, not a network problem, and the two need opposite
        # fixes: rewrite the join, or retry with backoff.
        pattern=re.compile(
            r"canceling statement due to statement timeout|QueryCanceled"
            r"|due to (?:statement|lock) timeout",
            re.I,
        ),
        category=RootCauseCategory.QUERY_DEFECT,
        strength=0.7,
    ),
    Signature(
        name="duplicate_key",
        pattern=re.compile(r"duplicate key value violates unique constraint|UniqueViolation", re.I),
        category=RootCauseCategory.QUERY_DEFECT,
        strength=0.7,
    ),
    Signature(
        name="connection_failure",
        pattern=re.compile(
            r"connection refused|could not connect|connection reset"
            r"|ConnectTimeout|ReadTimeout|ConnectionError"
            r"|timed out (?:connecting|reading|waiting)|connection timed out"
            r"|OperationalError.*server closed",
            re.I,
        ),
        category=RootCauseCategory.TRANSIENT_INFRASTRUCTURE,
        strength=0.85,
    ),
    Signature(
        name="out_of_memory",
        pattern=re.compile(r"MemoryError|out of memory|Killed process|OOMKilled", re.I),
        category=RootCauseCategory.RESOURCE_EXHAUSTION,
        strength=0.8,
    ),
    Signature(
        name="upstream_failed",
        pattern=re.compile(r"upstream_failed|UpstreamFailedError", re.I),
        category=RootCauseCategory.UPSTREAM_DEPENDENCY_FAILURE,
        strength=0.9,
    ),
    Signature(
        name="missing_relation",
        pattern=re.compile(r"relation\s+\"?[\w.]+\"?\s+does not exist|UndefinedTable", re.I),
        # Genuinely ambiguous: the table may have been renamed, or the upstream job that
        # creates it may simply not have run. Scored low so triage never shortcuts it.
        category=RootCauseCategory.MISSING_UPSTREAM_DATA,
        strength=0.55,
    ),
)


class ConfidenceBreakdown(BaseModel):
    """The terms behind a confidence score, so the number can be argued with."""

    signature_match: float = Field(ge=0.0)
    hypothesis_test: float = Field(ge=0.0)
    evidence_support: float = Field(ge=0.0)
    refutation_penalty: float = Field(le=0.0)
    total: float = Field(ge=0.0, le=1.0)
    independent_tools: int = 0
    matched_signature: str | None = None

    def explain(self) -> str:
        """One line showing how the total was arrived at."""
        return (
            f"{self.total:.2f} = {BASE_CONFIDENCE:.2f} base "
            f"+ {self.signature_match:.2f} signature "
            f"+ {self.hypothesis_test:.2f} test "
            f"+ {self.evidence_support:.2f} evidence "
            f"{self.refutation_penalty:+.2f} refutations"
        )


def match_signature(exception_type: str | None, message: str | None) -> Signature | None:
    """Find the known failure pattern this error matches, if any.

    Args:
        exception_type: The exception class name, if the event carried one.
        message: The exception message, if the event carried one.

    Returns:
        The first matching signature, or ``None`` when nothing recognisable matched.
    """
    haystack = " ".join(part for part in (exception_type, message) if part)
    if not haystack.strip():
        return None
    return next(
        (signature for signature in KNOWN_SIGNATURES if signature.pattern.search(haystack)),
        None,
    )


def independent_tool_count(state: InvestigationState) -> int:
    """How many distinct tools produced usable evidence.

    Distinct tools, not evidence items. Calling one tool three times is one line of
    argument repeated, and counting it as three would let a confident-sounding
    investigation manufacture its own support.
    """
    return len({item.tool_name for item in state.successful_evidence})


def compute_confidence(state: InvestigationState) -> ConfidenceBreakdown:
    """Score how much the investigation actually supports its conclusion.

    Args:
        state: The investigation, at the point a conclusion is being drawn.

    Returns:
        The total and the terms it came from.
    """
    signature = match_signature(state.failure.exception_type, state.failure.exception_message)
    signature_term = MAX_SIGNATURE * signature.strength if signature else 0.0

    hypothesis = state.current_hypothesis
    hypothesis_term = 0.0
    if hypothesis is not None:
        if hypothesis.outcome is HypothesisOutcome.CONFIRMED:
            hypothesis_term = MAX_HYPOTHESIS
        elif hypothesis.outcome is HypothesisOutcome.INCONCLUSIVE:
            # A test that ran and decided nothing is worth a little: it at least failed to
            # refute. An untested hypothesis is worth nothing, because it is a guess.
            hypothesis_term = MAX_HYPOTHESIS * 0.2

    tools = independent_tool_count(state)
    evidence_term = MAX_EVIDENCE * min(tools, EVIDENCE_SATURATION) / EVIDENCE_SATURATION

    penalty = -min(REFUTATION_PENALTY * len(state.refuted_hypotheses), MAX_REFUTATION_PENALTY)

    total = BASE_CONFIDENCE + signature_term + hypothesis_term + evidence_term + penalty
    return ConfidenceBreakdown(
        signature_match=round(signature_term, 4),
        hypothesis_test=round(hypothesis_term, 4),
        evidence_support=round(evidence_term, 4),
        refutation_penalty=round(penalty, 4),
        total=round(min(max(total, 0.0), 1.0), 4),
        independent_tools=tools,
        matched_signature=signature.name if signature else None,
    )


def triage_confidence(signature: Signature | None, model_agrees: bool) -> float:
    """How far triage alone can be trusted.

    Two things have to line up before an investigation is skipped: a pattern we already
    recognise, and a model that independently reaches the same category. Either alone is
    not enough, and a pattern that admits more than one explanation is never enough.

    Args:
        signature: The matched failure pattern, if any.
        model_agrees: Whether the model classified it the same way.

    Returns:
        A confidence between zero and one.
    """
    if signature is None:
        return 0.0
    if not model_agrees:
        # Disagreement is informative. The signature may be right, but the gap is exactly
        # the case that deserves tools rather than a shortcut.
        return signature.strength * 0.5
    return signature.strength
