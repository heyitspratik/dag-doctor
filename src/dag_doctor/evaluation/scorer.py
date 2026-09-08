"""Scoring the agent against the seeded scenarios.

Two numbers matter and they are reported separately. Category accuracy asks whether the
agent named the right kind of failure. Attribution accuracy asks whether it found the task
actually at fault, which is the harder question and the one a log grep cannot answer.

An inconclusive diagnosis scores as wrong. That is the honest treatment: an agent that
declines to answer has not diagnosed the failure, and averaging it away as a partial
success would make the headline number meaningless.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from dag_doctor.core.models import RootCauseCategory
from dag_doctor.evaluation.scenarios import Scenario


@dataclass(frozen=True)
class Observed:
    """What the agent actually produced for one scenario."""

    incident_id: str | None = None
    category: RootCauseCategory | None = None
    responsible_task: str | None = None
    confidence: float = 0.0
    conclusive: bool = False
    halt_reason: str | None = None
    summary: str = ""
    model_used: str = ""
    duration_s: float | None = None
    diagnosed_at: datetime | None = None

    @property
    def diagnosed(self) -> bool:
        """Whether the agent produced any diagnosis at all."""
        return self.incident_id is not None


@dataclass(frozen=True)
class ScenarioResult:
    """How the agent did on one scenario."""

    scenario: Scenario
    observed: Observed

    @property
    def category_correct(self) -> bool:
        """Whether the root cause category matches the answer key.

        The control is correct precisely when nothing was diagnosed, because an incident
        for a DAG that succeeded is a false positive and a noisy agent is an ignored one.
        """
        if not self.scenario.expects_failure:
            return not self.observed.diagnosed
        if not self.observed.conclusive:
            return False
        return self.observed.category is self.scenario.expected_category

    @property
    def attribution_correct(self) -> bool | None:
        """Whether the agent named the task genuinely at fault.

        ``None`` where the scenario does not test attribution, so those cases are excluded
        from the denominator rather than counted as free marks.
        """
        if not self.scenario.scores_attribution:
            return None
        if not self.observed.conclusive:
            return False
        return self.observed.responsible_task == self.scenario.expected_responsible_task

    @property
    def verdict(self) -> str:
        """A short label for the table."""
        if not self.scenario.expects_failure:
            return "correct" if self.category_correct else "false positive"
        if not self.observed.diagnosed:
            return "no diagnosis"
        if not self.observed.conclusive:
            return "inconclusive"
        return "correct" if self.category_correct else "wrong"


@dataclass(frozen=True)
class Scorecard:
    """The accuracy table, and the numbers behind it."""

    results: list[ScenarioResult]
    model: str = ""

    @property
    def category_accuracy(self) -> float:
        """Fraction of scenarios whose category was right."""
        return _fraction([result.category_correct for result in self.results])

    @property
    def attribution_accuracy(self) -> float:
        """Fraction of the scenarios that test attribution where it was right."""
        judged = [
            result.attribution_correct
            for result in self.results
            if result.attribution_correct is not None
        ]
        return _fraction(judged)

    @property
    def conclusive_rate(self) -> float:
        """Fraction of the failing scenarios where the agent committed to an answer."""
        failing = [result for result in self.results if result.scenario.expects_failure]
        return _fraction([result.observed.conclusive for result in failing])

    @property
    def mean_confidence(self) -> float:
        """Mean confidence over the diagnoses that were actually produced."""
        scores = [
            result.observed.confidence for result in self.results if result.observed.diagnosed
        ]
        return round(sum(scores) / len(scores), 3) if scores else 0.0

    @property
    def calibration_gap(self) -> float:
        """Mean confidence on wrong answers minus mean confidence on right ones.

        A positive number means the agent was more sure when it was wrong, which is the
        single most useful thing this harness can tell you and the reason confidence is
        reported next to correctness rather than on its own.
        """
        right = [
            r.observed.confidence
            for r in self.results
            if r.category_correct and r.observed.diagnosed
        ]
        wrong = [
            r.observed.confidence
            for r in self.results
            if not r.category_correct and r.observed.diagnosed
        ]
        if not right or not wrong:
            return 0.0
        return round(sum(wrong) / len(wrong) - sum(right) / len(right), 3)

    def to_markdown(self) -> str:
        """Render the accuracy table, for the README and for a CI job summary."""
        header = (
            f"### Accuracy: {self.model or 'unspecified model'}\n\n"
            f"| Scenario | Expected | Diagnosed | Confidence | Attribution | Verdict |\n"
            f"|---|---|---|---|---|---|\n"
        )
        rows = "".join(self._row(result) for result in self.results)
        summary = (
            f"\n**Category accuracy: {self.category_accuracy:.0%}** "
            f"({_count(r.category_correct for r in self.results)}/{len(self.results)})  \n"
            f"Attribution accuracy: {self.attribution_accuracy:.0%}  \n"
            f"Conclusive on failing scenarios: {self.conclusive_rate:.0%}  \n"
            f"Mean confidence: {self.mean_confidence:.2f}  \n"
            f"Calibration gap (wrong minus right): {self.calibration_gap:+.2f}\n"
        )
        return header + rows + summary

    def _row(self, result: ScenarioResult) -> str:
        """One line of the table."""
        expected = (
            result.scenario.expected_category.value
            if result.scenario.expected_category
            else "no incident"
        )
        diagnosed = (
            result.observed.category.value
            if result.observed.category
            else ("none" if not result.observed.diagnosed else "unknown")
        )
        attribution = {None: "n/a", True: "correct", False: "wrong"}[result.attribution_correct]
        return (
            f"| `{result.scenario.dag_id}` | {expected} | {diagnosed} "
            f"| {result.observed.confidence:.2f} | {attribution} | {result.verdict} |\n"
        )


def _fraction(flags: list[bool]) -> float:
    """Proportion of true values, or zero for an empty list."""
    return round(sum(flags) / len(flags), 3) if flags else 0.0


def _count(flags: Iterable[bool]) -> int:
    """Count true values."""
    return sum(1 for flag in flags if flag)
