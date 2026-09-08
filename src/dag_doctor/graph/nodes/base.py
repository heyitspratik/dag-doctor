"""What every node has in common.

Each node records a step in the trace, whatever happens inside it. Doing that here rather
than in each node means a new node cannot forget to, and the "show your work" endpoint is
never quietly missing a row.

Nodes also convert a provider failure into a halt rather than an exception. An
investigation that dies mid-flight leaves an incident stuck in ``investigating``; one that
halts produces an inconclusive diagnosis saying what it had managed to learn.
"""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar

from pydantic import JsonValue

from dag_doctor.core.exceptions import DagDoctorError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import HaltReason, NodeName
from dag_doctor.graph.state import InvestigationState, StepRecord

logger = get_logger(__name__)

#: What a node hands back to LangGraph: the state fields it changed.
type NodeResult = dict[str, object]


@dataclass
class NodeUpdate:
    """A node's outcome, separated from how it is recorded."""

    updates: NodeResult = field(default_factory=dict)
    step_input: dict[str, JsonValue] = field(default_factory=dict)
    step_output: dict[str, JsonValue] = field(default_factory=dict)
    model_used: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0


class InvestigationNode(ABC):
    """Base class for the graph's nodes."""

    node: ClassVar[NodeName]

    @abstractmethod
    async def run(self, state: InvestigationState) -> NodeUpdate:
        """Do the node's work.

        Args:
            state: The investigation so far.

        Returns:
            The state changes and what to record in the trace.
        """

    async def __call__(self, state: InvestigationState) -> NodeResult:
        """Run the node, record the step, and never raise into the graph.

        Args:
            state: The investigation so far.

        Returns:
            The state changes, always including the appended step.
        """
        started = time.perf_counter()
        try:
            outcome = await self.run(state)
        except DagDoctorError as exc:
            # A halt is a state the graph can route on. An exception is not, and would
            # strand the incident mid-investigation with nothing to show for it.
            logger.warning(
                "node.halted",
                node=self.node,
                error=exc.message,
                incident_id=str(state.incident_id),
            )
            outcome = NodeUpdate(
                updates={"halt_reason": HaltReason.INVESTIGATION_ERROR},
                step_output={"error": exc.message, "code": exc.code},
            )

        step = StepRecord(
            node=self.node,
            sequence=state.next_sequence(),
            input=outcome.step_input,
            output=outcome.step_output,
            duration_ms=int((time.perf_counter() - started) * 1000),
            prompt_tokens=outcome.prompt_tokens,
            completion_tokens=outcome.completion_tokens,
            model_used=outcome.model_used,
        )
        result = dict(outcome.updates)
        result["steps"] = [*state.steps, step]
        if outcome.model_used:
            result.setdefault("model_used", outcome.model_used)
        logger.debug(
            "node.finished",
            node=self.node,
            duration_ms=step.duration_ms,
            incident_id=str(state.incident_id),
        )
        return result
