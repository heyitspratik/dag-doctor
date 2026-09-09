"""Testing a hypothesis: run its test, then judge the result.

This is the node that makes the graph a graph. Its outcome decides whether the
investigation concludes or loops back for more evidence, and the loop is why a linear
chain could not express this.

The test runs first and the model judges afterwards, given the result. Asking the model to
predict its own test's outcome would let it confirm anything.
"""

from typing import ClassVar

from pydantic import BaseModel, Field

from dag_doctor.core.models import Evidence, HypothesisOutcome, NodeName
from dag_doctor.graph import prompts
from dag_doctor.graph.arguments import ArgumentResolver
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.nodes.base import InvestigationNode, NodeUpdate
from dag_doctor.graph.state import InvestigationState
from dag_doctor.graph.toolbox import Toolbox


class TestVerdict(BaseModel):
    """What the model is asked for after a test has run."""

    outcome: HypothesisOutcome
    notes: str = Field(default="")


class TestHypothesisNode(InvestigationNode):
    """Execute the check that would refute the current hypothesis."""

    node: ClassVar[NodeName] = "test_hypothesis"

    def __init__(self, caller: ModelCaller, toolbox: Toolbox) -> None:
        """Initialise the node.

        Args:
            caller: How this node reaches a model.
            toolbox: The tools a test may run.
        """
        self._caller = caller
        self._toolbox = toolbox
        self._arguments = ArgumentResolver(caller, toolbox)

    async def run(self, state: InvestigationState) -> NodeUpdate:
        """Run the hypothesis's test and record whether it survived."""
        hypothesis = state.current_hypothesis
        if hypothesis is None:
            return NodeUpdate(
                updates={},
                step_output={"skipped": "no current hypothesis"},
            )

        evidence: list[Evidence] = []
        if hypothesis.test_tool and state.tool_calls_remaining > 0:
            arguments = dict(hypothesis.test_arguments) or await self._arguments.resolve(
                self.node, hypothesis.test_tool, state, hypothesis.proposed_test
            )
            evidence.append(
                await self._toolbox.run(hypothesis.test_tool, arguments, state.tool_context())
            )
            test_result = evidence[0].summary
        elif hypothesis.test_tool:
            # The test exists but there is no budget left to run it. Saying so is more
            # honest than judging the hypothesis on evidence that never tested it.
            test_result = "not run: the tool call budget is exhausted"
        else:
            test_result = "no executable test was proposed; judge from existing evidence alone"

        prompt = prompts.render(
            "test_hypothesis",
            statement=hypothesis.statement,
            proposed_test=hypothesis.proposed_test,
            test_result=test_result,
            evidence=_render_evidence(state, evidence),
        )
        answer = await self._caller.call(self.node, prompt, TestVerdict)

        tested = hypothesis.model_copy(
            update={"outcome": answer.value.outcome, "test_notes": answer.value.notes}
        )
        return NodeUpdate(
            updates={
                "current_hypothesis": tested,
                "hypotheses": [
                    tested if item.id == tested.id else item for item in state.hypotheses
                ],
                "evidence": [*state.evidence, *evidence],
                "tool_calls_made": state.tool_calls_made + len(evidence),
            },
            step_input={"hypothesis": hypothesis.statement, "test_tool": hypothesis.test_tool},
            step_output={"outcome": answer.value.outcome.value, "notes": answer.value.notes},
            model_used=answer.model_used,
            prompt_tokens=answer.prompt_tokens,
            completion_tokens=answer.completion_tokens,
        )


def _render_evidence(state: InvestigationState, fresh: list[Evidence]) -> str:
    """Number all the evidence, including whatever the test just produced."""
    items = [*state.evidence, *fresh]
    if not items:
        return "  (nothing gathered yet)"
    return "\n".join(
        f"  {index}. [{item.tool_name}] {item.summary}" for index, item in enumerate(items, start=1)
    )
