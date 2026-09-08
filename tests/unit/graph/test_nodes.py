"""Node behaviours that a whole-graph run does not reach."""

from uuid import uuid4

from dag_doctor.core.models import HypothesisOutcome, RootCauseCategory
from dag_doctor.core.settings import BudgetSettings
from dag_doctor.graph.builder import initial_state
from dag_doctor.graph.nodes.form_hypothesis import FormHypothesisNode
from dag_doctor.graph.nodes.test_hypothesis import TestHypothesisNode
from dag_doctor.graph.nodes.triage import TriageNode
from dag_doctor.graph.state import InvestigationState

from .conftest import ScriptedCaller, hypothesis_set, triage_answer, verdict


def _state(failure, budgets, **overrides) -> InvestigationState:
    return initial_state(uuid4(), failure, budgets).model_copy(update=overrides)


async def _formed(caller, toolbox, state) -> InvestigationState:
    updates = await FormHypothesisNode(caller, toolbox)(state)
    return state.model_copy(update=updates)


async def test_triage_disagreeing_with_the_signature_forces_an_investigation(
    toolbox, budgets, drift_failure
):
    # The message plainly says a column is missing. A model that classifies it as
    # something else may be right, but the disagreement is exactly the case that
    # deserves tools rather than a shortcut.
    caller = ScriptedCaller(
        {
            "triage": [
                triage_answer(
                    category=RootCauseCategory.TRANSIENT_INFRASTRUCTURE,
                    needs_investigation=False,
                )
            ]
        }
    )

    updates = await TriageNode(caller)(_state(drift_failure, budgets))

    assert updates["triage"].needs_investigation is True
    assert updates["triage"].confidence < 0.9


async def test_triage_tells_the_model_what_pattern_matched(toolbox, budgets, drift_failure):
    caller = ScriptedCaller({"triage": [triage_answer()]})

    await TriageNode(caller)(_state(drift_failure, budgets))

    assert "undefined_column" in caller.prompt_for("triage")


async def test_a_failure_with_no_exception_recorded_is_still_triaged(budgets, drift_failure):
    caller = ScriptedCaller({"triage": [triage_answer(category=RootCauseCategory.UNKNOWN)]})
    bare = drift_failure.model_copy(update={"exception_type": None, "exception_message": None})

    updates = await TriageNode(caller)(_state(bare, budgets))

    assert updates["triage"].matched_signature is None
    assert updates["triage"].confidence == 0.0


async def test_testing_without_a_hypothesis_does_nothing_rather_than_failing(
    toolbox, budgets, drift_failure
):
    caller = ScriptedCaller({"test_hypothesis": [verdict("confirmed")]})

    updates = await TestHypothesisNode(caller, toolbox)(_state(drift_failure, budgets))

    assert updates["steps"][-1].output == {"skipped": "no current hypothesis"}
    assert caller.call_count("test_hypothesis") == 0


async def test_a_hypothesis_with_no_executable_test_is_judged_on_what_is_known(
    toolbox, budgets, drift_failure
):
    caller = ScriptedCaller(
        {
            "form_hypothesis": [hypothesis_set(test_tool=None)],
            "test_hypothesis": [verdict("inconclusive", notes="nothing discriminated")],
        }
    )
    state = await _formed(caller, toolbox, _state(drift_failure, budgets))

    updates = await TestHypothesisNode(caller, toolbox)(state)

    assert updates["tool_calls_made"] == 0
    assert "no executable test" in caller.prompt_for("test_hypothesis")
    assert updates["current_hypothesis"].outcome is HypothesisOutcome.INCONCLUSIVE


async def test_a_test_that_cannot_be_run_within_budget_says_so_rather_than_pretending(
    toolbox, drift_failure
):
    # Judging a hypothesis on evidence that never tested it, and calling that a test, is
    # how an agent talks itself into a wrong answer.
    budgets = BudgetSettings(max_tool_calls=1)
    caller = ScriptedCaller(
        {
            "form_hypothesis": [hypothesis_set()],
            "test_hypothesis": [verdict("inconclusive")],
        }
    )
    exhausted = _state(drift_failure, budgets, tool_calls_made=1)
    state = await _formed(caller, toolbox, exhausted)

    await TestHypothesisNode(caller, toolbox)(state)

    assert "budget is exhausted" in caller.prompt_for("test_hypothesis")


async def test_a_hypothesis_citing_evidence_that_is_not_there_is_kept_anyway(
    toolbox, budgets, drift_failure
):
    # A model miscounting its citations is not a reason to discard an otherwise sound
    # hypothesis; the confidence score already reflects how much really supports it.
    caller = ScriptedCaller({"form_hypothesis": [hypothesis_set(supporting=[7, 99])]})

    updates = await FormHypothesisNode(caller, toolbox)(_state(drift_failure, budgets))

    assert updates["current_hypothesis"] is not None
    assert updates["current_hypothesis"].supporting_evidence_ids == []
