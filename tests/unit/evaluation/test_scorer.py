"""Scoring, which is the part of the harness that decides what the README may claim."""

import pytest

from dag_doctor.core.models import RootCauseCategory
from dag_doctor.evaluation.scenarios import SCENARIOS, Scenario, by_dag_id
from dag_doctor.evaluation.scorer import Observed, ScenarioResult, Scorecard

DRIFT = by_dag_id("schema_drift_orders")
CONTROL = by_dag_id("healthy_baseline")
TIMEOUT = by_dag_id("connection_timeout_api")


def _observed(**overrides) -> Observed:
    fields = {
        "incident_id": "11111111-1111-1111-1111-111111111111",
        "category": RootCauseCategory.SCHEMA_DRIFT,
        "responsible_task": "land_raw_orders",
        "confidence": 0.86,
        "conclusive": True,
    }
    return Observed(**{**fields, **overrides})


def test_a_right_answer_scores_on_both_counts():
    result = ScenarioResult(scenario=DRIFT, observed=_observed())

    assert result.category_correct
    assert result.attribution_correct
    assert result.verdict == "correct"


def test_the_wrong_category_is_wrong():
    result = ScenarioResult(
        scenario=DRIFT, observed=_observed(category=RootCauseCategory.TRANSIENT_INFRASTRUCTURE)
    )

    assert not result.category_correct
    assert result.verdict == "wrong"


def test_blaming_the_task_that_visibly_failed_is_an_attribution_miss():
    # The distinction the whole harness exists to measure: naming the right kind of
    # failure while blaming the wrong task is not a correct diagnosis.
    result = ScenarioResult(
        scenario=DRIFT, observed=_observed(responsible_task="build_orders_by_customer")
    )

    assert result.category_correct
    assert result.attribution_correct is False


def test_an_inconclusive_diagnosis_scores_as_wrong():
    # An agent that declines to answer has not diagnosed the failure. Averaging it away
    # as a partial success would make the headline number meaningless.
    result = ScenarioResult(scenario=DRIFT, observed=_observed(conclusive=False))

    assert not result.category_correct
    assert result.attribution_correct is False
    assert result.verdict == "inconclusive"


def test_no_diagnosis_at_all_scores_as_wrong():
    result = ScenarioResult(scenario=DRIFT, observed=Observed())

    assert not result.category_correct
    assert result.verdict == "no diagnosis"


def test_the_control_is_correct_precisely_when_nothing_happened():
    # A noisy agent is an ignored agent, so an incident for a DAG that succeeded is a
    # failure of the same weight as a wrong diagnosis.
    quiet = ScenarioResult(scenario=CONTROL, observed=Observed())
    noisy = ScenarioResult(scenario=CONTROL, observed=_observed())

    assert quiet.category_correct
    assert quiet.verdict == "correct"
    assert not noisy.category_correct
    assert noisy.verdict == "false positive"


def test_scenarios_that_do_not_test_attribution_are_excluded_not_given_free_marks():
    result = ScenarioResult(scenario=TIMEOUT, observed=_observed(responsible_task=None))

    assert result.attribution_correct is None


def test_attribution_accuracy_ignores_the_scenarios_it_does_not_apply_to():
    card = Scorecard(
        results=[
            ScenarioResult(scenario=DRIFT, observed=_observed()),
            ScenarioResult(scenario=TIMEOUT, observed=_observed(responsible_task=None)),
        ]
    )

    # One attribution scenario, answered correctly, so the denominator is one not two.
    assert card.attribution_accuracy == 1.0


def test_accuracy_is_the_fraction_of_scenarios_answered_correctly():
    card = Scorecard(
        results=[
            ScenarioResult(scenario=DRIFT, observed=_observed()),
            ScenarioResult(
                scenario=DRIFT, observed=_observed(category=RootCauseCategory.QUERY_DEFECT)
            ),
        ]
    )

    assert card.category_accuracy == 0.5


def test_an_empty_run_scores_zero_rather_than_dividing_by_zero():
    assert Scorecard(results=[]).category_accuracy == 0.0
    assert Scorecard(results=[]).mean_confidence == 0.0


def test_being_more_confident_when_wrong_is_reported_as_a_calibration_gap():
    # The most useful thing this harness can say. An agent that is surest when it is
    # wrong is worse than one that is uniformly unsure, and the average alone hides that.
    card = Scorecard(
        results=[
            ScenarioResult(scenario=DRIFT, observed=_observed(confidence=0.5)),
            ScenarioResult(
                scenario=DRIFT,
                observed=_observed(confidence=0.95, category=RootCauseCategory.QUERY_DEFECT),
            ),
        ]
    )

    assert card.calibration_gap == pytest.approx(0.45)


def test_the_calibration_gap_is_zero_when_there_is_nothing_to_compare():
    card = Scorecard(results=[ScenarioResult(scenario=DRIFT, observed=_observed())])

    assert card.calibration_gap == 0.0


def test_the_conclusive_rate_ignores_the_control():
    card = Scorecard(
        results=[
            ScenarioResult(scenario=DRIFT, observed=_observed()),
            ScenarioResult(scenario=CONTROL, observed=Observed()),
        ]
    )

    assert card.conclusive_rate == 1.0


def test_the_table_reports_every_scenario_and_the_headline_number():
    card = Scorecard(
        results=[
            ScenarioResult(scenario=DRIFT, observed=_observed()),
            ScenarioResult(scenario=CONTROL, observed=Observed()),
        ],
        model="llama3.2:3b",
    )

    table = card.to_markdown()

    assert "llama3.2:3b" in table
    assert "`schema_drift_orders`" in table
    assert "`healthy_baseline`" in table
    assert "Category accuracy: 100%" in table
    assert "Calibration gap" in table


def test_the_table_names_what_the_control_expects():
    card = Scorecard(results=[ScenarioResult(scenario=CONTROL, observed=Observed())])

    assert "no incident" in card.to_markdown()


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.dag_id)
def test_every_scenario_states_what_the_right_answer_is(scenario: Scenario):
    if scenario.expects_failure:
        assert scenario.expected_category is not None
    else:
        assert scenario.expected_category is None
    assert scenario.description.endswith(".")


def test_the_answer_key_covers_all_eight_seeded_scenarios():
    assert len(SCENARIOS) == 8
    assert len({scenario.dag_id for scenario in SCENARIOS}) == 8


def test_every_root_cause_the_key_expects_is_a_real_category():
    for scenario in SCENARIOS:
        if scenario.expected_category is not None:
            assert scenario.expected_category in RootCauseCategory


def test_an_unknown_dag_is_not_a_scenario():
    assert by_dag_id("something_else") is None
