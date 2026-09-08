"""What each seeded failure is, and what the right answer would be.

This is the answer key, and it is deliberately a single, readable table rather than
something inferred from the DAG files. The accuracy number in the README means nothing
unless a reader can see exactly what it was scored against and disagree with it.

Every scenario names the task whose failure is scored. Several DAGs fail more than one
task, and scoring the wrong one would flatter the agent: attributing
``extract_partner_feed`` to itself is trivial, while attributing
``publish_partner_metrics`` to it is the thing worth measuring.
"""

from dataclasses import dataclass

from dag_doctor.core.models import RootCauseCategory


@dataclass(frozen=True)
class Scenario:
    """One seeded failure and its correct diagnosis."""

    dag_id: str
    #: The task whose incident is scored, which is not always the first task to fail.
    failing_task: str
    expected_category: RootCauseCategory | None
    #: The task genuinely at fault, when that differs from the one that failed. ``None``
    #: means attribution is not scored for this scenario.
    expected_responsible_task: str | None
    description: str
    #: The control does not fail, and producing any incident for it is itself the error.
    expects_failure: bool = True

    @property
    def scores_attribution(self) -> bool:
        """Whether this scenario tests finding the true culprit."""
        return self.expected_responsible_task is not None


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        dag_id="schema_drift_orders",
        failing_task="build_orders_by_customer",
        expected_category=RootCauseCategory.SCHEMA_DRIFT,
        expected_responsible_task="land_raw_orders",
        description="An upstream rename breaks a downstream aggregate.",
    ),
    Scenario(
        dag_id="null_explosion_customers",
        failing_task="build_customer_dim",
        expected_category=RootCauseCategory.DATA_QUALITY_REGRESSION,
        expected_responsible_task="land_raw_customers",
        description="A source column goes mostly null; a not-null constraint fails downstream.",
    ),
    Scenario(
        dag_id="upstream_dependency_failure",
        failing_task="publish_partner_metrics",
        expected_category=RootCauseCategory.UPSTREAM_DEPENDENCY_FAILURE,
        expected_responsible_task="extract_partner_feed",
        description="The real failure is two tasks upstream; this task fails on missing input.",
    ),
    Scenario(
        dag_id="connection_timeout_api",
        failing_task="enrich_from_partner_api",
        expected_category=RootCauseCategory.TRANSIENT_INFRASTRUCTURE,
        expected_responsible_task=None,
        description="An external API call times out; the fix is retry policy, not code.",
    ),
    Scenario(
        dag_id="bad_sql_join_explosion",
        failing_task="build_order_tag_revenue",
        expected_category=RootCauseCategory.QUERY_DEFECT,
        expected_responsible_task=None,
        description="An accidental cartesian join runs away and is cancelled.",
    ),
    Scenario(
        dag_id="stale_partition_missing",
        failing_task="build_event_summary",
        expected_category=RootCauseCategory.MISSING_UPSTREAM_DATA,
        expected_responsible_task=None,
        description="The expected date partition never landed upstream.",
    ),
    Scenario(
        dag_id="type_coercion_failure",
        failing_task="load_payments",
        expected_category=RootCauseCategory.TYPE_MISMATCH,
        expected_responsible_task=None,
        description="A string arrives where a numeric is expected.",
    ),
    Scenario(
        dag_id="healthy_baseline",
        failing_task="build_orders_daily",
        expected_category=None,
        expected_responsible_task=None,
        description="Succeeds every run. The agent must never be invoked.",
        expects_failure=False,
    ),
)


def by_dag_id(dag_id: str) -> Scenario | None:
    """Find a scenario by its DAG.

    Args:
        dag_id: The DAG to look up.

    Returns:
        The scenario, or ``None`` if the DAG is not one of the seeded ones.
    """
    return next((scenario for scenario in SCENARIOS if scenario.dag_id == dag_id), None)
