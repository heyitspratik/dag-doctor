"""Seeded failure: the real problem is two tasks upstream.

``extract_partner_feed`` fails. The tasks after it run anyway, because they are set to
``all_done``, which is common in pipelines that would rather produce partial output than
none. ``publish_partner_metrics`` then fails on input that was never written.

Correct diagnosis: attribute the incident to ``extract_partner_feed``, not to the task
that visibly failed. This is the scenario that separates an agent from a log grep: the
error message names the wrong task, and only walking the dependency graph upward finds the
one that actually broke.

The ``all_done`` trigger rule is what makes this testable. Left at the default, Airflow
would mark the downstream tasks ``upstream_failed`` without running them, no callback
would fire, and there would be no incident to misattribute.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

from dag_doctor.messaging.airflow_callback import on_task_failure

DEFAULT_ARGS = {
    "owner": "dag-doctor",
    "retries": 0,
    "on_failure_callback": on_task_failure,
}

# The genuine break: the source table this extract reads does not exist.
EXTRACT_PARTNER_FEED = """
INSERT INTO raw.partner_feed (partner_id, region)
SELECT partner_id, region FROM raw.partner_source_feed;
"""

# Harmless, and it succeeds. Its only job is to put a step between the break and the
# symptom, so the agent has to walk further than one hop to find the cause.
NORMALISE_PARTNER_FEED = """
UPDATE raw.partner_feed SET region = upper(region);
"""

# The symptom. It names partner_feed, which is empty, and says nothing about the extract.
PUBLISH_PARTNER_METRICS = """
DO $$
DECLARE
    partners bigint;
BEGIN
    SELECT count(*) INTO partners FROM raw.partner_feed;

    IF partners = 0 THEN
        RAISE EXCEPTION
            'raw.partner_feed is empty, cannot publish partner metrics';
    END IF;

    INSERT INTO analytics.partner_metrics (partner_id, order_count)
    SELECT partner_id, 0 FROM raw.partner_feed
    ON CONFLICT (partner_id) DO NOTHING;
END
$$;
"""

with DAG(
    dag_id="upstream_dependency_failure",
    description="A task fails on missing input that a task two steps upstream never wrote.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dag-doctor", "seeded-failure", "upstream"],
) as dag:
    extract_partner_feed = SQLExecuteQueryOperator(
        task_id="extract_partner_feed",
        conn_id="warehouse",
        sql=EXTRACT_PARTNER_FEED,
    )

    normalise_partner_feed = SQLExecuteQueryOperator(
        task_id="normalise_partner_feed",
        conn_id="warehouse",
        sql=NORMALISE_PARTNER_FEED,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    publish_partner_metrics = SQLExecuteQueryOperator(
        task_id="publish_partner_metrics",
        conn_id="warehouse",
        sql=PUBLISH_PARTNER_METRICS,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    extract_partner_feed >> normalise_partner_feed >> publish_partner_metrics
