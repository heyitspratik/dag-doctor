"""Seeded failure: the date partition that should have landed never did.

``raw.events_daily`` carries every day up to yesterday. The summary build asks for today
and finds nothing, and says so explicitly rather than quietly writing a zero.

Correct diagnosis: missing upstream data, not a defect in this pipeline. Nothing here is
broken; the thing that feeds it did not run. Note that this failure matches no known error
signature, so the agent has to actually investigate rather than pattern-match.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from dag_doctor.messaging.airflow_callback import on_task_failure

DEFAULT_ARGS = {
    "owner": "dag-doctor",
    "retries": 0,
    "on_failure_callback": on_task_failure,
}

# Failing loudly on absent input beats writing a zero. A summary row of zero events looks
# like a quiet day and is discovered a quarter later.
BUILD_EVENT_SUMMARY = """
DO $$
DECLARE
    landed bigint;
BEGIN
    SELECT count(*) INTO landed
    FROM raw.events_daily
    WHERE event_date = current_date;

    IF landed = 0 THEN
        RAISE EXCEPTION
            'no rows landed in raw.events_daily for partition %, expected the daily '
            'extract to have written them', current_date;
    END IF;

    INSERT INTO analytics.events_daily_summary (event_date, event_count)
    VALUES (current_date, landed)
    ON CONFLICT (event_date) DO UPDATE SET event_count = EXCLUDED.event_count;
END
$$;
"""

with DAG(
    dag_id="stale_partition_missing",
    description="The expected date partition never landed upstream.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dag-doctor", "seeded-failure", "missing-data"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="build_event_summary",
        conn_id="warehouse",
        sql=BUILD_EVENT_SUMMARY,
    )
