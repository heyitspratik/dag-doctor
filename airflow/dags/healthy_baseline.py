"""The control: a DAG that simply works.

Every seeded failure proves the agent can diagnose something. This proves it stays quiet
when there is nothing to diagnose. The failure callback is attached exactly as it is on
the broken DAGs, so a run of this producing an incident is a bug in the callback, not a
finding about the pipeline.

Deliberately touches no column that any other scenario drifts, so it keeps succeeding
whatever order the seeded failures are triggered in.
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

BUILD_ORDERS_DAILY = """
INSERT INTO analytics.orders_daily (order_date, order_count, revenue_cents)
SELECT
    order_ts::date     AS order_date,
    count(*)           AS order_count,
    sum(amount_cents)  AS revenue_cents
FROM raw.orders
GROUP BY order_ts::date
ON CONFLICT (order_date) DO UPDATE SET
    order_count   = EXCLUDED.order_count,
    revenue_cents = EXCLUDED.revenue_cents,
    built_at      = now();
"""

with DAG(
    dag_id="healthy_baseline",
    description="Succeeds every run. The agent must never be invoked for this DAG.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dag-doctor", "control"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="build_orders_daily",
        conn_id="warehouse",
        sql=BUILD_ORDERS_DAILY,
    )
