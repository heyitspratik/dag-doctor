"""Seeded failure: an accidental cartesian join.

``raw.order_tags`` has several rows per order, so joining it to ``raw.orders`` on
``order_id`` without aggregating first multiplies the row count many times over. The query
is then cancelled by a statement timeout.

Correct diagnosis: a query defect, identifying the join condition. Not transient
infrastructure, which is what "the query timed out" looks like from a distance and is the
mistake worth avoiding here: retrying a cartesian join simply times out again.

The statement timeout stands in for genuinely exhausting memory. That is deliberate, and
honest: a demonstration that OOM-kills its own container is not a demonstration, and in
practice a runaway join meets a statement timeout far more often than it meets the OOM
killer.
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

# The defect is the missing aggregation on order_tags: one order has forty tags, so every
# order row is multiplied by its tag count before anything is summed. The amplifier is
# sized so the product genuinely outruns the statement timeout; at 400 it finished inside
# it and the scenario silently succeeded, which is a seeded failure that does not fail.
EXPLODING_JOIN = """
SET statement_timeout = '20s';

DROP TABLE IF EXISTS analytics.order_tag_revenue;

CREATE TABLE analytics.order_tag_revenue AS
SELECT
    tags.tag,
    count(*)                 AS order_count,
    sum(orders.amount_cents) AS revenue_cents
FROM raw.orders AS orders
JOIN raw.order_tags AS tags ON tags.order_id = orders.order_id
CROSS JOIN generate_series(1, 20000) AS amplifier
GROUP BY tags.tag;
"""

with DAG(
    dag_id="bad_sql_join_explosion",
    description="A join that multiplies rows instead of matching them.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dag-doctor", "seeded-failure", "query-defect"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="build_order_tag_revenue",
        conn_id="warehouse",
        sql=EXPLODING_JOIN,
        split_statements=True,
    )
