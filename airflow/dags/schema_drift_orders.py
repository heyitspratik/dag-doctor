"""Seeded failure: an upstream rename breaks a downstream query.

The upstream system renames ``raw.orders.customer_id`` to ``customer_uuid``. The
downstream aggregate still selects the old name and fails with an undefined column.

Correct diagnosis: schema drift, naming the column that changed. The interesting part for
the agent is that the visible error names ``customer_id``, but the actual change is a
rename, so the answer is not "add the column back" but "the upstream contract changed".
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from dag_doctor.messaging.airflow_callback import on_task_failure

# retries=0 on purpose. This failure is deterministic, so retrying it only delays the
# diagnosis. The transient scenario in connection_timeout_api is where retries belong.
DEFAULT_ARGS = {
    "owner": "dag-doctor",
    "retries": 0,
    "on_failure_callback": on_task_failure,
}

# Inline rather than in a template file so that get_dag_source shows the agent the actual
# defective query, which is what it needs to name the column.
BUILD_ORDERS_BY_CUSTOMER = """
DROP TABLE IF EXISTS analytics.orders_by_customer;

CREATE TABLE analytics.orders_by_customer AS
SELECT
    customer_id,
    count(*)           AS order_count,
    sum(amount_cents)  AS revenue_cents
FROM raw.orders
GROUP BY customer_id;
"""

with DAG(
    dag_id="schema_drift_orders",
    description="Upstream renames a column; the downstream aggregate still selects it.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    template_searchpath=["/opt/airflow/seed"],
    tags=["dag-doctor", "seeded-failure", "schema-drift"],
) as dag:
    land_raw_orders = SQLExecuteQueryOperator(
        task_id="land_raw_orders",
        conn_id="warehouse",
        sql="02_schema_drift.sql",
    )

    build_orders_by_customer = SQLExecuteQueryOperator(
        task_id="build_orders_by_customer",
        conn_id="warehouse",
        sql=BUILD_ORDERS_BY_CUSTOMER,
        split_statements=True,
    )

    land_raw_orders >> build_orders_by_customer
