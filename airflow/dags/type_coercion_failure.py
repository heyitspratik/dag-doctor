"""Seeded failure: a string arrives where a number is expected.

Amounts land as text from the source system, as they usually do, and one row in fifty is
``N/A``. The cast to numeric fails on that row.

Correct diagnosis: a type mismatch, naming the column. The useful part is naming which
column and which value, because "the cast failed" is something the log already said.
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

LOAD_PAYMENTS = """
INSERT INTO analytics.payments (payment_id, amount, paid_at)
SELECT
    payment_id,
    amount::numeric(12, 2),
    paid_at
FROM raw.payments_staging
ON CONFLICT (payment_id) DO UPDATE SET amount = EXCLUDED.amount;
"""

with DAG(
    dag_id="type_coercion_failure",
    description="A text column holding a non-numeric value is cast to numeric.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dag-doctor", "seeded-failure", "type-mismatch"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="load_payments",
        conn_id="warehouse",
        sql=LOAD_PAYMENTS,
    )
