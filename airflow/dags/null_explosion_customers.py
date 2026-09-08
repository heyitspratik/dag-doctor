"""Seeded failure: a source column goes mostly null and breaks a downstream constraint.

The upstream load starts writing nulls into ``raw.customers.country_code``. Nothing
complains there, because the source table allows nulls. The dimension build downstream
does insist, so that is where the failure surfaces.

Correct diagnosis: a data quality regression upstream, not a code defect in the task that
failed. The code here has not changed and is not wrong; the data changed underneath it.
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

# The regression itself. A source system stopped populating a field, which is far more
# common than a source system breaking outright.
DROP_COUNTRY_CODES = """
UPDATE raw.customers
SET country_code = NULL
WHERE (substr(md5(customer_id), 1, 1) < 'e');
"""

BUILD_CUSTOMER_DIM = """
INSERT INTO analytics.customer_dim (customer_id, country_code)
SELECT customer_id, country_code
FROM raw.customers
ON CONFLICT (customer_id) DO UPDATE SET country_code = EXCLUDED.country_code;
"""

with DAG(
    dag_id="null_explosion_customers",
    description="An upstream column goes mostly null; a not-null constraint fails downstream.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dag-doctor", "seeded-failure", "data-quality"],
) as dag:
    land_raw_customers = SQLExecuteQueryOperator(
        task_id="land_raw_customers",
        conn_id="warehouse",
        sql=DROP_COUNTRY_CODES,
    )

    build_customer_dim = SQLExecuteQueryOperator(
        task_id="build_customer_dim",
        conn_id="warehouse",
        sql=BUILD_CUSTOMER_DIM,
    )

    land_raw_customers >> build_customer_dim
