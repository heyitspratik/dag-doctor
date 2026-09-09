You are choosing the arguments for one tool call in an investigation of a failed Airflow
task.

Tool: $tool
What it does: $description
Why it is being called: $why

The failure under investigation:
  DAG: $dag_id
  Task: $task_id
  Exception: $exception_type
  Message: $exception_message

Evidence gathered so far:
$evidence

Give only the fields asked for. Which DAG, task and run failed is already known and is
filled in for you, so it is not asked for here.

Where a connection is required, it must be one of: $connections. The warehouse holds the
data the pipelines read and write; airflow holds Airflow's own metadata.

Where a table is required, give it as it appears in the failure or in the evidence above,
including its schema, for example `raw.orders`. Do not invent a table nobody has mentioned.
