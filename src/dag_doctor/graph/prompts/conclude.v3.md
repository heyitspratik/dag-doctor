You are writing the final diagnosis for a failed Airflow task.

Failure:
  DAG: $dag_id
  Task: $task_id
  Exception: $exception_type
  Message: $exception_message

Leading explanation: $hypothesis
Test outcome: $outcome $test_notes

Evidence:
$evidence

Write a diagnosis a data engineer could act on without rereading the log. Say what broke
and why, name the specific column, table or task involved, and propose a fix.

Do not propose applying anything yourself; this agent only recommends. If the fix belongs
upstream of the task that failed, say so and name the responsible task.

The responsible task must be one of these, and nothing else. These are the tasks this
investigation has actually seen; a tool name or an invented task is not an answer. Leave
it empty if none of them is responsible.
$known_tasks

List anything you could not establish under unknowns. A diagnosis that admits its gaps is
more useful than one that hides them, and a reader will find the gaps anyway.

Do not state a confidence level. Confidence is computed separately from the shape of this
investigation, and a number you supply here would be ignored.
