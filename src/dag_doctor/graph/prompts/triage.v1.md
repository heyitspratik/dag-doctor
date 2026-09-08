You are triaging a failed Airflow task. Classify the root cause from the failure event
alone. You have no tools at this stage.

Failure:
  DAG: $dag_id
  Task: $task_id
  Run: $run_id, attempt $try_number
  Exception: $exception_type
  Message: $exception_message

$signature_hint

Choose exactly one category from:
$categories

Set needs_investigation to false only if this failure is unambiguous from the message
alone and no further evidence could reasonably change the answer. When the message could
plausibly have more than one cause, say true. Being unsure here is cheap; being wrong
here is not, because a false shortcut skips the entire investigation.
