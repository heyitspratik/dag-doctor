# dag-doctor

An autonomous agent that diagnoses failing Airflow data pipelines: it consumes task
failure events, investigates them with a set of read-only tools, tests its own
hypotheses, and produces a structured diagnosis with an evidence chain and a computed
confidence score.

> **Status: under construction.** The repository is being built in phases. This README
> is replaced in the final phase with a worked example, the measured accuracy table
> across all eight seeded failure scenarios, and the architecture diagrams. Nothing here
> claims a result that has not been measured.

## What it does

When an Airflow task fails, an `on_failure_callback` publishes the event to a Redpanda
topic. A worker consumes it and runs a LangGraph state machine:

```
triage -> gather_evidence -> form_hypothesis -> test_hypothesis -> conclude
                  ^                                    |
                  +------------------------------------+
                      hypothesis refuted, budget remains
```

That back edge is the reason this is a graph rather than a chain: the investigation
loops until a hypothesis survives its test or a budget is exhausted.

## Quickstart

The stack runs end to end: a failure becomes an incident, the graph investigates it, and
the diagnosis and its full trace are readable over HTTP. All eight scenarios are seeded,
`make evaluate` scores them, and a Helm chart deploys the API and worker separately. No
accuracy table is published yet, because no run has been performed. That is the remaining
work.

```bash
make dev                              # sync dependencies, install the pre-commit hooks
make check                            # lint, type-check, unit tests
make up                               # postgres, redpanda, ollama, airflow, the worker
make seed-failures                    # trigger every seeded DAG
make topic-tail                       # see the failure events on airflow.task.failed
make logs                             # watch the worker diagnose them
curl localhost:8000/api/v1/incidents  # the incidents, newest first
```

Then follow one incident to the endpoint worth looking at:

```bash
curl "localhost:8000/api/v1/incidents/<id>/investigation"
```

It returns everything the agent did: what it looked at, what it believed, what it tried to
disprove, and what it concluded. OpenAPI docs are at http://localhost:8000/docs.

Airflow is at http://localhost:8080 (`airflow` / `airflow`). `schema_drift_orders` fails
by design and `healthy_baseline` succeeds; only the first should put a message on the
topic.

Every image tag in `docker/docker-compose.yml` is an overridable variable
(`AIRFLOW_IMAGE_TAG`, `POSTGRES_IMAGE`, `REDPANDA_IMAGE`, `OLLAMA_IMAGE`), because a stale
pin is the most common reason a cloned repository will not start.

### The seeded failures

Eight scenarios, seeded so the repository demonstrates itself without you providing any
data. They double as the evaluation set, and the answer key is a single readable table in
[`evaluation/scenarios.py`](src/dag_doctor/evaluation/scenarios.py).

| DAG | Failure | Correct diagnosis |
|---|---|---|
| `schema_drift_orders` | An upstream rename breaks a downstream aggregate | Schema drift, naming the changed column |
| `null_explosion_customers` | A source column goes mostly null; a not-null constraint fails downstream | Data quality regression upstream, not a code bug |
| `upstream_dependency_failure` | The real failure is two tasks upstream | Attribute it to the true upstream task, not the symptom |
| `connection_timeout_api` | An external API times out | Transient infrastructure; retry policy, not a code change |
| `bad_sql_join_explosion` | An accidental cartesian join runs away | Query defect; identify the join condition |
| `stale_partition_missing` | The expected date partition never landed | Missing upstream data, not a pipeline defect |
| `type_coercion_failure` | A string arrives where a numeric is expected | Type mismatch, naming the column |
| `healthy_baseline` | None. The control | The agent must never be invoked |

```bash
make evaluate    # triggers all eight, waits for diagnoses, prints the accuracy table
```

The harness reports **category accuracy** and **attribution accuracy** separately, because
naming the right kind of failure while blaming the wrong task is not a correct diagnosis.
An inconclusive answer scores as wrong. It also reports a calibration gap: mean confidence
on wrong answers minus mean confidence on right ones, so an agent that is surest when it
is wrong cannot hide behind a good average.

## Design notes

- **Redpanda instead of Kafka.** Kafka protocol compatible, one container rather than
  Kafka plus a coordination layer. Same client library, same topics, same consumer
  groups. The substitution is deliberate, not an oversight.
- **Ollama is the default provider.** The quickstart costs nothing and needs no signup.
  Anthropic and OpenAI are supported through the same factory.
- **Confidence is computed, not claimed.** It comes from the shape of the investigation:
  whether a known signature matched, whether a hypothesis survived a real test, how many
  independent tools support it, and how many hypotheses were discarded first. See
  [docs/confidence.md](docs/confidence.md).
- **Read-only by design.** The agent proposes fixes; it never applies them. Tools take a
  connection *name*, never a DSN, table and column names are validated rather than
  interpolated, and every query runs in a read-only transaction with a statement timeout.
  The agent writes only to its own database, which is its memory.

## Kubernetes

```bash
make kind-deploy    # kind cluster, dependencies, image, chart
make helm-lint      # lint and render against all three values files
```

The chart is at [`deploy/helm/dag-doctor/`](deploy/helm/dag-doctor/), with separate
Deployments for the API and the worker so they scale on the things that actually load
them: requests for one, consumer lag for the other. Its
[README](deploy/helm/dag-doctor/README.md) covers the three decisions worth knowing about,
including why the worker deliberately has no `preStop` hook and what the lag-based
autoscaler needs before it will do anything.

## Licence

MIT. See [LICENSE](LICENSE).
