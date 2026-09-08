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

The graph is built and tested, but the worker does not run it yet, so the stack
currently ends at "a failure becomes an incident row".

```bash
make dev                              # sync dependencies, install the pre-commit hooks
make check                            # lint, type-check, unit tests
make up                               # postgres, redpanda, ollama, airflow, the worker
make seed-failures                    # trigger every seeded DAG
make topic-tail                       # see the failure events on airflow.task.failed
```

Airflow is at http://localhost:8080 (`airflow` / `airflow`). `schema_drift_orders` fails
by design and `healthy_baseline` succeeds; only the first should put a message on the
topic.

Every image tag in `docker/docker-compose.yml` is an overridable variable
(`AIRFLOW_IMAGE_TAG`, `POSTGRES_IMAGE`, `REDPANDA_IMAGE`, `OLLAMA_IMAGE`), because a stale
pin is the most common reason a cloned repository will not start.

### The seeded failures

| DAG | Failure | Correct diagnosis |
|---|---|---|
| `schema_drift_orders` | An upstream rename breaks a downstream aggregate | Schema drift, naming the changed column |
| `healthy_baseline` | None. The control | The agent must never be invoked |

Six more scenarios land with the evaluation harness.

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

## Licence

MIT. See [LICENSE](LICENSE).
