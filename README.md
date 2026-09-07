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

Not yet runnable end to end. Once the stack lands:

```bash
make dev        # sync dependencies and install the pre-commit hooks
make check      # lint, type-check, unit tests
```

## Design notes

- **Redpanda instead of Kafka.** Kafka protocol compatible, one container rather than
  Kafka plus a coordination layer. Same client library, same topics, same consumer
  groups. The substitution is deliberate, not an oversight.
- **Ollama is the default provider.** The quickstart costs nothing and needs no signup.
  Anthropic and OpenAI are supported through the same factory.
- **Read-only by design.** The agent proposes fixes; it never applies them.

## Licence

MIT. See [LICENSE](LICENSE).
