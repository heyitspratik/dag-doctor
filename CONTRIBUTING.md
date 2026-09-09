# Contributing

## Getting set up

```bash
make dev      # sync every dependency group and install the pre-commit hooks
make check    # the gate: ruff format, ruff check, mypy strict, tests with coverage
```

`make check` is what CI runs. If it passes locally it will pass there.

## What the tooling expects

- **uv** for dependencies. The lockfile is committed; never pip, never `requirements.txt`.
- **ruff** for lint and format, line length 100.
- **mypy strict** over `src/`. No `Any` in public signatures, no blanket `# type: ignore`.
- **Python 3.12** syntax: `X | None`, `list[str]`, `pathlib`.
- **Google-style docstrings.** Comments explain *why*, in one or two lines. If a comment
  restates the code, delete it.
- **structlog**, never `print()`. Every line inside an investigation carries its
  `incident_id`.
- **Custom exceptions** from the `DagDoctorError` hierarchy.

## Testing

Three rules, and the first two are absolute:

1. **No test may need an API key, a running Ollama, or a network call.** The graph is
   driven by a scripted model; tools are driven against fixture databases.
2. **Every graph node must be testable with a scripted model.** A node that cannot be is
   badly designed. Raise it rather than working around it.
3. Integration tests, marked `integration`, may use Docker via testcontainers. They are
   excluded from the default run: `make test-integration`.

Coverage is gated at 85%. That is a floor for catching untested modules, not a target to
game.

## Where the interesting decisions are

Before changing behaviour, the reasoning is written down:

- [Architecture](docs/architecture.md), including why there is an event log at all
- [The investigation graph](docs/graph-design.md), including why the loop exists
- [How confidence is computed](docs/confidence.md), and why it is not the model's number
- [Adding a tool](docs/adding-tools.md)

## Commits

Conventional commits. Release notes are generated from them, so they are project output
rather than private notes.

```
feat(graph): add the investigation state machine with budgets
```

Scopes: `core`, `graph`, `tools`, `messaging`, `db`, `api`, `eval`, `deploy`, `ci`.

## Adding a failure scenario

The seeded DAGs double as the evaluation set, so a new one needs three things: the DAG
under `airflow/dags/`, an entry in
[`evaluation/scenarios.py`](src/dag_doctor/evaluation/scenarios.py) saying what the right
answer is, and the fixture data it needs in `airflow/seed/`. Tests assert that the DAGs and
the answer key cover exactly the same set, so a scenario cannot be added without somebody
deciding what a correct diagnosis of it looks like.
