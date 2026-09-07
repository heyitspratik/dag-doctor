UV      ?= uv
COMPOSE ?= docker compose -f docker/docker-compose.yml
CHART   ?= deploy/helm/dag-doctor
KIND_CLUSTER ?= dag-doctor
AIRFLOW_URL  ?= http://localhost:8080
AIRFLOW_AUTH ?= airflow:airflow

.DEFAULT_GOAL := help
.PHONY: help install dev up down logs migrate pull-models trigger seed-failures \
        topic-tail evaluate test test-integration lint format typecheck check \
        helm-lint kind-deploy clean

help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install:  ## Sync the runtime environment only
	$(UV) sync --frozen --no-dev

dev:  ## Sync every dependency group and install the pre-commit hooks
	$(UV) sync --all-groups
	$(UV) run pre-commit install
	@test -f .env || cp .env.example .env

up:  ## Start the full stack (airflow, redpanda, postgres, agent, ollama)
	$(COMPOSE) up -d --wait

down:  ## Stop the stack and drop its volumes
	$(COMPOSE) down -v

logs:  ## Follow the agent worker and API logs
	$(COMPOSE) logs -f agent-worker agent-api

migrate:  ## Apply database migrations
	$(UV) run alembic upgrade head

pull-models:  ## Pull the default Ollama model (several GB on first run)
	$(COMPOSE) exec ollama ollama pull $${OLLAMA_MODEL:-llama3.2:3b}

trigger:  ## Trigger one DAG: make trigger DAG=schema_drift_orders
	@test -n "$(DAG)" || { echo "usage: make trigger DAG=<dag_id>"; exit 2; }
	@curl -sS -u $(AIRFLOW_AUTH) -X POST -H 'Content-Type: application/json' \
		-d '{"conf": {}}' $(AIRFLOW_URL)/api/v1/dags/$(DAG)/dagRuns \
		| head -c 400
	@echo

seed-failures:  ## Trigger every seeded DAG so the agent has incidents to diagnose
	@for dag_file in airflow/dags/*.py; do \
		dag_id=$$(basename $$dag_file .py); \
		echo "triggering $$dag_id"; \
		$(MAKE) --no-print-directory trigger DAG=$$dag_id; \
	done

topic-tail:  ## Print what is currently on the failure topic
	$(COMPOSE) exec redpanda rpk topic consume airflow.task.failed \
		--brokers localhost:9092 --num 10 --offset start

evaluate:  ## Trigger the scenarios, wait for diagnoses, print the accuracy table
	$(UV) run python -m dag_doctor.evaluation.runner evaluate

test:  ## Run unit tests with the coverage gate
	$(UV) run pytest --cov --cov-report=term-missing --cov-report=xml

test-integration:  ## Run integration tests (needs Docker)
	$(UV) run pytest -m integration -p no:cacheprovider

lint:  ## Check formatting and lint rules
	$(UV) run ruff format --check .
	$(UV) run ruff check .

format:  ## Apply formatting and autofixes
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

typecheck:  ## Type-check the source tree
	$(UV) run mypy

check: lint typecheck test  ## The full quality gate

helm-lint:  ## Lint and render the Helm chart
	helm lint $(CHART)
	helm template dag-doctor $(CHART) >/dev/null

kind-deploy:  ## Create a kind cluster and deploy the chart into it
	kind create cluster --name $(KIND_CLUSTER) || true
	helm upgrade --install dag-doctor $(CHART) -f $(CHART)/values-local.yaml --wait

clean:  ## Remove caches and build artefacts
	rm -rf .mypy_cache .pytest_cache .ruff_cache htmlcov .coverage coverage.xml dist build
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
