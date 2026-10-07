.DEFAULT_GOAL := help
COMPOSE := docker compose
API := http://localhost:8000

.PHONY: help up down logs demo observability test test-unit e2e coverage lint fmt load dlq dlq-replay

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the stack, wait until the API is ready
	$(COMPOSE) up -d --build
	@printf "waiting for $(API)/ready"; until curl -sf $(API)/ready >/dev/null; do printf "."; sleep 1; done; echo " ready"

down: ## Stop everything and delete volumes
	$(COMPOSE) --profile observability down -v

logs: ## Follow service logs
	$(COMPOSE) logs -f api outbox-relay consumer webhook-sink

demo: up ## Guided tour: idempotency, signed webhooks, retries, DLQ
	uv run python scripts/demo.py

observability: ## Start the stack with Prometheus, Grafana and Jaeger
	OTEL_ENABLED=true $(COMPOSE) --profile observability up -d --build
	@echo "Grafana http://localhost:3000  Jaeger http://localhost:16686  Prometheus http://localhost:9090"

test: ## Unit and integration tests (Docker required)
	uv run pytest

test-unit: ## Unit tests only, no Docker
	uv run pytest tests/unit

e2e: up ## End-to-end tests against the running stack
	uv run pytest -m e2e tests/e2e -v

coverage: ## Tests with the 90% coverage gate
	uv run pytest --cov=paystream --cov-branch --cov-report=term-missing --cov-fail-under=90

lint: ## ruff, formatting check and mypy
	uv run ruff check . && uv run ruff format --check . && uv run mypy src

fmt: ## Auto-fix lint and format
	uv run ruff check --fix . && uv run ruff format .

load: up ## Load test: 500 payments, 50 concurrent
	uv run python scripts/loadtest.py --payments 500 --concurrency 50

dlq: ## List dead letters
	$(COMPOSE) exec consumer paystream dlq list

dlq-replay: ## Replay every dead letter
	$(COMPOSE) exec consumer paystream dlq replay --all
