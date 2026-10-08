.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up stack down logs demo observability test test-unit e2e coverage lint fmt load dlq dlq-replay

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Build and (re)create the stack, wait until it is running and healthy
	$(COMPOSE) up -d --build --wait

stack: ## Start the stack if it is down; a running stack is left as it is
	$(COMPOSE) up -d --wait --no-recreate

down: ## Stop everything and delete volumes
	$(COMPOSE) --profile observability down -v

logs: ## Follow service logs
	$(COMPOSE) logs -f api outbox-relay consumer webhook-sink

demo: stack ## Guided tour: idempotency, signed webhooks, retries, DLQ
	uv run python scripts/demo.py

observability: ## Build and (re)create the stack with tracing on, plus Prometheus, Grafana and Jaeger
	OTEL_ENABLED=true $(COMPOSE) --profile observability up -d --build
	@echo "Grafana http://localhost:3000  Jaeger http://localhost:16686  Prometheus http://localhost:9090"

test: ## Unit and integration tests (Docker required)
	uv run pytest

test-unit: ## Unit tests only, no Docker
	uv run pytest tests/unit

e2e: stack ## End-to-end tests against the running stack
	uv run pytest -m e2e tests/e2e -v

coverage: ## Tests with the 90% coverage gate
	uv run pytest --cov=paystream --cov-report=term-missing --cov-fail-under=90

lint: ## ruff, formatting check and mypy
	uv run ruff check . && uv run ruff format --check . && uv run mypy src

fmt: ## Auto-fix lint and format
	uv run ruff check --fix . && uv run ruff format .

load: stack ## Load test: 500 payments, 50 concurrent
	uv run python scripts/loadtest.py --payments 500 --concurrency 50

dlq: ## List dead letters
	$(COMPOSE) exec consumer paystream dlq list

dlq-replay: ## Replay every dead letter
	$(COMPOSE) exec consumer paystream dlq replay --all
