# Paystream

Reliable asynchronous payment processing: FastAPI + FastStream (RabbitMQ) + PostgreSQL transactional outbox.

## Run

```bash
docker compose up -d --build
```

## Try it

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' -H 'Idempotency-Key: order-1' -H 'Content-Type: application/json' \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Order #1"}'

curl -s http://localhost:8000/api/v1/payments/<payment_id> -H 'X-API-Key: dev-api-key'
```

Interactive API docs: http://localhost:8000/docs · RabbitMQ UI: http://localhost:15672 (paystream / paystream).

## Develop

```bash
uv sync
uv run pytest          # needs Docker for tests/integration
uv run ruff check . && uv run mypy src
```

Dead letters: `docker compose exec consumer paystream dlq list` / `paystream dlq replay --all`.
