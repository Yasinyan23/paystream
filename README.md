<div align="center">

<img src="docs/assets/banner.svg" alt="paystream: API → outbox → RabbitMQ → consumer → signed webhook" width="100%">

[![CI](https://github.com/Yasinyan23/paystream/actions/workflows/ci.yml/badge.svg)](https://github.com/Yasinyan23/paystream/actions/workflows/ci.yml)
![Python 3.14](https://img.shields.io/badge/python-3.14-3776AB?logo=python&logoColor=white)
![Coverage gate 90%](https://img.shields.io/badge/coverage%20gate-90%25-brightgreen)
![mypy strict](https://img.shields.io/badge/mypy-strict-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

**FastAPI · FastStream (RabbitMQ 4) · PostgreSQL 18 · SQLAlchemy 2 · Alembic · OpenTelemetry · Prometheus · Grafana**

[Quick start](#quick-start) · [Guarantees](#guarantees) · [Architecture](#architecture) · [API](#api) ·
[What happens when…](#what-happens-when) · [Observability](#observability) · [Design decisions](docs/adr/README.md) ·
[Русская версия](README.ru.md)

</div>

Paystream accepts payments over HTTP, processes them in the background through an emulated payment gateway and
notifies clients with **signed webhooks**. It started as a test assignment ([the brief](docs/task.md), in Russian) and
is built as a compact reference for the guarantees a payment backend needs: no duplicate payments, no lost events, no
silent failures. Every guarantee below is backed by an automated test.

## Guarantees

| | Guarantee | How |
|---|---|---|
| 🧾 | **No duplicate payments** | `Idempotency-Key` is arbitrated by a unique index (`INSERT … ON CONFLICT DO NOTHING`), so concurrent retries cannot race. A replay returns the payment with `Idempotent-Replayed: true`; the same key with a different body gets `422` ([ADR 0002](docs/adr/0002-idempotency-key.md)) |
| 📬 | **No lost events** | The payment, its outbox row and `pg_notify` commit in one transaction. The relay publishes with publisher confirms and `mandatory` routing, and backs off instead of giving up ([ADR 0001](docs/adr/0001-transactional-outbox.md)) |
| 🔁 | **One outcome per payment** | The consumer is an idempotent state machine: the status changes only through `UPDATE … WHERE status = 'pending'`, a finished payment never goes to the gateway again, and a redelivery only re-sends the webhook ([ADR 0003](docs/adr/0003-at-least-once-idempotent-consumer.md)) |
| 🪦 | **No silent failures** | 3 attempts with exponential delay (2 s → 4 s) through TTL queues, then a dead-letter queue with the reason in `x-dead-reason`, and a replay CLI ([ADR 0004](docs/adr/0004-exponential-retry-ttl-queues.md)) |
| 🛟 | **Outages are waited out** | PostgreSQL down: the API answers `503` with `Retry-After`, and the consumer parks events without spending their attempts. RabbitMQ down: payments are still accepted and the outbox drains when the broker returns |
| ✍️ | **Verifiable webhooks** | HMAC-SHA256 per [Standard Webhooks](https://www.standardwebhooks.com), a stable `webhook-id` for deduplication, an SSRF guard, no redirects ([ADR 0005](docs/adr/0005-webhook-signing-and-ssrf.md)) |
| 🔭 | **Observable** | JSON logs with correlation ids, Prometheus metrics with a Grafana dashboard, and one OpenTelemetry trace from the API request to the webhook |

The emulated gateway can still be asked twice about the same payment: when the consumer dies between the charge request
and the status commit, or when two copies of one message are processed at the same moment. The stored outcome stays
single either way; a real gateway closes that window with its own idempotency key, the payment id
([ADR 0003](docs/adr/0003-at-least-once-idempotent-consumer.md)).

## Quick start

Requirements: Docker with Compose v2, `make` and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/Yasinyan23/paystream && cd paystream
make up     # build and start postgres, rabbitmq, migrate, api, outbox-relay, consumer, webhook-sink
make demo   # a guided tour of the five scenarios below
```

The API is then on http://localhost:8000, with interactive docs at http://localhost:8000/docs (key: `dev-api-key`).
Without `make` and uv, `docker compose up -d --build --wait` starts the same stack.

`make demo` walks through a payment with a signed webhook, an idempotent replay, a reused key, a flaky receiver that
succeeds on retry, and a receiver that is down until the message lands in the dead-letter queue:

```text
▶ 1. Create a payment: 202 Accepted, processed asynchronously
  ✔ 202 pending  payment_id=01a11960-35df-764b-aaf0-c4a7ec738e08
  ✔ gateway result: failed (insufficient_funds), processed_at=2026-10-08T02:38:20.555068Z
  ✔ signed webhook verified by the receiver: payment.failed id=evt_963e7c42614751ae85fb09d9c4850228
▶ 2. Retry the same request: idempotent replay
  ✔ 202, same payment_id=True, Idempotent-Replayed: true
▶ 3. Same key, different body: rejected
  ✔ 422 urn:paystream:problem:idempotency-key-reused
▶ 4. Flaky receiver: first delivery 503, retried after 2 s, then delivered
  ✔ delivered after 2 attempts (same webhook-id on every attempt)
▶ 5. Receiver down: 3 attempts (2 s, 4 s backoff), then the dead-letter queue
  ✔ payment status kept: succeeded; webhook state=failing, last error: HTTP 503:
  ✔ payments.dead holds 2 message(s)  →  inspect: make dlq, replay: make dlq-replay
```

`make observability` rebuilds the stack with tracing on and adds Prometheus (http://localhost:9090), Grafana
(http://localhost:3000) and Jaeger (http://localhost:16686). `make demo`, `make e2e` and `make load` never rebuild or
recreate a running stack, so run `make up` (or `make observability`) again after changing `src/`. `make down` stops
everything and deletes the volumes.

## See it running

<img src="docs/assets/grafana.png" alt="Grafana dashboard: payments, outbox, webhooks, retries, latency, queue depth" width="100%">

<img src="docs/assets/jaeger.png" alt="Jaeger: one trace from POST /api/v1/payments through the outbox relay and the consumer to the webhook" width="100%">

<img src="docs/assets/swagger.png" alt="Interactive API docs at /docs" width="100%">

## Architecture

```mermaid
flowchart LR
    C([Client]) -- "POST /api/v1/payments<br/>Idempotency-Key · X-API-Key" --> API[api<br/>FastAPI]
    API -- "one transaction:<br/>payment + outbox row + NOTIFY" --> PG[(PostgreSQL)]
    PG -- "LISTEN / NOTIFY" --> R[outbox-relay]
    R -- "publish, confirmed<br/>message_id = event id" --> X{{payments}}
    X -- payment.created --> Q[["payments.new<br/>quorum"]]
    Q --> W[consumer<br/>FastStream]
    W -- "pending → final<br/>conditional UPDATE" --> PG
    W -- "signed webhook" --> S([Client endpoint])
    W -- "transient error" --> RX{{payments.retry}}
    RX --> R1[["payments.retry.2s"]]
    RX --> R2[["payments.retry.4s"]]
    R1 -- "TTL expiry" --> X
    R2 -- "TTL expiry" --> X
    W -- "3 attempts failed<br/>or poison message" --> DX{{payments.dlx}}
    DX --> DLQ[["payments.dead"]]
```

| Service | Role | Scales |
|---|---|---|
| `api` | Accepts and serves payments; never talks to RabbitMQ | Stateless, any number of replicas |
| `outbox-relay` | Moves outbox rows to RabbitMQ, woken by `LISTEN/NOTIFY`, polling every second as a fallback | Replicas share work through `FOR UPDATE SKIP LOCKED` |
| `consumer` | The single handler: gateway call → status update → signed webhook | Replicas × `CONSUMER_PREFETCH` messages in flight |
| `migrate` | Runs `alembic upgrade head` once; every other service waits for it | — |
| `webhook-sink` | Demo receiver that verifies signatures and lists what it got | Dev and e2e only |

All ports are published on `127.0.0.1` only; PostgreSQL is not published at all.

## A payment, step by step

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as api
    participant DB as PostgreSQL
    participant R as outbox-relay
    participant MQ as RabbitMQ
    participant W as consumer
    participant H as Client webhook
    C->>A: POST /api/v1/payments (Idempotency-Key)
    A->>DB: INSERT payment + outbox row, NOTIFY (one transaction)
    A-->>C: 202 Accepted {payment_id, status: pending, created_at}
    DB-->>R: NOTIFY paystream_outbox
    R->>MQ: publish payment.created (publisher confirm)
    R->>DB: mark the outbox row published
    MQ->>W: deliver from payments.new
    W->>W: gateway call (2–5 s, 90 % approved)
    W->>DB: UPDATE payments SET status = … WHERE status = 'pending'
    W->>H: POST signed webhook
    H-->>W: 2xx
    W->>DB: webhook attempt + webhook_delivered_at (one transaction)
    W->>MQ: ack
```

## API

All `/api/v1/*` endpoints require `X-API-Key`. Every error is [RFC 9457 Problem Details](docs/problems.md)
(`application/problem+json`), and every response carries `X-Request-ID`.

### Create a payment

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Order #42",
       "metadata": {"order_id": 42}, "webhook_url": "http://webhook-sink:8080/webhooks/ok"}'
```

```http
HTTP/1.1 202 Accepted
content-type: application/json
location: /api/v1/payments/01a11a35-ecd7-748c-b608-609f0ade4af0
x-request-id: e340b5d9fb7d4ef69928b93581dad942

{"payment_id":"01a11a35-ecd7-748c-b608-609f0ade4af0","status":"pending","created_at":"2026-10-08T06:31:44.089630Z"}
```

- `Idempotency-Key` is required: 1–255 printable ASCII characters, no spaces. Without it the API answers `400 idempotency-key-missing`.
- Repeating the same request returns `202` with the payment's **current** state and `Idempotent-Replayed: true`.
- The same key with a different body returns `422 idempotency-key-reused`.
- `amount` is a decimal string, at least `0.01`, with at most two decimal places; `currency` is `RUB`, `USD` or `EUR`.
- `metadata` is any JSON object up to 16 KB (UTF-8) and 32 levels deep. NUL characters, lone surrogates, `NaN` and
  `Infinity` are rejected with `422` because PostgreSQL could not store them.

### Get a payment

```bash
curl -s http://localhost:8000/api/v1/payments/01a11a35-ecd7-748c-b608-609f0ade4af0 -H 'X-API-Key: dev-api-key'
```

```json
{
  "id": "01a11a35-ecd7-748c-b608-609f0ade4af0",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "failure_reason": null,
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-sink:8080/webhooks/ok",
  "webhook": {"state": "delivered", "attempts": 1, "last_status_code": 204, "last_error": null,
              "delivered_at": "2026-10-08T06:31:48.356308Z"},
  "created_at": "2026-10-08T06:31:44.089630Z",
  "processed_at": "2026-10-08T06:31:48.343615Z"
}
```

`status` is `pending`, `succeeded` or `failed`; a failed payment carries `failure_reason` (`card_declined`,
`insufficient_funds` or `do_not_honor`). `webhook.state` is `not_configured`, `pending`, `delivered` or `failing`.

Operational endpoints need no key: `GET /health` (the process is up), `GET /ready` (the database answers; by design
the broker is not checked, because payments are accepted while RabbitMQ is down) and `GET /metrics`
(`METRICS_REQUIRE_API_KEY=true` protects it).

## Webhooks

When a payment reaches its final status, the consumer POSTs a signed event to the payment's `webhook_url`:

```http
POST /webhooks/ok HTTP/1.1
content-type: application/json
user-agent: paystream-webhooks/0.1.0
webhook-id: evt_08caa279825855708ab7d994c171153f
webhook-timestamp: 1791441108
webhook-signature: v1,eLI9tmGcELKX52m8IvjrB+gl1pbk/LFKv7TOdnXBuyA=

{"type":"payment.succeeded","timestamp":"2026-10-08T06:31:48.343615+00:00","data":{"id":"01a11a35-ecd7-748c-b608-609f0ade4af0","amount":"1500.00","currency":"RUB","status":"succeeded","failure_reason":null,"description":"Order #42","metadata":{"order_id":42},"created_at":"2026-10-08T06:31:44.089630+00:00","processed_at":"2026-10-08T06:31:48.343615+00:00"}}
```

Verify it with any [Standard Webhooks](https://www.standardwebhooks.com) library, or in plain Python:

```python
import base64, hashlib, hmac, time

def verify(secret: str, headers: dict[str, str], body: bytes, tolerance: int = 300) -> bool:
    msg_id, ts = headers["webhook-id"], headers["webhook-timestamp"]
    if abs(time.time() - int(ts)) > tolerance:
        return False
    key = base64.b64decode(secret.removeprefix("whsec_"))
    expected = base64.b64encode(hmac.new(key, f"{msg_id}.{ts}.".encode() + body, hashlib.sha256).digest()).decode()
    return any(
        version == "v1" and hmac.compare_digest(sig, expected)
        for version, _, sig in (part.partition(",") for part in headers["webhook-signature"].split())
    )
```

- Any `2xx` counts as delivered. Anything else, or no answer within the connect (3 s) and read (5 s) timeouts, is
  retried after 2 s and 4 s; then the message goes to the dead-letter queue and the payment keeps its status.
- `webhook-id` is the same on every retry of an event: deduplicate on it.
- Before sending, the URL's host is resolved and every address must be public (`is_global`), unless the host is listed
  in `WEBHOOK_ALLOWED_HOSTS`. Redirects are not followed, proxy environment variables are ignored, and at most 1 KiB of
  an error response is read and stored. See [SECURITY.md](SECURITY.md) for what this guard does not cover.

## What happens when…

| Situation | Behaviour |
|---|---|
| The client retries after a timeout | Same `Idempotency-Key` → `202` with the same payment and `Idempotent-Replayed: true` |
| Ten identical requests arrive at once | One payment, one event: the unique index decides |
| A key is reused for another payment | `422 idempotency-key-reused` |
| RabbitMQ is down | Payments are still accepted; the outbox queues up and drains when the broker returns |
| PostgreSQL is down | The API answers `503` with `Retry-After`; the consumer parks events without spending attempts and finishes them when the database is back |
| The relay crashes after publishing | The event is published again; the consumer ignores the duplicate |
| The consumer crashes mid-payment | The message is redelivered; a payment that already has its final status is not sent to the gateway again |
| The client's endpoint returns 5xx | Retried after 2 s and 4 s, then dead-lettered as `max_attempts_exceeded`; the payment keeps its status |
| The webhook URL points to a private address | Refused before sending and dead-lettered as `webhook_rejected` |
| A malformed message reaches the queue | Dead-lettered immediately as `invalid_message` or `invalid_event` |
| A message keeps crashing the consumer | The quorum queue's delivery limit (10) dead-letters it |

Each row is covered by an automated test in `tests/`.

## RabbitMQ topology

```mermaid
flowchart LR
    X{{"payments (direct)"}} -- payment.created --> N[["payments.new<br/>quorum, delivery limit 10"]]
    RX{{"payments.retry (direct)"}} -- retry.2000 --> T2[["payments.retry.2s<br/>TTL 2 s"]]
    RX -- retry.4000 --> T4[["payments.retry.4s<br/>TTL 4 s"]]
    T2 -- "dead-letter on expiry" --> X
    T4 -- "dead-letter on expiry" --> X
    N -- "delivery limit exceeded" --> D{{"payments.dlx (direct)"}}
    D -- payment.dead --> DLQ[["payments.dead<br/>classic"]]
```

The consumer itself publishes to `payments.retry` (with `x-attempt` incremented) or straight to `payments.dlx` (with
`x-dead-reason`, `x-error` and `x-failed-at`). Inspect and replay dead letters:

```bash
make dlq          # docker compose exec consumer paystream dlq list
make dlq-replay   # docker compose exec consumer paystream dlq replay --all
```

`dlq replay --all` handles each message at most once per run, so a message that fails again immediately does not loop.
Replay one message with `paystream dlq replay --message-id <id>`.

## Observability

- **Logs:** one JSON object per line with `service`; API lines carry `request_id`, consumer lines carry `message_id`,
  `correlation_id` (the API's request id), `payment_id` and `attempt`. `LOG_JSON=false` gives coloured console output.
- **Metrics:** Prometheus text at `/metrics` on the API and on port 9100 of the relay and consumer:
  `paystream_payments_created_total`, `paystream_idempotent_replays_total`, `paystream_payments_processed_total{status}`,
  `paystream_gateway_seconds`, `paystream_webhook_deliveries_total{result}`, `paystream_webhook_seconds`,
  `paystream_outbox_pending_events`, `paystream_outbox_publish_lag_seconds`, `paystream_messages_retried_total{delay_ms}`,
  `paystream_messages_dead_lettered_total{reason}`, `paystream_http_requests_total`, `paystream_http_request_seconds`.
  FastStream's own consumer metrics keep their `faststream_` prefix. The Grafana dashboard reads the dead-letter queue
  depth straight from RabbitMQ.
- **Traces:** opt-in with `OTEL_ENABLED=true` (on in `make observability`). The W3C `traceparent` travels in the outbox
  row and the message headers, so one trace runs from the API request through the relay and the consumer to the webhook
  POST. Health and metrics endpoints are kept out of traces.

## Configuration

Every setting is an environment variable; [`.env.example`](.env.example) lists the ones compose passes through.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://paystream:paystream@localhost:5432/paystream` | PostgreSQL DSN |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ URL |
| `API_KEYS` | *(required)* | Comma-separated keys; several at once allow rotation |
| `WEBHOOK_SIGNING_SECRET` | *(required by the consumer)* | `whsec_…` secret |
| `WEBHOOK_ALLOWED_HOSTS` | *(empty)* | Hosts exempt from the public-address check |
| `WEBHOOK_CONNECT_TIMEOUT` / `WEBHOOK_READ_TIMEOUT` | `3.0` / `5.0` | Webhook timeouts, seconds |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2.0` / `5.0` | Emulated gateway latency, seconds |
| `GATEWAY_SUCCESS_RATE` | `0.9` | Share of approved payments |
| `RETRY_MAX_ATTEMPTS` | `3` | Attempts before the dead-letter queue |
| `RETRY_BASE_DELAY_MS` | `2000` | First retry delay; doubles each time |
| `CONSUMER_PREFETCH` | `10` | Messages in flight per consumer |
| `DELIVERY_LIMIT` | `10` | Redeliveries of one message before RabbitMQ dead-letters it; every process must agree, and changing it means recreating `payments.new` |
| `OUTBOX_BATCH_SIZE` | `100` | Rows per relay pass |
| `OUTBOX_POLL_INTERVAL` | `1.0` | Fallback poll when no NOTIFY arrives, seconds |
| `OUTBOX_MAX_BACKOFF` | `60.0` | Cap of the relay's publish backoff, seconds |
| `OUTBOX_RETENTION_DAYS` | `7` | Published outbox rows are purged after this |
| `LOG_JSON` / `LOG_LEVEL` | `true` / `INFO` | Log format and level |
| `METRICS_PORT` | `9100` | Metrics port of the relay and the consumer |
| `METRICS_REQUIRE_API_KEY` | `false` | Protect the API's `/metrics` with `X-API-Key` |
| `OTEL_ENABLED` | `false` | Export traces |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` | OTLP/HTTP traces endpoint (the full URL) |

Generate a signing secret with
`uv run python -c "from paystream.webhooks.signing import generate_secret; print(generate_secret())"`.

## Development

```bash
uv sync
make test        # unit + integration tests; integration tests start PostgreSQL and RabbitMQ with testcontainers
make test-unit   # unit tests only, no Docker
make coverage    # the same suite with the 90 % branch-coverage gate
make lint        # ruff, ruff format --check, mypy --strict
make e2e         # end-to-end tests against the running compose stack
make load        # load test: 500 payments, 50 concurrent
```

CI runs lint, the coverage-gated suite and the end-to-end tests against `docker compose` on every push to `main` and
every pull request.

```
src/paystream/
├── api/            FastAPI app, routes, Problem Details, auth, request ids
├── core/           settings, logging, metrics, telemetry
├── db/             models and engine
├── devtools/       demo webhook receiver (webhook-sink)
├── domain/         statuses, event envelope, fingerprints, backoff
├── gateway/        emulated payment gateway
├── messaging/      topology, broker, publisher, retry policy
├── repositories/   SQL for payments, outbox, webhook attempts
├── services/       payment use cases, outbox relay, consumer state machine
├── webhooks/       signing, SSRF guard, sender
└── workers/        relay and consumer processes, DLQ CLI
```

## Performance

Measured with `scripts/loadtest.py` on one laptop (stack, Docker Desktop and load generator on the same Apple Silicon
Mac); full conditions, raw output and a concurrency sweep are in [docs/benchmarks.md](docs/benchmarks.md).

| Scenario | Accept p50 / p95 | End-to-end p50 / p95 | Throughput |
|---|---|---|---|
| Default gateway (2–5 s), 1 consumer, 500 payments, 50 concurrent | 70.0 ms / 197.7 ms | 90.02 s / 168.11 s | 2.8 payments/s |
| Fast gateway (50–100 ms), 3 consumers, 1000 payments, 100 concurrent | 211.5 ms / 2416.6 ms | 0.20 s / 0.63 s | 160.5 payments/s |

**Accept** is the client's time to the `202`; **end-to-end** is `processed_at − created_at` as the API reports it. In
the first scenario the end-to-end time is queue wait: one consumer with 10 messages in flight against a 2–5 s gateway
completes about 2.8 payments per second. In the second the pipeline keeps up; its throughput is the offered load, and
its accept p95 is bound by the macOS host's path into Docker Desktop rather than by the service.

## Security

The compose defaults (`API_KEYS=dev-api-key` and the public Standard Webhooks test secret) are for local use only:
replace both before running Paystream anywhere else. Put the API behind a reverse proxy that terminates TLS and limits
request body size. [SECURITY.md](SECURITY.md) covers the threat model, the limits of the SSRF guard and how to report a
vulnerability.

## Design decisions

The [architecture decision records](docs/adr/README.md) explain the transactional outbox, idempotency semantics, the
idempotent consumer, the retry tiers, webhook signing with the SSRF guard, and quorum queues.

## Roadmap

- Connect to the address that passed the SSRF check when delivering webhooks (closes the window described in
  [ADR 0005](docs/adr/0005-webhook-signing-and-ssrf.md))
- A real gateway adapter that passes the payment id as the gateway's idempotency key
- Listing payments with cursor pagination
- An optional CDC-based relay (Debezium) instead of polling the outbox

## License

MIT, see [LICENSE](LICENSE).
