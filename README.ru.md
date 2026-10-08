<div align="center">

<img src="docs/assets/banner.svg" alt="paystream: API → outbox → RabbitMQ → consumer → подписанный webhook" width="100%">

[![CI](https://github.com/Yasinyan23/paystream/actions/workflows/ci.yml/badge.svg)](https://github.com/Yasinyan23/paystream/actions/workflows/ci.yml)
![Python 3.14](https://img.shields.io/badge/python-3.14-3776AB?logo=python&logoColor=white)
![Coverage gate 90%](https://img.shields.io/badge/coverage%20gate-90%25-brightgreen)
![mypy strict](https://img.shields.io/badge/mypy-strict-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

**FastAPI · FastStream (RabbitMQ 4) · PostgreSQL 18 · SQLAlchemy 2 · Alembic · OpenTelemetry · Prometheus · Grafana**

[Быстрый старт](#быстрый-старт) · [Гарантии](#гарантии) · [Архитектура](#архитектура) · [API](#api) ·
[Что будет, если…](#что-будет-если) · [Наблюдаемость](#наблюдаемость) · [Архитектурные решения](docs/adr/README.md) ·
[English version](README.md)

</div>

Paystream принимает платежи по HTTP, обрабатывает их в фоне через эмулятор платёжного шлюза и уведомляет клиентов
**подписанными webhook'ами**. Проект вырос из тестового задания ([исходное ТЗ](docs/task.md)) и собран как компактный
эталон гарантий, которые нужны платёжному бэкенду: никаких дублей платежей, потерянных событий и тихих отказов. Каждая
гарантия ниже подтверждена автоматическим тестом.

## Гарантии

| | Гарантия | Как |
|---|---|---|
| 🧾 | **Никаких дублей платежей** | `Idempotency-Key` разрешается уникальным индексом (`INSERT … ON CONFLICT DO NOTHING`), поэтому параллельные повторы не устраивают гонку. Повтор возвращает платёж с `Idempotent-Replayed: true`; тот же ключ с другим телом получает `422` ([ADR 0002](docs/adr/0002-idempotency-key.md)) |
| 📬 | **Никаких потерянных событий** | Платёж, его строка outbox и `pg_notify` фиксируются одной транзакцией. Relay публикует с publisher confirms и маршрутизацией `mandatory` и при ошибке откладывает попытку, а не сдаётся ([ADR 0001](docs/adr/0001-transactional-outbox.md)) |
| 🔁 | **Один исход на платёж** | Consumer — идемпотентный конечный автомат: статус меняется только через `UPDATE … WHERE status = 'pending'`, завершённый платёж больше не уходит в шлюз, а повторная доставка лишь заново отправляет webhook ([ADR 0003](docs/adr/0003-at-least-once-idempotent-consumer.md)) |
| 🪦 | **Никаких тихих отказов** | 3 попытки с экспоненциальной задержкой (2 с → 4 с) через очереди с TTL, затем dead-letter queue с причиной в `x-dead-reason` и CLI для повторной отправки ([ADR 0004](docs/adr/0004-exponential-retry-ttl-queues.md)) |
| 🛟 | **Сбои пережидаются** | PostgreSQL недоступен: API отвечает `503` с `Retry-After`, а consumer откладывает события, не расходуя их попытки. RabbitMQ недоступен: платежи по-прежнему принимаются, outbox выгружается, когда брокер вернётся |
| ✍️ | **Проверяемые webhook'и** | HMAC-SHA256 по [Standard Webhooks](https://www.standardwebhooks.com), стабильный `webhook-id` для дедупликации, защита от SSRF, без редиректов ([ADR 0005](docs/adr/0005-webhook-signing-and-ssrf.md)) |
| 🔭 | **Наблюдаемость** | JSON-логи с идентификаторами корреляции, метрики Prometheus с дашбордом Grafana и один трейс OpenTelemetry от запроса к API до webhook'а |

Эмулятор шлюза всё ещё может получить два запроса по одному платежу: если consumer упадёт между запросом списания и
фиксацией статуса или если две копии одного сообщения обрабатываются одновременно. Сохранённый исход в любом случае
один; настоящий шлюз закрывает это окно собственным ключом идемпотентности — идентификатором платежа
([ADR 0003](docs/adr/0003-at-least-once-idempotent-consumer.md)).

## Быстрый старт

Нужны Docker с Compose v2, `make` и [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/Yasinyan23/paystream && cd paystream
make up     # build and start postgres, rabbitmq, migrate, api, outbox-relay, consumer, webhook-sink
make demo   # a guided tour of the five scenarios below
```

После этого API доступен на http://localhost:8000, интерактивная документация — на http://localhost:8000/docs (ключ:
`dev-api-key`). Без `make` и uv тот же стек запускается командой `docker compose up -d --build --wait`.

`make demo` проходит пять сценариев: платёж с подписанным webhook'ом, идемпотентный повтор, повторно использованный
ключ, нестабильный получатель, который принимает webhook со второй попытки, и недоступный получатель, после которого
сообщение попадает в dead-letter queue:

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

`make observability` пересобирает стек с включённой трассировкой и добавляет Prometheus (http://localhost:9090), Grafana
(http://localhost:3000) и Jaeger (http://localhost:16686). `make demo`, `make e2e` и `make load` никогда не пересобирают
и не пересоздают запущенный стек, поэтому после изменений в `src/` снова выполните `make up` (или `make observability`).
`make down` останавливает всё и удаляет тома.

## Как это выглядит

<img src="docs/assets/grafana.png" alt="Дашборд Grafana: платежи, outbox, webhook'и, повторы, задержки, глубина очередей" width="100%">

<img src="docs/assets/jaeger.png" alt="Jaeger: один трейс от POST /api/v1/payments через outbox relay и consumer до webhook'а" width="100%">

<img src="docs/assets/swagger.png" alt="Интерактивная документация API на /docs" width="100%">

## Архитектура

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

| Сервис | Роль | Масштабирование |
|---|---|---|
| `api` | Принимает и отдаёт платежи; с RabbitMQ не общается | Без состояния, любое число реплик |
| `outbox-relay` | Переносит строки outbox в RabbitMQ; просыпается по `LISTEN/NOTIFY`, раз в секунду опрашивает таблицу на случай пропущенного уведомления | Реплики делят работу через `FOR UPDATE SKIP LOCKED` |
| `consumer` | Единственный обработчик: вызов шлюза → обновление статуса → подписанный webhook | Реплики × `CONSUMER_PREFETCH` сообщений в работе |
| `migrate` | Один раз выполняет `alembic upgrade head`; остальные сервисы ждут его | — |
| `webhook-sink` | Демонстрационный получатель: проверяет подписи и показывает, что получил | Только для разработки и e2e |

Все порты опубликованы только на `127.0.0.1`; PostgreSQL наружу не публикуется вовсе.

## Платёж по шагам

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

Все эндпоинты `/api/v1/*` требуют `X-API-Key`. Любая ошибка — [RFC 9457 Problem Details](docs/problems.md)
(`application/problem+json`), и каждый ответ содержит `X-Request-ID`.

### Создать платёж

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

- `Idempotency-Key` обязателен: от 1 до 255 печатных ASCII-символов без пробелов. Без него API отвечает
  `400 idempotency-key-missing`.
- Повтор того же запроса возвращает `202` с **текущим** состоянием платежа и `Idempotent-Replayed: true`.
- Тот же ключ с другим телом возвращает `422 idempotency-key-reused`.
- `amount` — десятичная строка, не меньше `0.01`, не больше двух знаков после запятой; `currency` — `RUB`, `USD`
  или `EUR`.
- `metadata` — любой JSON-объект до 16 КБ (UTF-8) и до 32 уровней вложенности. Символы NUL, одиночные суррогаты,
  `NaN` и `Infinity` отклоняются с `422`, потому что PostgreSQL не смог бы их сохранить.

### Получить платёж

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

`status` — `pending`, `succeeded` или `failed`; у неуспешного платежа есть `failure_reason` (`card_declined`,
`insufficient_funds` или `do_not_honor`). `webhook.state` — `not_configured`, `pending`, `delivered` или `failing`.

Служебные эндпоинты работают без ключа: `GET /health` (процесс жив), `GET /ready` (база отвечает; брокер намеренно не
проверяется, потому что платежи принимаются и при недоступном RabbitMQ) и `GET /metrics` (`METRICS_REQUIRE_API_KEY=true`
закрывает его ключом).

## Webhook'и

Когда платёж получает окончательный статус, consumer отправляет подписанное событие POST-запросом на `webhook_url`
платежа:

```http
POST /webhooks/ok HTTP/1.1
content-type: application/json
user-agent: paystream-webhooks/0.1.0
webhook-id: evt_08caa279825855708ab7d994c171153f
webhook-timestamp: 1791441108
webhook-signature: v1,eLI9tmGcELKX52m8IvjrB+gl1pbk/LFKv7TOdnXBuyA=

{"type":"payment.succeeded","timestamp":"2026-10-08T06:31:48.343615+00:00","data":{"id":"01a11a35-ecd7-748c-b608-609f0ade4af0","amount":"1500.00","currency":"RUB","status":"succeeded","failure_reason":null,"description":"Order #42","metadata":{"order_id":42},"created_at":"2026-10-08T06:31:44.089630+00:00","processed_at":"2026-10-08T06:31:48.343615+00:00"}}
```

Проверить подпись можно любой библиотекой [Standard Webhooks](https://www.standardwebhooks.com) или на чистом Python:

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

- Любой ответ `2xx` считается доставкой. Всё остальное, а также отсутствие ответа в пределах таймаутов подключения
  (3 с) и чтения (5 с), повторяется через 2 с и 4 с; затем сообщение уходит в dead-letter queue, а платёж сохраняет свой
  статус.
- `webhook-id` одинаков во всех повторах одного события: дедуплицируйте по нему.
- Перед отправкой хост из URL разрешается, и каждый его адрес должен быть публичным (`is_global`), если хост не указан в
  `WEBHOOK_ALLOWED_HOSTS`. Редиректы не выполняются, переменные окружения прокси игнорируются, из ответа с ошибкой
  читается и сохраняется не больше 1 КиБ. Чего эта защита не покрывает — в [SECURITY.md](SECURITY.md).

## Что будет, если…

| Ситуация | Поведение |
|---|---|
| Клиент повторяет запрос после таймаута | Тот же `Idempotency-Key` → `202` с тем же платежом и `Idempotent-Replayed: true` |
| Одновременно приходят десять одинаковых запросов | Один платёж, одно событие: решает уникальный индекс |
| Ключ используют для другого платежа | `422 idempotency-key-reused` |
| RabbitMQ недоступен | Платежи по-прежнему принимаются; outbox накапливается и выгружается, когда брокер вернётся |
| PostgreSQL недоступен | API отвечает `503` с `Retry-After`; consumer откладывает события, не расходуя попытки, и завершает их, когда база вернётся |
| Relay падает после публикации | Событие публикуется ещё раз; consumer игнорирует дубль |
| Consumer падает посреди платежа | Сообщение доставляется заново; платёж с окончательным статусом в шлюз повторно не уходит |
| Сервер клиента отвечает 5xx | Повтор через 2 с и 4 с, затем dead-letter queue с причиной `max_attempts_exceeded`; платёж сохраняет статус |
| URL webhook'а указывает на частный адрес | Отказ до отправки, dead-letter queue с причиной `webhook_rejected` |
| В очередь попадает некорректное сообщение | Сразу dead-letter queue с причиной `invalid_message` или `invalid_event` |
| Сообщение раз за разом роняет consumer | Лимит доставок quorum queue (10) отправляет его в dead-letter queue |

Каждая строка покрыта автоматическим тестом в `tests/`.

## Топология RabbitMQ

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

Consumer сам публикует в `payments.retry` (с увеличенным `x-attempt`) или напрямую в `payments.dlx` (с `x-dead-reason`,
`x-error` и `x-failed-at`). Просмотр и повторная отправка сообщений из dead-letter queue:

```bash
make dlq          # docker compose exec consumer paystream dlq list
make dlq-replay   # docker compose exec consumer paystream dlq replay --all
```

`dlq replay --all` обрабатывает каждое сообщение не больше одного раза за запуск, поэтому сообщение, которое сразу
падает снова, не зацикливается. Одно сообщение отправляется командой `paystream dlq replay --message-id <id>`.

## Наблюдаемость

- **Логи:** один JSON-объект на строку с полем `service`; строки API содержат `request_id`, строки consumer —
  `message_id`, `correlation_id` (идентификатор запроса к API), `payment_id` и `attempt`. `LOG_JSON=false` включает
  цветной консольный вывод.
- **Метрики:** формат Prometheus на `/metrics` у API и на порту 9100 у relay и consumer:
  `paystream_payments_created_total`, `paystream_idempotent_replays_total`, `paystream_payments_processed_total{status}`,
  `paystream_gateway_seconds`, `paystream_webhook_deliveries_total{result}`, `paystream_webhook_seconds`,
  `paystream_outbox_pending_events`, `paystream_outbox_publish_lag_seconds`, `paystream_messages_retried_total{delay_ms}`,
  `paystream_messages_dead_lettered_total{reason}`, `paystream_http_requests_total`, `paystream_http_request_seconds`.
  Собственные метрики FastStream сохраняют префикс `faststream_`. Глубину dead-letter queue дашборд Grafana берёт
  напрямую из RabbitMQ.
- **Трейсы:** включаются через `OTEL_ENABLED=true` (в `make observability` уже включены). W3C `traceparent` передаётся
  в строке outbox и в заголовках сообщения, поэтому один трейс идёт от запроса к API через relay и consumer до отправки
  webhook'а. Эндпоинты здоровья и метрик в трейсы не попадают.

## Конфигурация

Все настройки — переменные окружения; [`.env.example`](.env.example) перечисляет те, что compose передаёт в контейнеры.

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://paystream:paystream@localhost:5432/paystream` | DSN PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | URL RabbitMQ |
| `API_KEYS` | *(обязательна)* | Ключи через запятую; несколько ключей позволяют их ротацию |
| `WEBHOOK_SIGNING_SECRET` | *(обязательна для consumer)* | Секрет `whsec_…` |
| `WEBHOOK_ALLOWED_HOSTS` | *(пусто)* | Хосты, для которых не проверяется публичность адреса |
| `WEBHOOK_CONNECT_TIMEOUT` / `WEBHOOK_READ_TIMEOUT` | `3.0` / `5.0` | Таймауты webhook'а, секунды |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2.0` / `5.0` | Задержка эмулятора шлюза, секунды |
| `GATEWAY_SUCCESS_RATE` | `0.9` | Доля одобренных платежей |
| `RETRY_MAX_ATTEMPTS` | `3` | Попыток до dead-letter queue |
| `RETRY_BASE_DELAY_MS` | `2000` | Первая задержка повтора; каждый раз удваивается |
| `CONSUMER_PREFETCH` | `10` | Сообщений в работе на один consumer |
| `DELIVERY_LIMIT` | `10` | Повторных доставок одного сообщения, после которых RabbitMQ отправит его в dead-letter queue; значение должно совпадать во всех процессах, а его смена требует пересоздать `payments.new` |
| `OUTBOX_BATCH_SIZE` | `100` | Строк за один проход relay |
| `OUTBOX_POLL_INTERVAL` | `1.0` | Резервный опрос, если NOTIFY не пришёл, секунды |
| `OUTBOX_MAX_BACKOFF` | `60.0` | Потолок задержки повторной публикации в relay, секунды |
| `OUTBOX_RETENTION_DAYS` | `7` | Через сколько дней удаляются опубликованные строки outbox |
| `LOG_JSON` / `LOG_LEVEL` | `true` / `INFO` | Формат и уровень логов |
| `METRICS_PORT` | `9100` | Порт метрик relay и consumer |
| `METRICS_REQUIRE_API_KEY` | `false` | Закрыть `/metrics` у API ключом `X-API-Key` |
| `OTEL_ENABLED` | `false` | Экспортировать трейсы |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` | Эндпоинт OTLP/HTTP для трейсов (полный URL) |

Сгенерировать секрет подписи:
`uv run python -c "from paystream.webhooks.signing import generate_secret; print(generate_secret())"`.

## Разработка

```bash
uv sync
make test        # unit + integration tests; integration tests start PostgreSQL and RabbitMQ with testcontainers
make test-unit   # unit tests only, no Docker
make coverage    # the same suite with the 90 % branch-coverage gate
make lint        # ruff, ruff format --check, mypy --strict
make e2e         # end-to-end tests against the running compose stack
make load        # load test: 500 payments, 50 concurrent
```

CI запускает линтеры, тесты с порогом покрытия и сквозные тесты против `docker compose` на каждый push в `main` и
каждый pull request.

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

## Производительность

Замеры сделаны `scripts/loadtest.py` на одном ноутбуке (стек, Docker Desktop и генератор нагрузки на одном Mac на Apple
Silicon); полные условия, сырой вывод и прогон по уровням параллелизма — в [docs/benchmarks.md](docs/benchmarks.md).

| Сценарий | Приём p50 / p95 | Сквозная p50 / p95 | Пропускная способность |
|---|---|---|---|
| Шлюз по умолчанию (2–5 с), 1 consumer, 500 платежей, 50 параллельно | 70.0 ms / 197.7 ms | 90.02 s / 168.11 s | 2.8 payments/s |
| Быстрый шлюз (50–100 мс), 3 consumer, 1000 платежей, 100 параллельно | 211.5 ms / 2416.6 ms | 0.20 s / 0.63 s | 160.5 payments/s |

**Приём** — время клиента до ответа `202`; **сквозная** — `processed_at − created_at`, как их отдаёт API. В первом
сценарии сквозное время — это ожидание в очереди: один consumer с 10 сообщениями в работе против шлюза с задержкой
2–5 с завершает около 2,8 платежа в секунду. Во втором конвейер справляется; его пропускная способность — это поданная
нагрузка, а p95 приёма ограничен путём от macOS к Docker Desktop, а не самим сервисом.

## Безопасность

Значения по умолчанию в compose (`API_KEYS=dev-api-key` и публичный тестовый секрет Standard Webhooks) годятся только
для локального запуска: замените оба, прежде чем запускать Paystream где-то ещё. Поставьте перед API обратный прокси,
который терминирует TLS и ограничивает размер тела запроса. [SECURITY.md](SECURITY.md) описывает модель угроз, границы
защиты от SSRF и то, как сообщить об уязвимости.

## Архитектурные решения

[Записи архитектурных решений](docs/adr/README.md) объясняют transactional outbox, семантику ключа идемпотентности,
идемпотентный consumer, уровни повторов, подпись webhook'ов с защитой от SSRF и quorum queues.

## Планы

- Подключаться к адресу, прошедшему проверку SSRF, при доставке webhook'ов (закрывает окно, описанное в
  [ADR 0005](docs/adr/0005-webhook-signing-and-ssrf.md))
- Адаптер настоящего шлюза, который передаёт идентификатор платежа как ключ идемпотентности шлюза
- Список платежей с курсорной пагинацией
- Необязательный relay на основе CDC (Debezium) вместо опроса outbox

## Лицензия

MIT, см. [LICENSE](LICENSE).
