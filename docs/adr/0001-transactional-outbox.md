# 0001. Transactional outbox with a LISTEN/NOTIFY relay

- Status: Accepted
- Date: 2026-10-06

## Context

Creating a payment must also publish `payment.created`. Writing to PostgreSQL and publishing to RabbitMQ are two systems
without a shared transaction: publish-then-commit can announce a payment that was rolled back, commit-then-publish can
lose the event if the process dies in between.

## Decision

The API writes the payment and an `outbox` row in **one** database transaction and calls
`pg_notify('paystream_outbox', '')` in the same transaction (Postgres delivers the notification only on commit). The API
never talks to RabbitMQ. A separate `outbox-relay` process:

1. wakes up on `LISTEN paystream_outbox`, with a polling fallback in case a notification is missed
   (`OUTBOX_POLL_INTERVAL`, 1 s by default);
2. claims due rows with `SELECT … FOR UPDATE SKIP LOCKED`, up to `OUTBOX_BATCH_SIZE` (100) per batch, so replicas share
   the work without coordination;
3. publishes each with publisher confirms, `mandatory=True` (an unroutable message raises instead of vanishing),
   persistent delivery and `message_id = outbox.id`, carrying the row's `headers`;
4. marks the rows `published`; on failure increments `attempts`, keeps the error in `last_error` and schedules
   `next_attempt_at` with exponential backoff (1 s doubling, ±20 % jitter, capped at `OUTBOX_MAX_BACKOFF`, 60 s by
   default), then stops the batch (the broker is probably down).

An event is never marked failed and dropped. Published rows are purged after `OUTBOX_RETENTION_DAYS` (7 by default); the
relay purges at most once an hour.

The row's `headers` hold the request's `correlation_id` (its `X-Request-ID`) and, when a trace is active, its W3C trace
context (`traceparent`). The relay makes that context current while it publishes and sends the same headers with the
message, so with tracing on (`OTEL_ENABLED=true`) one trace runs from the API request through the relay and the consumer
to the webhook call.

## Consequences

- No lost events: if RabbitMQ is down, payments are still accepted (`/ready` checks only the database) and the outbox
  drains when the broker comes back.
- Publication is at-least-once: a crash after publish and before commit re-publishes the same `message_id`. The consumer
  is idempotent ([ADR 0003](0003-at-least-once-idempotent-consumer.md)), so duplicates are harmless.
- Publishing does not wait for the polling interval: the relay wakes on NOTIFY as soon as the transaction commits
  (`tests/integration/test_outbox_relay.py::test_run_relay_wakes_up_on_notify` runs it with a 30 s interval and expects
  the message within 5 s). The interval only matters when a notification is missed.
- Events are not ordered: relay replicas, retries after a failed publish and a concurrent consumer can all reorder them.
  Nothing relies on order: each payment has exactly one event and the consumer works from the payment row.
- `paystream_outbox_pending_events` (rows not yet published) and `paystream_outbox_publish_lag_seconds` (time from
  writing the row to publishing it) make a stuck or lagging relay visible.

## Alternatives considered

- **Publish from the API after commit**: loses the event if the process dies between commit and publish. Rejected.
- **CDC (Debezium) instead of a relay**: log-based, so no polling or `NOTIFY`, but it adds Kafka Connect or a Debezium
  server to a service whose stack the [task brief](../task.md) fixes. Not implemented.
- **Polling only** (no `LISTEN`): simpler, but every event waits up to one polling interval before it is published.
