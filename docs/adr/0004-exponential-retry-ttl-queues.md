# 0004. Exponential retry through tiered TTL queues

- Status: Accepted
- Date: 2026-10-06

## Context

The task brief ([docs/task.md](../task.md)) asks for 3 attempts with exponential delay and a dead-letter queue
afterwards. The two simple implementations fall short: one retry queue with a fixed delay is not exponential, and an
in-process `asyncio.sleep` backoff holds the message and its prefetch slot for the whole wait.

## Decision

- `RETRY_MAX_ATTEMPTS=3` attempts in total; the delay before attempt *n+1* is `RETRY_BASE_DELAY_MS × 2^(n-1)`:
  2 s, then 4 s.
- One durable classic queue per delay tier (`payments.retry.2s`, `payments.retry.4s`), bound to the `payments.retry`
  exchange with the routing key `retry.<delay in ms>`. Each has an `x-message-ttl` equal to its delay and a dead-letter
  exchange pointing back to `payments` / `payment.created`. Tiers are generated from configuration.
- On an error that is not permanent (a failed webhook delivery, for instance, or an unexpected exception) the consumer
  publishes a copy to the right tier with `x-attempt = n+1` and `x-last-error` (publisher confirms), then acks the
  original. After the last attempt it publishes to `payments.dlx → payments.dead` with
  `x-dead-reason: max_attempts_exceeded`, `x-error`, `x-attempt`, `x-failed-at`, then acks. The message's own headers
  (correlation id, trace context) survive each hop; RabbitMQ's bookkeeping (`x-death` and the like) does not.
- An unreachable database is an infrastructure failure, not the message's: a refused or lost connection, an unknown
  host, a server that is shutting down or starting up. Then the copy goes to the longest tier with `x-attempt`
  unchanged (a warning in the log, counted in `paystream_messages_retried_total` like any retry), as many times as the
  outage lasts, so a backlog waits it out instead of using up its attempts and landing in the DLQ within seconds.
- A permanent failure skips the tiers and goes to the DLQ at once, with the attempt it happened on in `x-attempt` and
  the reason in `x-dead-reason`: `invalid_message`, `unsupported_event`, `invalid_event` and `payment_not_found`
  ([ADR 0003](0003-at-least-once-idempotent-consumer.md)), or `webhook_rejected` (the webhook URL failed the SSRF check,
  [ADR 0005](0005-webhook-signing-and-ssrf.md)).
- `paystream dlq list` peeks without consuming: messages are fetched unacked and go back to the queue when the channel
  closes. It prints each message's id, reason, error, attempts and failure time. The reason is `x-dead-reason` or, for a
  message that RabbitMQ dead-lettered itself, the broker's `x-last-death-reason`, such as `delivery_limit`
  ([ADR 0006](0006-quorum-queues.md)).
- `paystream dlq replay (--message-id ID | --all)` publishes dead letters back to `payments` with a fresh attempt
  counter (`x-attempt = 1`) and an `x-replayed-at` timestamp, without the dead-letter headers, and acks each one only
  after the publish is confirmed. One invocation looks only at the messages in the DLQ when it starts, each at most
  once: a message that fails again at once, an invalid one for instance, lands at the tail of the DLQ and waits for the
  next invocation. `--all` replays at most `--limit` (1000 by default) and says how many it left.

## Consequences

- No message waits inside the consumer: while it waits for its retry it sits in a TTL queue, holding neither a prefetch
  slot nor memory in a consumer.
- Retry state travels in headers, so any consumer replica can pick the message up.
- Publish-copy-then-ack is at-least-once; [ADR 0003](0003-at-least-once-idempotent-consumer.md) makes the duplicate
  harmless.

## Alternatives considered

- **Per-message TTL in one retry queue**: RabbitMQ expires messages only at the head of a queue, so a 4 s message
  blocks a 2 s one behind it. Rejected.
- **rabbitmq-delayed-message-exchange plugin**: not shipped with RabbitMQ (the official image does not include it),
  and it keeps delayed messages in a single node-local Mnesia table, so they are not replicated. Rejected.
- **Requeue** (`reject` with `requeue=True`): the broker redelivers at once, so there is no delay at all, and on a
  quorum queue every such return counts toward the delivery limit ([ADR 0006](0006-quorum-queues.md)). Rejected as a
  retry mechanism; the consumer requeues only when it cannot publish a retry or dead-letter copy.
