# 0006. Quorum queue with a delivery limit

- Status: Accepted
- Date: 2026-10-06

## Context

`payments.new` holds work that must survive a broker node failure, and a message that crashes the consumer process
must not be redelivered forever.

## Decision

- `payments.new` is a **quorum** queue (Raft-replicated, durable) with `x-delivery-limit` taken from `DELIVERY_LIMIT`
  (10 by default) and `x-dead-letter-exchange=payments.dlx` (routing key `payment.dead`). Once a message has been
  returned to the queue more often than the limit, RabbitMQ dead-letters it by itself with the reason `delivery_limit`:
  after 11 deliveries with the default.
- What counts as a return depends on how the message is given back. On RabbitMQ 4.3 (checked on 4.3.6) a `reject` with
  `requeue=True` counts, and so does a consumer connection that closes while it holds the message, which is what a
  crash looks like to the broker. A `nack` with `requeue=True` does not count: a consumer that gave a message back that
  way would loop on it forever, whatever the limit.
- So the consumer gives a message back with `reject(requeue=True)`, never with `nack`. It does so in one case, when it
  cannot route the message because the broker refused the retry or dead-letter publish
  ([ADR 0003](0003-at-least-once-idempotent-consumer.md)). The message returns to the queue, and the limit dead-letters
  it if that keeps happening.
- Retry tiers are classic durable queues: they only hold messages for seconds.
- `payments.dead` is a classic durable queue on purpose: `dlq list` returns messages to the queue by closing the channel
  ([ADR 0004](0004-exponential-retry-ttl-queues.md)), and a quorum queue would count each listing toward its delivery
  limit and eventually drop them.

## Consequences

- Crash loops end in the DLQ instead of burning CPU. The message arrives in `payments.dead` with an `x-death` entry
  whose reason is `delivery_limit` and no `x-dead-reason`; `dlq list` shows `delivery_limit` as its reason.
- The limit covers what the consumer cannot count itself: `x-attempt` only grows when the consumer lives to publish a
  retry copy, never when it dies.
- During a database outage neither bound applies, on purpose: the consumer spends no attempt on it
  ([ADR 0004](0004-exponential-retry-ttl-queues.md)), and each wait in a retry tier is a new message to the delivery
  limit. Events go round the longest tier until the database is back instead of filling the DLQ.
- RabbitMQ's own dead-lettering, at the delivery limit and on each expiry in a retry tier, is at-most-once by default:
  the broker moves the message without publisher confirms and can lose it if the target queue cannot take it. On a
  single node, as in the compose stack, that is fine. A cluster needs `x-dead-letter-strategy: at-least-once` and
  `x-overflow: reject-publish` on `payments.new` and on the retry tiers, which then have to be quorum queues as well
  (only quorum queues dead-letter at-least-once). Not implemented.
- The counting rules belong to the broker, not to this code. The compose file and the tests run RabbitMQ 4
  (`rabbitmq:4-management-alpine`), and the integration tests pin the `reject` path
  (`tests/integration/test_consumer.py::test_crash_loop_is_dead_lettered_at_the_delivery_limit` and
  `::test_message_that_cannot_be_dead_lettered_is_requeued_until_the_delivery_limit`), so a release that changes it
  shows up in CI.

## Alternatives considered

- **Classic mirrored queues**: removed in RabbitMQ 4.0. Rejected.
- **Streams**: a replayable log that consumers read by offset, not a queue that redelivers and dead-letters individual
  messages. Rejected.
- **No broker-side limit** (counting attempts in `x-attempt` only): a message that kills the consumer never reaches the
  code that increments the counter, so it would be redelivered forever. Rejected.
