# 0003. At-least-once delivery with an idempotent consumer

- Status: Accepted
- Date: 2026-10-06

## Context

RabbitMQ delivers at least once: retries, consumer crashes and relay re-publishes all produce duplicates. A consumer
that re-runs the gateway on every delivery lets a duplicate flip `succeeded` to `failed` and send a second,
contradictory webhook.

## Decision

The single consumer handler is a resumable state machine. Every step is safe to repeat:

1. Check the message. One that can never succeed is dead-lettered at once, without retries, with its reason in
   `x-dead-reason`: a body that cannot be decoded or is not a valid event envelope, or an unreadable `x-attempt` header
   (`invalid_message`); an `event_type` other than `payment.created` (`unsupported_event`); an envelope whose `data` is
   not a valid `payment.created` payload (`invalid_event`).
2. Load the payment (unknown id → dead-lettered as `payment_not_found`).
3. If `pending`: call the gateway **outside** any transaction, then
   `UPDATE payments SET status=… WHERE id=… AND status='pending'`. Zero rows means another delivery won; continue with
   the stored status.
4. If final, `webhook_url` set and `webhook_delivered_at` empty: send the signed webhook, log the attempt in
   `webhook_attempts`, set `webhook_delivered_at` on 2xx.
5. Ack once the message is settled: processed, retried (a copy published to a retry tier,
   [ADR 0004](0004-exponential-retry-ttl-queues.md)) or dead-lettered. If the broker refuses that publish, the message
   goes back to the queue instead ([ADR 0006](0006-quorum-queues.md)).

A gateway decline (10 % by default in the emulation, `GATEWAY_SUCCESS_RATE`) is a business outcome (`failed` +
`failure_reason`), never a retry.

## Consequences

- A redelivery never records a second outcome, a payment that has left `pending` never goes to the gateway again, and a
  retry after a webhook failure re-sends only the webhook.
- The webhook id is derived from the payment id and its final status, so receivers can deduplicate on `webhook-id`.
- Remaining window: between the gateway's answer and the commit of the status. A consumer that crashes there, or two
  copies of the message processed at the very same moment, call the gateway twice for one payment. Only one status is
  recorded; two copies at once both send the webhook, with that status and under the same `webhook-id`
  (`tests/integration/test_processing.py::test_concurrent_duplicates_agree_on_one_outcome`). In production the
  gateway's own idempotency key, the payment id, closes this window; the emulation does not need one. Receivers
  deduplicate the webhooks on `webhook-id`.
- No inbox table is needed: the payment row itself is the deduplication state.

## Alternatives considered

- **Inbox table keyed by message_id**: duplicates the information already in the payment's status. Rejected (YAGNI).
- **Advisory lock per payment**: closes the concurrent-duplicate window but serializes on Postgres for a rare case.
  Not implemented.
