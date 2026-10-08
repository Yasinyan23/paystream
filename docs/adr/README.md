# Architecture Decision Records

| # | Decision | Status |
|---|---|---|
| [0001](0001-transactional-outbox.md) | Transactional outbox with a LISTEN/NOTIFY relay | Accepted |
| [0002](0002-idempotency-key.md) | Idempotency-Key semantics: unique-index arbitration and request fingerprints | Accepted |
| [0003](0003-at-least-once-idempotent-consumer.md) | At-least-once delivery with an idempotent consumer state machine | Accepted |
| [0004](0004-exponential-retry-ttl-queues.md) | Exponential retry through tiered TTL queues | Accepted |
| [0005](0005-webhook-signing-and-ssrf.md) | Webhook signing (Standard Webhooks) and SSRF protection | Accepted |
| [0006](0006-quorum-queues.md) | Quorum queue with a delivery limit for the main work queue | Accepted |
