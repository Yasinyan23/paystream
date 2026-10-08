# 0002. Idempotency-Key semantics

- Status: Accepted
- Date: 2026-10-06

## Context

Clients retry `POST /api/v1/payments` after timeouts. A retry must never create a second payment, two concurrent
retries must not race, and a key reused for a *different* payment must not silently return the old one.

## Decision

Modelled on the IETF draft [*The Idempotency-Key HTTP Header Field*](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/),
with three deliberate differences: the key is an opaque string, a replay returns the payment as it is now instead of
the first response, and a concurrent duplicate waits instead of getting `409`.

- The header is required, and the OpenAPI document marks it so. Missing → `400` (problem type
  `urn:paystream:problem:idempotency-key-missing`); a value that is not 1–255 printable ASCII characters (space
  excluded) → `400` (`idempotency-key-invalid`). The draft specifies a quoted Structured Fields string; this service
  does not parse one and takes the value as it is.
- The body is validated before the key is looked up: `amount` of at least 0.01 with at most 2 decimal places and 18
  digits; `metadata` of at most 16 KB (16,384 bytes) as UTF-8 JSON and at most 32 levels deep, without NUL, lone
  surrogates, NaN or Infinity. A body that fails gets `422` (`validation-error`) and writes nothing, so it cannot
  reserve a key.
- The amount is normalized to 2 decimals (`100` and `"100.00"` are the same), and `amount`, `currency`, `description`,
  `metadata` and `webhook_url` are fingerprinted with sha256 over canonical JSON (sorted keys, compact separators), so
  the order of keys in the request does not matter. The fingerprint is stored in `payments.request_hash`.
- `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING *` creates the payment. The unique index is the only
  arbiter; there is no check-then-insert window.
- On conflict the stored payment is read: same fingerprint → `202` with the payment as it is now, its current status
  included, and `Idempotent-Replayed: true`; different fingerprint → `422` (`idempotency-key-reused`). No response is
  stored: the draft has a retry answered with the result of the original request, this service answers with the
  payment's current state.
- A concurrent duplicate is not rejected: PostgreSQL blocks its `INSERT` until the first request commits, and it then
  takes the replay path. The draft suggests `409 Conflict` for a request whose twin is still in flight; this service
  never returns it.

## Consequences

- Concurrent identical requests produce exactly one payment and one outbox event (covered by
  `tests/integration/test_payments_api.py::test_concurrent_duplicates_create_one_payment`, which fires 10 at once).
- A replay returns the payment's **current** status (it may already be `succeeded`), which is more useful to a client
  than a byte-identical copy of the first response.
- Keys live as long as the payment; there is no expiry.
- The key space is global, not per API key: the service has static keys and no tenants. Two callers that send the same
  `Idempotency-Key` collide, so clients should use unguessable keys such as UUIDs.

## Alternatives considered

- **SELECT then INSERT**: two concurrent requests both miss the SELECT, and the loser fails on the unique index with
  `IntegrityError` → `500`, unless that error is caught and turned into a replay. Rejected.
- **Caching responses in Redis with a TTL**: adds a dependency and a second source of truth. Rejected.
- **`409 Conflict` for a concurrent duplicate**, as the draft suggests: the client would have to retry, while waiting on
  the unique index hands it the result at once. Rejected.
