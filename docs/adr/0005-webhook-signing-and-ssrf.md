# 0005. Webhook signing and SSRF protection

- Status: Accepted
- Date: 2026-10-06

## Context

A webhook is an HTTP request from our infrastructure to a URL a client chose. The receiver must be able to verify it
came from us, and we must not let the URL point into our own network.

## Decision

- Sign per the [**Standard Webhooks**](https://www.standardwebhooks.com) specification: headers `webhook-id`,
  `webhook-timestamp` (Unix seconds), `webhook-signature: v1,<base64 HMAC-SHA256 of "id.timestamp.body">`, secret
  `whsec_…` (`WEBHOOK_SIGNING_SECRET`; the consumer refuses to start without it). The signed body is the exact bytes
  that are sent. The implementation is checked against the reference test vector used by the Standard
  Webhooks libraries
  (`tests/unit/test_webhooks.py::test_sign_matches_reference_vector`).
- `webhook-id` is stable across retries of the same event: it is derived from the payment id and its status.
- Before **every** delivery: only `http`/`https`; resolve the host and refuse if any address is not global according to
  Python's `ipaddress` (private, loopback, link-local including the cloud metadata address, carrier-grade NAT, and the
  other special-purpose ranges). The name that is resolved is the one httpx will connect to, the IDNA 2008 A-label
  (`raw_host`): the standard library's IDNA 2003 codec would turn `faß.example` into `fass.example`, a different name.
  A URL that cannot be parsed, or a host that cannot be encoded for DNS (an empty or over-long label), is refused.
  `WEBHOOK_ALLOWED_HOSTS` exempts named hosts from the check, for local development (`webhook-sink`).
- The HTTP client does not follow redirects and ignores proxy settings from the environment (`trust_env=False`): either
  would send the request somewhere the check never looked.
- Timeouts: connect 3 s and read 5 s (`WEBHOOK_CONNECT_TIMEOUT`, `WEBHOOK_READ_TIMEOUT`), plus one deadline of connect +
  read (8 s by default) for the whole delivery, URL check and response included, so a receiver that sends a byte every
  few seconds cannot hold a worker.
- Of any response, at most about 1 KiB of body is read, raw (`Accept-Encoding: identity`), and the rest of the stream
  is abandoned; a shorter body is read to its end, which lets the connection go back to the pool. For a failed
  response the excerpt goes into the stored error, which is at most 1,024 characters and has NUL removed, which a
  Postgres text column rejects.
- A refused URL is a permanent failure (straight to the DLQ as `webhook_rejected`); DNS errors, connection errors,
  timeouts and non-2xx responses are transient (retried, [ADR 0004](0004-exponential-retry-ttl-queues.md)).

## Consequences

- Receivers can verify deliveries with a Standard Webhooks library instead of custom code.
- Re-checking on each attempt catches a host re-pointed to an internal address after the payment was created.
- A host named in `WEBHOOK_ALLOWED_HOSTS` skips the check entirely. The compose file and `.env.example` set it to
  `webhook-sink` for the demo receiver; elsewhere it should stay empty or name only hosts you trust.
- Remaining gap: between our DNS check and httpx's own resolution a hostile DNS server could change the answer
  (TOCTOU). Closing it needs connecting to the checked IP with SNI/Host pinned. Not implemented.

## Alternatives considered

- **Unsigned webhooks**: receivers cannot trust them. Rejected.
- **Custom signature scheme**: no ecosystem support. Rejected.
