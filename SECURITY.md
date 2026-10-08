# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's private vulnerability reporting (the repository's
**Security** tab → **Report a vulnerability**), not in a public issue. If that option is not available, open an issue
that asks for a private contact without describing the problem. Include the steps to reproduce and the affected
commit. You will get an answer within 7 days.

## Before running Paystream outside your laptop

Paystream is a reference implementation. The compose file is tuned for local development:

- **Replace the dev credentials.** `API_KEYS=dev-api-key` and `WEBHOOK_SIGNING_SECRET` (the public Standard Webhooks
  test secret) are well known. Generate a secret with
  `uv run python -c "from paystream.webhooks.signing import generate_secret; print(generate_secret())"`, use long random
  API keys, and change the PostgreSQL and RabbitMQ passwords.
- **Put a reverse proxy in front of the API** that terminates TLS and limits the request body size. The API parses the
  body before it checks the key; metadata is capped at 16 KB, but the request itself is not.
- **Decide who may read `/metrics`.** It is open by default; `METRICS_REQUIRE_API_KEY=true` puts it behind `X-API-Key`.
- **Keep the observability profile private.** Grafana runs with anonymous Editor access, and Prometheus, Jaeger and the
  RabbitMQ management UI have no extra authentication. Compose publishes all of them on `127.0.0.1` only.

## What the webhook SSRF guard does and does not cover

Before each delivery the webhook host is resolved, and every address must be public (`ipaddress.is_global`); the name
that is checked is the same IDNA 2008 name the HTTP client connects to. Redirects are not followed, proxy environment
variables are ignored, and only a bounded part of an error response is read.

Known limits:

- **DNS rebinding.** The address is checked, then the HTTP client resolves the name again when it connects. A host
  whose DNS answer changes between the two lookups can still reach a private address
  ([ADR 0005](docs/adr/0005-webhook-signing-and-ssrf.md)). Run the consumer in a network that cannot reach internal
  services, or route webhooks through an egress proxy.
- **`WEBHOOK_ALLOWED_HOSTS` skips the address check** for the hosts it lists. Use it for trusted internal receivers
  only.
- **Tracing exports webhook URLs.** With `OTEL_ENABLED=true` the HTTP client span records the full webhook URL,
  including its path and query, which may contain tokens. Tracing is off by default.
