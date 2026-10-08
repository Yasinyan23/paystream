# Problem types

Every error response is [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457) with the media type
`application/problem+json`. The `type` field is one of:

| `type` | Status | When |
|---|---|---|
| `urn:paystream:problem:unauthorized` | 401 | `X-API-Key` is missing or is not one of `API_KEYS` |
| `urn:paystream:problem:idempotency-key-missing` | 400 | `POST /api/v1/payments` without an `Idempotency-Key` header |
| `urn:paystream:problem:idempotency-key-invalid` | 400 | The key is not 1–255 printable ASCII characters without spaces |
| `urn:paystream:problem:idempotency-key-reused` | 422 | The same key was used with a different request body |
| `urn:paystream:problem:validation-error` | 422 | The body or the path is invalid; the details are in `errors` |
| `urn:paystream:problem:payment-not-found` | 404 | No payment with that id |
| `urn:paystream:problem:not-found` | 404 | Unknown route |
| `urn:paystream:problem:method-not-allowed` | 405 | Wrong HTTP method; the `Allow` header lists the right ones |
| `urn:paystream:problem:http-error` | other 4xx | Any other HTTP error raised by the framework |
| `urn:paystream:problem:service-unavailable` | 503 | The database is unreachable: from `/ready`, or from any request during an outage (with `Retry-After`) |
| `urn:paystream:problem:internal-error` | 500 | An unexpected error; it is logged with the request id |

Every problem also carries `title`, `status`, `detail`, `instance` (the request path) and `request_id`, which equals the
`X-Request-ID` response header. A validation error lists each problem in `errors` with `type`, `loc`, `msg` and `ctx`;
the rejected input is not echoed back.

```json
{
  "type": "urn:paystream:problem:validation-error",
  "title": "Request validation failed",
  "status": 422,
  "detail": "One or more fields are invalid",
  "instance": "/api/v1/payments",
  "request_id": "ad84bbe857d545bab7b70a15afa8987e",
  "errors": [
    {"type": "greater_than_equal", "loc": ["body", "amount"], "msg": "Input should be greater than or equal to 0.01",
     "ctx": {"ge": 0.01}},
    {"type": "enum", "loc": ["body", "currency"], "msg": "Input should be 'RUB', 'USD' or 'EUR'",
     "ctx": {"expected": "'RUB', 'USD' or 'EUR'"}}
  ]
}
```
