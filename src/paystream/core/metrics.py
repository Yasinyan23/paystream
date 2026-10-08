"""All Prometheus metrics in one place. Names follow Prometheus conventions; label sets are small and bounded."""

from prometheus_client import Counter, Gauge, Histogram

PAYMENTS_CREATED = Counter("paystream_payments_created_total", "Payments accepted by the API", ["currency"])
IDEMPOTENT_REPLAYS = Counter("paystream_idempotent_replays_total", "Requests answered with an existing payment")
PAYMENTS_PROCESSED = Counter("paystream_payments_processed_total", "Payments that reached a final status", ["status"])
GATEWAY_SECONDS = Histogram(
    "paystream_gateway_seconds", "Gateway call duration", buckets=(0.5, 1, 2, 3, 4, 5, 7.5, 10, 15)
)
WEBHOOK_DELIVERIES = Counter("paystream_webhook_deliveries_total", "Webhook delivery attempts", ["result"])
WEBHOOK_SECONDS = Histogram("paystream_webhook_seconds", "Webhook HTTP round trip")
OUTBOX_PENDING = Gauge("paystream_outbox_pending_events", "Outbox events not yet published")
OUTBOX_LAG_SECONDS = Histogram(
    "paystream_outbox_publish_lag_seconds",
    "Time from writing the outbox row to publishing it",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
MESSAGES_RETRIED = Counter("paystream_messages_retried_total", "Messages sent to a retry tier", ["delay_ms"])
MESSAGES_DEAD_LETTERED = Counter(
    "paystream_messages_dead_lettered_total",
    "Dead letters published by the consumer; RabbitMQ's own delivery-limit dead-letters are not counted",
    ["reason"],
)
HTTP_REQUESTS = Counter("paystream_http_requests_total", "HTTP requests", ["method", "route", "status"])
HTTP_SECONDS = Histogram("paystream_http_request_seconds", "HTTP request duration", ["method", "route"])
