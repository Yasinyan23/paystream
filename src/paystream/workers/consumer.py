import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

import httpx
import structlog
from faststream import AckPolicy, FastStream
from faststream.message import StreamMessage
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware
from prometheus_client import REGISTRY, start_http_server

from paystream.core.config import Settings, get_settings
from paystream.core.logging import configure_logging
from paystream.core.telemetry import configure_tracing, instrument_database
from paystream.db.database import Database, database_unavailable
from paystream.domain.events import EventEnvelope
from paystream.gateway.simulated import PaymentGateway, SimulatedGateway
from paystream.messaging.broker import build_broker, telemetry_middlewares
from paystream.messaging.retry import ATTEMPT_HEADER, RetryPublisher
from paystream.messaging.topology import PAYMENTS_EXCHANGE, declare_topology, payments_queue
from paystream.services.processing import PaymentProcessor, PermanentProcessingError
from paystream.webhooks.sender import WebhookSender, build_http_client

logger = logging.getLogger(__name__)


class DeliveredMessage(Protocol):
    @property
    def headers(self) -> Mapping[str, Any]: ...

    @property
    def message_id(self) -> str | None: ...

    @property
    def correlation_id(self) -> str | None: ...

    async def ack(self, multiple: bool = False) -> None: ...

    async def reject(self, requeue: bool = False) -> None: ...


async def decode_or_raw(
    message: StreamMessage[Any], original_decoder: Callable[[StreamMessage[Any]], Awaitable[Any]]
) -> Any:
    """A payload that cannot be decoded still has to be settled.

    Invalid JSON under a JSON content type, bytes that are not UTF-8, JSON nested deeply enough to overflow the
    parser's stack: whatever the decoder raises, FastStream would leave the message unacked. Its raw bytes go to
    handle_delivery instead, which dead-letters them as an invalid message.
    """
    try:
        return await original_decoder(message)
    except Exception:  # noqa: BLE001 — a decoder failure of any kind means the payload is poison
        return message.body


def _attempt_number(headers: Mapping[str, Any]) -> int:
    """1-based number of the delivery attempt; a message that never failed carries no header."""
    value = headers.get(ATTEMPT_HEADER, 1)
    try:
        return int(value)
    except TypeError, ValueError:
        raise ValueError(f"unreadable {ATTEMPT_HEADER} header: {value!r:.100}") from None


async def handle_delivery(
    processor: PaymentProcessor,
    retries: RetryPublisher,
    *,
    body: Any,
    message: DeliveredMessage,
) -> None:
    """Settle one delivery: ack it once it is routed, give it back to the queue if it cannot be. With manual acks
    an exception escaping from here would leave the message unacked, holding a prefetch slot until the channel
    closes."""
    data = body.get("data") if isinstance(body, dict) else None
    payment_id = data.get("payment_id") if isinstance(data, dict) else None
    # Every log line of this delivery carries these. Contextvars are per task and aio-pika runs each delivery in
    # its own, so concurrent messages keep their own context. The correlation id is the API request's X-Request-ID.
    structlog.contextvars.bind_contextvars(message_id=message.message_id, correlation_id=message.correlation_id)
    if payment_id:
        structlog.contextvars.bind_contextvars(payment_id=payment_id)
    try:
        headers = dict(message.headers or {})
        route: dict[str, Any] = {
            "body": body,
            "message_id": message.message_id or "",
            "correlation_id": message.correlation_id,
            "headers": headers,
        }
        attempt = 1  # stays 1 if the header turns out to be unreadable
        try:
            attempt = _attempt_number(headers)
            structlog.contextvars.bind_contextvars(attempt=attempt)
            event = EventEnvelope.model_validate(body)
        except ValueError as exc:  # an unreadable header, or pydantic's ValidationError (a ValueError as well)
            await retries.dead_letter(**route, reason="invalid_message", error=str(exc), attempt=attempt)
        else:
            try:
                await processor.process(event)
            except PermanentProcessingError as exc:
                await retries.dead_letter(**route, reason=exc.reason, error=str(exc), attempt=attempt)
            except Exception as exc:  # noqa: BLE001 — anything else is worth a retry
                # An unreachable database is an outage, not the message's fault: spending attempts on it would send
                # the whole backlog to the DLQ within seconds.
                await retries.retry_or_dead_letter(
                    **route,
                    attempt=attempt,
                    error=f"{type(exc).__name__}: {exc}",
                    spend_attempt=not database_unavailable(exc),
                )
            else:
                logger.info("message %s attempt %d processed", message.message_id, attempt)
    except Exception:
        # Could not even route the message (the broker refused the retry or dead-letter publish). Give it back to
        # the queue: a reject counts toward the quorum queue's x-delivery-limit (a nack does not), which
        # dead-letters it if this keeps happening.
        logger.exception("could not route message %s, requeueing", message.message_id)
        await message.reject(requeue=True)
    else:
        await message.ack()
    finally:
        structlog.contextvars.unbind_contextvars("message_id", "correlation_id", "attempt", "payment_id")


def build_consumer(
    settings: Settings,
    *,
    gateway: PaymentGateway | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> FastStream:
    if settings.webhook_signing_secret is None:
        raise RuntimeError("WEBHOOK_SIGNING_SECRET must be set")
    broker = build_broker(
        settings,
        middlewares=[
            *telemetry_middlewares(settings),
            RabbitPrometheusMiddleware(registry=REGISTRY, app_name="paystream-consumer"),
        ],
    )
    db = Database(settings.database_url)
    if settings.otel_enabled:
        instrument_database(db)
    client = http_client or build_http_client(settings)
    sender = WebhookSender(
        client,
        secret=settings.webhook_signing_secret.get_secret_value(),
        allowed_hosts=settings.webhook_allowed_hosts,
    )
    processor = PaymentProcessor(
        db,
        gateway
        or SimulatedGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        ),
        sender,
    )
    retries = RetryPublisher(broker, settings.retry_delays_ms)

    @broker.subscriber(
        payments_queue(settings.delivery_limit),
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        decoder=decode_or_raw,
    )
    async def handle_payment(body: Any, message: RabbitMessage) -> None:
        await handle_delivery(processor, retries, body=body, message=message)

    app = FastStream(broker)

    @app.on_startup
    async def _declare_topology() -> None:
        # Retry and DLQ exchanges must exist before the first message can fail.
        await broker.connect()
        await declare_topology(broker, settings)

    @app.after_shutdown
    async def _close() -> None:
        if http_client is None:
            await client.aclose()
        await db.dispose()

    return app


def main() -> None:  # pragma: no cover
    settings = get_settings()
    configure_logging("consumer", json_logs=settings.log_json, level=settings.log_level)
    configure_tracing("consumer", settings)
    start_http_server(settings.metrics_port)
    asyncio.run(build_consumer(settings).run())


if __name__ == "__main__":
    main()
