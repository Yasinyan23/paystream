import asyncio
import logging
from collections.abc import Mapping
from typing import Any, Protocol

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from paystream.core.config import Settings, get_settings
from paystream.db.database import Database
from paystream.domain.events import EventEnvelope
from paystream.gateway.simulated import PaymentGateway, SimulatedGateway
from paystream.messaging.broker import build_broker
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
    try:
        headers = dict(message.headers or {})
        attempt = int(headers.get(ATTEMPT_HEADER, 1))
        route: dict[str, Any] = {
            "body": body,
            "message_id": message.message_id or "",
            "correlation_id": message.correlation_id,
            "headers": headers,
        }
        try:
            event = EventEnvelope.model_validate(body)
        except ValidationError as exc:
            await retries.dead_letter(**route, reason="invalid_message", error=str(exc), attempt=attempt)
        else:
            try:
                await processor.process(event)
            except PermanentProcessingError as exc:
                await retries.dead_letter(**route, reason=exc.reason, error=str(exc), attempt=attempt)
            except Exception as exc:  # noqa: BLE001 — anything else is worth a retry
                await retries.retry_or_dead_letter(**route, attempt=attempt, error=f"{type(exc).__name__}: {exc}")
            else:
                logger.info("message %s attempt %d processed", message.message_id, attempt)
    except Exception:
        # Could not even route the message (broker trouble, unreadable headers). Give it back to the queue:
        # a reject counts toward the quorum queue's x-delivery-limit (a nack does not), which dead-letters it
        # if this keeps happening.
        logger.exception("could not route message %s, requeueing", message.message_id)
        await message.reject(requeue=True)
    else:
        await message.ack()


def build_consumer(
    settings: Settings,
    *,
    gateway: PaymentGateway | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> FastStream:
    if settings.webhook_signing_secret is None:
        raise RuntimeError("WEBHOOK_SIGNING_SECRET must be set")
    broker = build_broker(settings)
    db = Database(settings.database_url)
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

    @broker.subscriber(payments_queue(settings.delivery_limit), PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL)
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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(build_consumer(get_settings()).run())


if __name__ == "__main__":
    main()
