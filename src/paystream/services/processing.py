import logging
from typing import Any
from uuid import UUID

from paystream.db.database import Database
from paystream.db.models import Payment, WebhookAttempt
from paystream.domain.events import PAYMENT_CREATED, EventEnvelope, PaymentCreatedData, webhook_event_id
from paystream.domain.payment import PaymentStatus
from paystream.gateway.simulated import PaymentGateway
from paystream.repositories.payments import PaymentRepository
from paystream.repositories.webhook_attempts import WebhookAttemptRepository
from paystream.webhooks.sender import WebhookSender

logger = logging.getLogger(__name__)


class PermanentProcessingError(Exception):
    """Retrying cannot help: the message goes straight to the DLQ."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


class TransientProcessingError(Exception):
    """Worth retrying with backoff (webhook endpoint down, timeouts)."""


def webhook_payload(payment: Payment) -> dict[str, Any]:
    occurred_at = payment.processed_at or payment.updated_at
    return {
        "type": f"payment.{payment.status}",
        "timestamp": occurred_at.isoformat(),
        "data": {
            "id": str(payment.id),
            "amount": f"{payment.amount:.2f}",
            "currency": payment.currency.value,
            "status": payment.status.value,
            "failure_reason": payment.failure_reason,
            "description": payment.description,
            "metadata": payment.metadata_,
            "created_at": payment.created_at.isoformat(),
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        },
    }


class PaymentProcessor:
    """The single consumer's logic. Every step is safe to repeat, so redelivery never double-charges
    and a retry after a webhook failure only re-sends the webhook.

    1. pending → call the gateway (outside any transaction), then a conditional UPDATE … WHERE status='pending'.
    2. final + webhook_url + not delivered → send the signed webhook, log the attempt, mark delivered on 2xx.
    """

    def __init__(self, db: Database, gateway: PaymentGateway, webhooks: WebhookSender) -> None:
        self._db, self._gateway, self._webhooks = db, gateway, webhooks

    async def process(self, event: EventEnvelope) -> None:
        if event.event_type != PAYMENT_CREATED:
            raise PermanentProcessingError("unsupported_event", event.event_type)
        data = PaymentCreatedData.model_validate(event.data)
        payment = await self._load(data.payment_id)

        if payment.status is PaymentStatus.PENDING:
            charge = await self._gateway.charge(payment.id, payment.amount, payment.currency)
            status = PaymentStatus.SUCCEEDED if charge.succeeded else PaymentStatus.FAILED
            async with self._db.sessionmaker() as session, session.begin():
                applied = await PaymentRepository(session).complete(
                    payment.id, status=status, failure_reason=charge.failure_reason
                )
            logger.info("payment %s -> %s (applied=%s)", payment.id, status, applied)
            payment = await self._load(payment.id)  # another consumer may have won the race

        if payment.webhook_url is not None and payment.webhook_delivered_at is None:
            await self._deliver_webhook(payment, payment.webhook_url)

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._db.sessionmaker() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PermanentProcessingError("payment_not_found", str(payment_id))
        return payment

    async def _deliver_webhook(self, payment: Payment, url: str) -> None:
        event_id = webhook_event_id(payment.id, payment.status)
        async with self._db.sessionmaker() as session:
            attempt = await WebhookAttemptRepository(session).count(payment.id) + 1
        result = await self._webhooks.deliver(url, event_id=event_id, payload=webhook_payload(payment))
        async with self._db.sessionmaker() as session, session.begin():
            WebhookAttemptRepository(session).add(
                WebhookAttempt(
                    payment_id=payment.id,
                    event_id=event_id,
                    attempt=attempt,
                    status_code=result.status_code,
                    error=result.error,
                    duration_ms=result.duration_ms,
                )
            )
            if result.ok:
                await PaymentRepository(session).mark_webhook_delivered(payment.id)
        if result.ok:
            logger.info("webhook %s delivered for payment %s (attempt %d)", event_id, payment.id, attempt)
            return
        if result.permanent:
            raise PermanentProcessingError("webhook_rejected", result.error or "")
        raise TransientProcessingError(f"webhook delivery failed: {result.error}")
