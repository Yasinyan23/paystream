from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid7

from paystream.core.metrics import IDEMPOTENT_REPLAYS, PAYMENTS_CREATED
from paystream.db.database import Database
from paystream.db.models import OutboxEvent, Payment
from paystream.domain.errors import IdempotencyKeyReusedError
from paystream.domain.events import PAYMENT_CREATED, EventEnvelope, PaymentCreatedData
from paystream.domain.fingerprint import request_fingerprint
from paystream.domain.payment import Currency
from paystream.repositories.outbox import OutboxRepository
from paystream.repositories.payments import NewPayment, PaymentRepository
from paystream.repositories.webhook_attempts import WebhookAttemptRepository, WebhookSummary


@dataclass(frozen=True, slots=True)
class PaymentCommand:
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] | None
    webhook_url: str | None

    def fingerprint_payload(self) -> dict[str, Any]:
        return {
            "amount": f"{self.amount:.2f}",
            "currency": self.currency.value,
            "description": self.description,
            "metadata": self.metadata,
            "webhook_url": self.webhook_url,
        }


@dataclass(frozen=True, slots=True)
class CreateResult:
    payment: Payment
    replayed: bool


@dataclass(frozen=True, slots=True)
class PaymentView:
    payment: Payment
    webhook: WebhookSummary


def build_payment_created_event(payment: Payment, correlation_id: str | None) -> OutboxEvent:
    event_id = uuid7()
    data = PaymentCreatedData(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        webhook_url=payment.webhook_url,
    )
    envelope = EventEnvelope(
        event_id=event_id,
        event_type=PAYMENT_CREATED,
        occurred_at=payment.created_at,
        data=data.model_dump(mode="json"),
    )
    return OutboxEvent(
        id=event_id,
        aggregate_id=payment.id,
        event_type=PAYMENT_CREATED,
        payload=envelope.model_dump(mode="json"),
        headers={"correlation_id": correlation_id} if correlation_id else {},
    )


class PaymentService:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def create(
        self,
        command: PaymentCommand,
        *,
        idempotency_key: str,
        correlation_id: str | None = None,
    ) -> CreateResult:
        fingerprint = request_fingerprint(command.fingerprint_payload())
        async with self._db.sessionmaker() as session, session.begin():
            payments = PaymentRepository(session)
            payment = await payments.insert_if_absent(
                NewPayment(
                    id=uuid7(),
                    amount=command.amount,
                    currency=command.currency,
                    description=command.description,
                    metadata=command.metadata,
                    idempotency_key=idempotency_key,
                    request_hash=fingerprint,
                    webhook_url=command.webhook_url,
                )
            )
            if payment is None:
                existing = await payments.get_by_idempotency_key(idempotency_key)
                if existing is None:  # the conflicting row was deleted between the two statements
                    raise RuntimeError("idempotency conflict without a stored payment")
                if existing.request_hash != fingerprint:
                    raise IdempotencyKeyReusedError(idempotency_key)
                IDEMPOTENT_REPLAYS.inc()
                return CreateResult(existing, replayed=True)

            outbox = OutboxRepository(session)
            outbox.add(build_payment_created_event(payment, correlation_id))
            await outbox.notify()
        PAYMENTS_CREATED.labels(payment.currency.value).inc()
        return CreateResult(payment, replayed=False)

    async def get(self, payment_id: UUID) -> PaymentView | None:
        async with self._db.sessionmaker() as session:
            payment = await PaymentRepository(session).get(payment_id)
            if payment is None:
                return None
            summary = await WebhookAttemptRepository(session).summary(payment_id)
        return PaymentView(payment=payment, webhook=summary)
