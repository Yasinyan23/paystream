from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel

from paystream.domain.payment import Currency, PaymentStatus

PAYMENT_CREATED = "payment.created"
SCHEMA_VERSION = 1

_WEBHOOK_NAMESPACE = uuid5(NAMESPACE_URL, "https://github.com/Yasinyan23/paystream#webhooks")


class EventEnvelope(BaseModel):
    """Every message on the bus uses this envelope; `event_id` is also the AMQP message_id."""

    event_id: UUID
    event_type: str
    schema_version: int = SCHEMA_VERSION
    occurred_at: datetime
    data: dict[str, Any]


class PaymentCreatedData(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    webhook_url: str | None


def webhook_event_id(payment_id: UUID, status: PaymentStatus) -> str:
    """Stable across delivery retries, so receivers can deduplicate on `webhook-id`."""
    return f"evt_{uuid5(_WEBHOOK_NAMESPACE, f'{payment_id}:{status}').hex}"
