import json
import math
import re
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from paystream.domain.payment import Currency, PaymentStatus
from paystream.services.payments import PaymentCommand, PaymentView

_MAX_METADATA_BYTES = 16 * 1024
_MAX_METADATA_DEPTH = 32  # pydantic serializes 255 levels at most: deeper metadata would be stored, never read back
_UNSTORABLE_TEXT = re.compile(r"[\x00\ud800-\udfff]")  # NUL: Postgres text/jsonb; lone surrogate: not UTF-8


def _check_storable(value: object, depth: int = 1) -> None:
    """Reject what Postgres or JSON cannot carry (NUL, lone surrogates, NaN, Infinity) and over-deep nesting.

    The client gets a 422 instead of a 500 from deeper down; recursion is bounded by `_MAX_METADATA_DEPTH`.
    """
    if isinstance(value, str):
        if _UNSTORABLE_TEXT.search(value):
            raise ValueError("must not contain NUL or lone surrogate characters")
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("must not contain NaN or Infinity")
    elif isinstance(value, dict | list):
        if depth > _MAX_METADATA_DEPTH:
            raise ValueError(f"must be nested at most {_MAX_METADATA_DEPTH} levels deep")
        for child in [*value, *value.values()] if isinstance(value, dict) else value:  # keys, then values
            _check_storable(child, depth + 1)


class PaymentCreateRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "1500.00",
                    "currency": "RUB",
                    "description": "Order #42",
                    "metadata": {"order_id": 42},
                    "webhook_url": "https://example.com/webhooks/payments",
                }
            ]
        },
    )

    # `ge` a cent, not `gt=0`: the digit checks miss exponents beyond -10^7, and such an amount would round to 0.00
    amount: Decimal = Field(ge=Decimal("0.01"), max_digits=18, decimal_places=2)
    currency: Currency
    description: str | None = Field(default=None, max_length=500)
    metadata: dict[str, Any] | None = None
    webhook_url: HttpUrl | None = None

    @field_validator("description", "metadata")
    @classmethod
    def _storable[T](cls, value: T) -> T:
        _check_storable(value)
        return value

    @field_validator("metadata")
    @classmethod
    def _limit_metadata(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is not None and len(json.dumps(value).encode()) > _MAX_METADATA_BYTES:
            raise ValueError("metadata must be at most 16 KB when serialized")
        return value

    def to_command(self) -> PaymentCommand:
        return PaymentCommand(
            amount=self.amount.quantize(Decimal("0.01")),
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            webhook_url=str(self.webhook_url) if self.webhook_url else None,
        )


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class WebhookState(StrEnum):
    NOT_CONFIGURED = "not_configured"
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILING = "failing"


class WebhookInfo(BaseModel):
    state: WebhookState
    attempts: int
    last_status_code: int | None
    last_error: str | None
    delivered_at: datetime | None


class PaymentDetail(BaseModel):
    id: UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] | None
    status: PaymentStatus
    failure_reason: str | None
    idempotency_key: str
    webhook_url: str | None
    webhook: WebhookInfo
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_view(cls, view: PaymentView) -> Self:
        payment, summary = view.payment, view.webhook
        if payment.webhook_url is None:
            state = WebhookState.NOT_CONFIGURED
        elif payment.webhook_delivered_at is not None:
            state = WebhookState.DELIVERED
        elif summary.attempts > 0:
            state = WebhookState.FAILING
        else:
            state = WebhookState.PENDING
        return cls(
            id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            failure_reason=payment.failure_reason,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            webhook=WebhookInfo(
                state=state,
                attempts=summary.attempts,
                last_status_code=summary.last_status_code,
                last_error=summary.last_error,
                delivered_at=payment.webhook_delivered_at,
            ),
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
