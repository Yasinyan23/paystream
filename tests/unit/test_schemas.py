from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid7

import pytest
from pydantic import ValidationError

from paystream.api.schemas import PaymentCreateRequest, PaymentDetail
from paystream.db.models import Payment
from paystream.domain.payment import Currency, PaymentStatus
from paystream.repositories.webhook_attempts import WebhookSummary
from paystream.services.payments import PaymentView

NOW = datetime(2026, 10, 7, tzinfo=UTC)


def test_amount_is_normalised_to_two_decimals() -> None:
    command = PaymentCreateRequest.model_validate({"amount": 100, "currency": "RUB"}).to_command()
    assert command.amount == Decimal("100.00")
    assert command.fingerprint_payload()["amount"] == "100.00"


def test_same_payment_in_different_forms_has_same_fingerprint_payload() -> None:
    a = PaymentCreateRequest.model_validate({"amount": "100", "currency": "RUB"}).to_command()
    b = PaymentCreateRequest.model_validate({"amount": 100.00, "currency": "RUB"}).to_command()
    assert a.fingerprint_payload() == b.fingerprint_payload()


@pytest.mark.parametrize(
    "body",
    [
        {"amount": 0, "currency": "RUB"},
        {"amount": -1, "currency": "RUB"},
        {"amount": "1.001", "currency": "RUB"},
        {"amount": 10, "currency": "GBP"},
        {"amount": 10, "currency": "RUB", "unexpected": True},
        {"amount": 10, "currency": "RUB", "webhook_url": "ftp://example.com"},
        {"amount": 10, "currency": "RUB", "description": "x" * 501},
        {"amount": 10, "currency": "RUB", "metadata": {"blob": "x" * 17_000}},
    ],
)
def test_invalid_bodies_are_rejected(body: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(body)


@pytest.mark.parametrize(
    ("webhook_url", "delivered_at", "attempts", "state"),
    [
        (None, None, 0, "not_configured"),
        ("https://hooks.test/p", None, 0, "pending"),
        ("https://hooks.test/p", None, 2, "failing"),
        ("https://hooks.test/p", NOW, 3, "delivered"),
    ],
)
def test_webhook_state_follows_url_attempts_and_delivery(
    webhook_url: str | None, delivered_at: datetime | None, attempts: int, state: str
) -> None:
    payment = Payment(
        id=uuid7(),
        amount=Decimal("1.00"),
        currency=Currency.RUB,
        description=None,
        metadata_=None,
        status=PaymentStatus.PENDING,
        failure_reason=None,
        idempotency_key="k",
        request_hash="h",
        webhook_url=webhook_url,
        webhook_delivered_at=delivered_at,
        created_at=NOW,
        processed_at=None,
    )
    summary = WebhookSummary(attempts=attempts, last_status_code=None, last_error=None)
    assert PaymentDetail.from_view(PaymentView(payment, summary)).webhook.state == state
