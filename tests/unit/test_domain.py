from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from paystream.domain.backoff import exponential_backoff
from paystream.domain.events import PAYMENT_CREATED, EventEnvelope, PaymentCreatedData, webhook_event_id
from paystream.domain.fingerprint import request_fingerprint
from paystream.domain.payment import Currency, PaymentStatus

PAYMENT_ID = UUID("0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")


def test_final_statuses() -> None:
    assert not PaymentStatus.PENDING.is_final
    assert PaymentStatus.SUCCEEDED.is_final
    assert PaymentStatus.FAILED.is_final


def test_fingerprint_ignores_key_order() -> None:
    a = request_fingerprint({"amount": "10.00", "currency": "RUB", "metadata": {"x": 1, "y": 2}})
    b = request_fingerprint({"metadata": {"y": 2, "x": 1}, "currency": "RUB", "amount": "10.00"})
    assert a == b
    assert len(a) == 64


def test_fingerprint_changes_with_body() -> None:
    assert request_fingerprint({"amount": "10.00"}) != request_fingerprint({"amount": "10.01"})


@pytest.mark.parametrize(("attempt", "expected"), [(1, 1.0), (2, 2.0), (3, 4.0), (10, 60.0)])
def test_backoff_without_jitter(attempt: int, expected: float) -> None:
    assert exponential_backoff(attempt, base=1.0, cap=60.0, jitter=0.0) == expected


def test_backoff_jitter_stays_within_bounds() -> None:
    low = exponential_backoff(3, base=1.0, cap=60.0, jitter=0.2, rand=lambda: 0.0)
    high = exponential_backoff(3, base=1.0, cap=60.0, jitter=0.2, rand=lambda: 1.0)
    assert (low, high) == (pytest.approx(3.2), pytest.approx(4.8))


def test_backoff_rejects_attempt_zero() -> None:
    with pytest.raises(ValueError, match="attempt"):
        exponential_backoff(0, base=1.0, cap=60.0)


def test_envelope_roundtrip() -> None:
    data = PaymentCreatedData(payment_id=PAYMENT_ID, amount=Decimal("10.50"), currency=Currency.RUB, webhook_url=None)
    envelope = EventEnvelope(
        event_id=PAYMENT_ID,
        event_type=PAYMENT_CREATED,
        occurred_at=datetime(2026, 10, 6, tzinfo=UTC),
        data=data.model_dump(mode="json"),
    )
    restored = EventEnvelope.model_validate(envelope.model_dump(mode="json"))
    assert restored.schema_version == 1
    assert PaymentCreatedData.model_validate(restored.data).amount == Decimal("10.50")


def test_webhook_event_id_is_stable() -> None:
    first = webhook_event_id(PAYMENT_ID, PaymentStatus.SUCCEEDED)
    assert first == webhook_event_id(PAYMENT_ID, PaymentStatus.SUCCEEDED)


def test_webhook_event_id_is_status_specific() -> None:
    first = webhook_event_id(PAYMENT_ID, PaymentStatus.SUCCEEDED)
    assert first != webhook_event_id(PAYMENT_ID, PaymentStatus.FAILED)


def test_webhook_event_id_format() -> None:
    event_id = webhook_event_id(PAYMENT_ID, PaymentStatus.SUCCEEDED)
    assert event_id.startswith("evt_")
    assert len(event_id) == 36
