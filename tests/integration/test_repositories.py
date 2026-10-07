import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid7

import pytest
from sqlalchemy import update

from paystream.db.database import Database
from paystream.db.models import OutboxEvent, Payment, WebhookAttempt
from paystream.domain.payment import Currency, PaymentStatus
from paystream.repositories.outbox import OutboxRepository
from paystream.repositories.payments import NewPayment, PaymentRepository
from paystream.repositories.webhook_attempts import WebhookAttemptRepository

pytestmark = pytest.mark.integration


def new_payment(key: str = "key-1", request_hash: str = "a" * 64) -> NewPayment:
    return NewPayment(
        id=uuid7(),
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Order #1",
        metadata={"order_id": 1},
        idempotency_key=key,
        request_hash=request_hash,
        webhook_url=None,
    )


async def insert(db: Database, new: NewPayment) -> Payment | None:
    async with db.sessionmaker() as session, session.begin():
        return await PaymentRepository(session).insert_if_absent(new)


async def test_insert_if_absent_returns_row_once(db: Database) -> None:
    first = await insert(db, new_payment())
    second = await insert(db, new_payment())
    assert first is not None
    assert first.status is PaymentStatus.PENDING
    assert first.created_at is not None
    assert first.metadata_ == {"order_id": 1}
    assert second is None


async def test_concurrent_inserts_with_same_key_create_one_row(db: Database) -> None:
    results = await asyncio.gather(*(insert(db, new_payment("same-key")) for _ in range(10)))
    assert sum(r is not None for r in results) == 1


async def test_complete_only_transitions_pending(db: Database) -> None:
    payment = await insert(db, new_payment())
    assert payment is not None
    async with db.sessionmaker() as session, session.begin():
        repo = PaymentRepository(session)
        assert await repo.complete(payment.id, status=PaymentStatus.SUCCEEDED, failure_reason=None)
        assert not await repo.complete(payment.id, status=PaymentStatus.FAILED, failure_reason="card_declined")
    async with db.sessionmaker() as session:
        stored = await PaymentRepository(session).get(payment.id)
    assert stored is not None
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None


async def test_complete_rejects_pending_status(db: Database) -> None:
    async with db.sessionmaker() as session, session.begin():
        with pytest.raises(ValueError, match="final"):
            await PaymentRepository(session).complete(uuid7(), status=PaymentStatus.PENDING, failure_reason=None)


async def test_mark_webhook_delivered(db: Database) -> None:
    payment = await insert(db, new_payment())
    assert payment is not None
    async with db.sessionmaker() as session, session.begin():
        await PaymentRepository(session).mark_webhook_delivered(payment.id)
    async with db.sessionmaker() as session:
        stored = await PaymentRepository(session).get(payment.id)
    assert stored is not None
    assert stored.webhook_delivered_at is not None


async def add_event(db: Database) -> OutboxEvent:
    event = OutboxEvent(id=uuid7(), aggregate_id=uuid7(), event_type="payment.created", payload={"k": "v"})
    async with db.sessionmaker() as session, session.begin():
        OutboxRepository(session).add(event)
    return event


async def test_claim_publish_and_retry_cycle(db: Database) -> None:
    published, retried = await add_event(db), await add_event(db)
    async with db.sessionmaker() as session, session.begin():
        repo = OutboxRepository(session)
        claimed = await repo.claim_due(10)
        assert [e.id for e in claimed] == [published.id, retried.id]
        await repo.mark_published([published.id])
        await repo.schedule_retry(retried.id, error="broker down", delay_seconds=60)
    async with db.sessionmaker() as session, session.begin():
        repo = OutboxRepository(session)
        assert await repo.claim_due(10) == []
        assert await repo.count_pending() == 1
        stored = await session.get(OutboxEvent, retried.id)
        assert stored is not None
        assert (stored.attempts, stored.last_error) == (1, "broker down")


async def test_claim_skips_rows_locked_by_another_relay(db: Database) -> None:
    await add_event(db)
    async with db.sessionmaker() as first, first.begin():
        assert len(await OutboxRepository(first).claim_due(10)) == 1
        async with db.sessionmaker() as second, second.begin():
            assert await OutboxRepository(second).claim_due(10) == []


async def test_delete_published_before(db: Database) -> None:
    event = await add_event(db)
    async with db.sessionmaker() as session, session.begin():
        await OutboxRepository(session).mark_published([event.id])
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event.id)
            .values(published_at=datetime.now(UTC) - timedelta(days=8))
        )
    async with db.sessionmaker() as session, session.begin():
        deleted = await OutboxRepository(session).delete_published_before(datetime.now(UTC) - timedelta(days=7))
    assert deleted == 1


async def test_webhook_summary(db: Database) -> None:
    payment = await insert(db, new_payment())
    assert payment is not None
    async with db.sessionmaker() as session, session.begin():
        repo = WebhookAttemptRepository(session)
        repo.add(
            WebhookAttempt(
                payment_id=payment.id, event_id="evt_1", attempt=1, status_code=500, error="HTTP 500", duration_ms=12
            )
        )
        repo.add(
            WebhookAttempt(
                payment_id=payment.id, event_id="evt_1", attempt=2, status_code=503, error="HTTP 503", duration_ms=10
            )
        )
    async with db.sessionmaker() as session:
        repo = WebhookAttemptRepository(session)
        assert await repo.count(payment.id) == 2
        summary = await repo.summary(payment.id)
    assert (summary.attempts, summary.last_status_code, summary.last_error) == (2, 503, "HTTP 503")
