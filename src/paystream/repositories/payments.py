from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

from sqlalchemy import CursorResult, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from paystream.db.models import Payment
from paystream.domain.payment import Currency, PaymentStatus


@dataclass(frozen=True, slots=True)
class NewPayment:
    id: UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] | None
    idempotency_key: str
    request_hash: str
    webhook_url: str | None


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, new: NewPayment) -> Payment | None:
        """Insert unless the idempotency key exists. The unique index arbitrates concurrent requests."""
        statement = (
            insert(Payment)
            .values(
                {
                    Payment.id: new.id,
                    Payment.amount: new.amount,
                    Payment.currency: new.currency,
                    Payment.description: new.description,
                    Payment.metadata_: new.metadata,
                    Payment.idempotency_key: new.idempotency_key,
                    Payment.request_hash: new.request_hash,
                    Payment.webhook_url: new.webhook_url,
                }
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self._session.scalars(statement)).one_or_none()

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return await self._session.scalar(select(Payment).where(Payment.idempotency_key == key))

    async def complete(self, payment_id: UUID, *, status: PaymentStatus, failure_reason: str | None) -> bool:
        """Move pending → final exactly once. Returns False if the payment was already final or does not exist."""
        if not status.is_final:
            raise ValueError(f"status must be final, got {status}")
        result = await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, failure_reason=failure_reason, processed_at=func.now())
            .execution_options(synchronize_session=False)
        )
        return cast(CursorResult[Any], result).rowcount == 1

    async def mark_webhook_delivered(self, payment_id: UUID) -> None:
        await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=func.now())
            .execution_options(synchronize_session=False)
        )
