from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import UUID

from sqlalchemy import CursorResult, delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from paystream.db.models import OutboxEvent
from paystream.domain.payment import OutboxStatus

OUTBOX_NOTIFY_CHANNEL = "paystream_outbox"


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def notify(self) -> None:
        """Wake the relay. Postgres delivers NOTIFY only when the surrounding transaction commits."""
        await self._session.execute(text("SELECT pg_notify(:channel, '')"), {"channel": OUTBOX_NOTIFY_CHANNEL})

    async def claim_due(self, limit: int) -> list[OutboxEvent]:
        """Lock due events; rows locked by another relay replica are skipped, not waited for."""
        statement = (
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PENDING, OutboxEvent.next_attempt_at <= func.now())
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(await self._session.scalars(statement))

    async def mark_published(self, event_ids: Sequence[UUID]) -> None:
        if not event_ids:
            return
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(event_ids))
            .values(status=OutboxStatus.PUBLISHED, published_at=func.now(), last_error=None)
            .execution_options(synchronize_session=False)
        )

    async def schedule_retry(self, event_id: UUID, *, error: str, delay_seconds: float) -> None:
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                attempts=OutboxEvent.attempts + 1,
                last_error=error[:1000],
                next_attempt_at=func.now() + timedelta(seconds=delay_seconds),
            )
            .execution_options(synchronize_session=False)
        )

    async def delete_published_before(self, cutoff: datetime) -> int:
        result = await self._session.execute(
            delete(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PUBLISHED, OutboxEvent.published_at < cutoff)
            .execution_options(synchronize_session=False)
        )
        return cast(CursorResult[Any], result).rowcount

    async def count_pending(self) -> int:
        statement = select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == OutboxStatus.PENDING)
        return int(await self._session.scalar(statement) or 0)
