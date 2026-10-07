from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from paystream.db.models import WebhookAttempt


@dataclass(frozen=True, slots=True)
class WebhookSummary:
    attempts: int
    last_status_code: int | None
    last_error: str | None


class WebhookAttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, attempt: WebhookAttempt) -> None:
        self._session.add(attempt)

    async def count(self, payment_id: UUID) -> int:
        statement = select(func.count()).select_from(WebhookAttempt).where(WebhookAttempt.payment_id == payment_id)
        return int(await self._session.scalar(statement) or 0)

    async def summary(self, payment_id: UUID) -> WebhookSummary:
        last = await self._session.scalar(
            select(WebhookAttempt)
            .where(WebhookAttempt.payment_id == payment_id)
            .order_by(WebhookAttempt.id.desc())
            .limit(1)
        )
        return WebhookSummary(
            attempts=await self.count(payment_id),
            last_status_code=last.status_code if last else None,
            last_error=last.error if last else None,
        )
