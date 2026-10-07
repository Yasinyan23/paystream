import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from faststream.rabbit import RabbitBroker

from paystream.core.metrics import OUTBOX_LAG_SECONDS, OUTBOX_PENDING
from paystream.core.telemetry import attached_context
from paystream.db.database import Database
from paystream.domain.backoff import exponential_backoff
from paystream.domain.events import PAYMENT_CREATED
from paystream.messaging.publisher import publish_json
from paystream.messaging.topology import PAYMENT_CREATED_KEY, PAYMENTS_EXCHANGE
from paystream.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

ROUTING_KEYS = {PAYMENT_CREATED: PAYMENT_CREATED_KEY}


@dataclass(frozen=True, slots=True)
class RelayStats:
    published: int
    failed: int


class OutboxRelay:
    """Moves outbox rows to RabbitMQ. At-least-once: a crash after publish and before commit re-publishes
    the same message_id, and the consumer is idempotent. An event is never marked failed and dropped."""

    def __init__(
        self,
        db: Database,
        broker: RabbitBroker,
        *,
        batch_size: int,
        max_backoff: float,
        rand: Callable[[], float] = random.random,
    ) -> None:
        self._db, self._broker = db, broker
        self._batch_size, self._max_backoff, self._rand = batch_size, max_backoff, rand

    async def publish_due(self) -> RelayStats:
        published: list[UUID] = []
        failed = 0
        async with self._db.sessionmaker() as session, session.begin():
            repo = OutboxRepository(session)
            for event in await repo.claim_due(self._batch_size):
                try:
                    # The broker's telemetry middleware parents its publish span on the current context: make
                    # that the API request's trace stored in the row.
                    with attached_context(event.headers):
                        await publish_json(
                            self._broker,
                            event.payload,
                            exchange=PAYMENTS_EXCHANGE,
                            routing_key=ROUTING_KEYS[event.event_type],
                            message_id=str(event.id),
                            correlation_id=event.headers.get("correlation_id"),
                            headers=event.headers,
                        )
                except Exception as exc:  # noqa: BLE001 — any publish failure is retried later
                    failed += 1
                    delay = exponential_backoff(event.attempts + 1, base=1.0, cap=self._max_backoff, rand=self._rand)
                    await repo.schedule_retry(event.id, error=f"{type(exc).__name__}: {exc}", delay_seconds=delay)
                    logger.warning("outbox publish failed for %s, retry in %.1fs: %s", event.id, delay, exc)
                    # The broker is probably down: stop here, the rest of the batch stays pending.
                    break
                OUTBOX_LAG_SECONDS.observe((datetime.now(UTC) - event.created_at).total_seconds())
                published.append(event.id)
            await repo.mark_published(published)
        if published:
            logger.info("outbox published %d event(s)", len(published))
        return RelayStats(published=len(published), failed=failed)

    async def drain(self) -> int:
        """Publish until the queue of due events is empty or a publish fails."""
        total = 0
        while True:
            stats = await self.publish_due()
            total += stats.published
            if stats.failed or stats.published < self._batch_size:
                return total

    async def refresh_pending_gauge(self) -> None:
        async with self._db.sessionmaker() as session:
            OUTBOX_PENDING.set(await OutboxRepository(session).count_pending())

    async def purge_published(self, older_than: timedelta) -> int:
        async with self._db.sessionmaker() as session, session.begin():
            return await OutboxRepository(session).delete_published_before(datetime.now(UTC) - older_than)
