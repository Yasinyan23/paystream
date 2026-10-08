import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from uuid import uuid7

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select, text

from paystream.core.config import Settings
from paystream.db.database import Database
from paystream.db.models import OutboxEvent
from paystream.domain.payment import OutboxStatus
from paystream.messaging.broker import build_broker
from paystream.messaging.topology import declare_topology
from paystream.repositories.outbox import OUTBOX_NOTIFY_CHANNEL, OutboxRepository
from paystream.services.outbox_relay import OutboxRelay
from paystream.workers.relay import run_relay
from tests.conftest import sample

pytestmark = pytest.mark.integration


async def add_events(db: Database, count: int, *, notify: bool = False) -> list[OutboxEvent]:
    events = [
        OutboxEvent(
            id=uuid7(),
            aggregate_id=uuid7(),
            event_type="payment.created",
            payload={"n": n},
            headers={"correlation_id": f"corr-{n}"},
        )
        for n in range(count)
    ]
    async with db.sessionmaker() as session, session.begin():
        repo = OutboxRepository(session)
        for event in events:
            repo.add(event)
        if notify:
            await repo.notify()
    return events


async def purge_queue(rabbitmq_url: str) -> None:
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("payments.new", passive=True)
        await queue.purge()


async def queued_message_ids(rabbitmq_url: str, expected: int) -> list[str]:
    connection = await aio_pika.connect(rabbitmq_url)
    ids: list[str] = []
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("payments.new", passive=True)
        for _ in range(expected):
            message = await queue.get(timeout=5)
            assert message is not None
            ids.append(str(message.message_id))
            await message.ack()
    return ids


@pytest.fixture
async def broker(settings: Settings, rabbitmq_url: str) -> AsyncIterator[RabbitBroker]:
    settings = settings.model_copy(update={"rabbitmq_url": rabbitmq_url})
    broker = build_broker(settings)
    await broker.connect()
    await declare_topology(broker, settings)
    await purge_queue(rabbitmq_url)
    yield broker
    await broker.stop()


async def test_publishes_due_events_with_event_id_as_message_id(
    db: Database, broker: RabbitBroker, rabbitmq_url: str
) -> None:
    events = await add_events(db, 3)
    relay = OutboxRelay(db, broker, batch_size=100, max_backoff=60)
    lag_samples_before = sample("paystream_outbox_publish_lag_seconds_count")
    stats = await relay.publish_due()
    assert (stats.published, stats.failed) == (3, 0)
    assert await queued_message_ids(rabbitmq_url, 3) == [str(e.id) for e in events]
    async with db.sessionmaker() as session:
        statuses = set(await session.scalars(select(OutboxEvent.status)))
    assert statuses == {OutboxStatus.PUBLISHED}
    assert sample("paystream_outbox_publish_lag_seconds_count") == lag_samples_before + 3


async def test_pending_gauge_counts_unpublished_events(db: Database, broker: RabbitBroker) -> None:
    await add_events(db, 2)
    relay = OutboxRelay(db, broker, batch_size=100, max_backoff=60)
    await relay.refresh_pending_gauge()
    assert sample("paystream_outbox_pending_events") == 2
    await relay.publish_due()
    await relay.refresh_pending_gauge()
    assert sample("paystream_outbox_pending_events") == 0


async def test_drain_processes_more_than_one_batch(db: Database, broker: RabbitBroker, rabbitmq_url: str) -> None:
    await add_events(db, 5)
    relay = OutboxRelay(db, broker, batch_size=2, max_backoff=60)
    assert await relay.drain() == 5
    assert len(await queued_message_ids(rabbitmq_url, 5)) == 5


async def test_publish_failure_schedules_retry_and_keeps_event(db: Database, settings: Settings) -> None:
    await add_events(db, 2)
    dead_broker = build_broker(settings)  # points at 127.0.0.1:1, publish fails
    relay = OutboxRelay(db, dead_broker, batch_size=100, max_backoff=60, rand=lambda: 0.5)
    stats = await relay.publish_due()
    assert (stats.published, stats.failed) == (0, 1)  # stops at the first failure
    async with db.sessionmaker() as session:
        events = list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id)))
    assert [e.status for e in events] == [OutboxStatus.PENDING, OutboxStatus.PENDING]
    assert events[0].attempts == 1
    assert events[0].last_error
    assert events[1].attempts == 0


async def test_purge_published(db: Database, broker: RabbitBroker, rabbitmq_url: str) -> None:
    await add_events(db, 1)
    relay = OutboxRelay(db, broker, batch_size=100, max_backoff=60)
    await relay.publish_due()
    await queued_message_ids(rabbitmq_url, 1)
    assert await relay.purge_published(older_than=timedelta(seconds=-1)) == 1


async def listening(db: Database) -> bool:
    """Some session's last statement was the relay's LISTEN; it runs once the relay is connected to RabbitMQ too."""
    async with db.engine.connect() as connection:
        statement = text("SELECT count(*) FROM pg_stat_activity WHERE query = :listen")
        return bool(await connection.scalar(statement, {"listen": f'LISTEN "{OUTBOX_NOTIFY_CHANNEL}"'}))


async def test_run_relay_wakes_up_on_notify(db: Database, settings: Settings, rabbitmq_url: str) -> None:
    settings = settings.model_copy(update={"rabbitmq_url": rabbitmq_url, "outbox_poll_interval": 30.0})
    stop = asyncio.Event()
    task = asyncio.create_task(run_relay(settings, stop))
    try:  # whatever fails, no relay may outlive the test in the session's event loop
        async with asyncio.timeout(10):
            while True:
                if await listening(db):
                    break
                await asyncio.sleep(0.05)
        await purge_queue(rabbitmq_url)
        await add_events(db, 1, notify=True)  # the relay's first pass over the outbox, right after LISTEN, may take it
        await asyncio.wait_for(queued_message_ids(rabbitmq_url, 1), timeout=5)
        events = await add_events(db, 1, notify=True)  # that pass is over: only the NOTIFY can beat the 30 s poll
        ids = await asyncio.wait_for(queued_message_ids(rabbitmq_url, 1), timeout=5)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)
    assert ids == [str(events[0].id)]
