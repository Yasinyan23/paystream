import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid7

import aio_pika
import httpx
import pytest
import respx
from faststream import TestApp

from paystream.core.config import Settings
from paystream.db.database import Database
from paystream.domain.events import PAYMENT_CREATED, EventEnvelope
from paystream.domain.payment import Currency, PaymentStatus
from paystream.gateway.simulated import ChargeResult
from paystream.messaging.broker import build_broker
from paystream.messaging.retry import RetryPublisher
from paystream.messaging.topology import declare_topology
from paystream.repositories.payments import PaymentRepository
from paystream.services.outbox_relay import OutboxRelay
from paystream.services.payments import PaymentCommand, PaymentService
from paystream.workers.consumer import build_consumer
from tests.conftest import sample

pytestmark = pytest.mark.integration
HOOK = "https://hooks.test/payments"
QUEUES = ("payments.new", "payments.retry.300ms", "payments.retry.600ms", "payments.dead")  # settings fixture tiers


class AlwaysSucceeds:
    def __init__(self, delay: float = 0.0) -> None:
        self.delay = delay

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        await asyncio.sleep(self.delay)
        return ChargeResult(True)


@contextlib.asynccontextmanager
async def channel_to(url: str) -> AsyncIterator[aio_pika.abc.AbstractChannel]:
    connection = await aio_pika.connect(url)
    async with connection:
        yield await connection.channel()


async def purge_queues(url: str) -> None:
    async with channel_to(url) as channel:
        # Retry tiers before payments.new: a message parked in a tier could otherwise expire into it after its purge.
        for name in reversed(QUEUES):
            await (await channel.declare_queue(name, passive=True)).purge()


async def queue_depths(url: str) -> dict[str, int]:
    """Ready messages per queue."""
    async with channel_to(url) as channel:
        return {
            name: (await channel.declare_queue(name, passive=True)).declaration_result.message_count for name in QUEUES
        }


def depths(dead: int = 0) -> dict[str, int]:
    """Every queue empty but the DLQ: whatever was delivered got acked and nothing waits in a retry tier."""
    return dict.fromkeys(QUEUES, 0) | {"payments.dead": dead}


async def dead_letters(url: str) -> list[aio_pika.abc.AbstractIncomingMessage]:
    messages = []
    async with channel_to(url) as channel:
        queue = await channel.declare_queue("payments.dead", passive=True)
        while (message := await queue.get(fail=False)) is not None:
            messages.append(message)
            await message.ack()
    return messages


async def publish_raw(
    url: str,
    body: bytes,
    *,
    message_id: str,
    correlation_id: str | None = None,
    headers: dict[str, Any] | None = None,
    content_type: str | None = None,
) -> None:
    """Publish the way an outside producer would: straight to the exchange, not through our own publisher."""
    async with channel_to(url) as channel:
        exchange = await channel.get_exchange("payments")
        message = aio_pika.Message(
            body, message_id=message_id, correlation_id=correlation_id, headers=headers, content_type=content_type
        )
        await exchange.publish(message, routing_key="payment.created")


def event_bytes(**data: Any) -> bytes:
    """A payment.created envelope as an outside producer would send it, with `data` as given."""
    event = EventEnvelope(event_id=uuid7(), event_type=PAYMENT_CREATED, occurred_at=datetime.now(UTC), data=data)
    return event.model_dump_json().encode()


@pytest.fixture
def broker_settings(settings: Settings, rabbitmq_url: str) -> Settings:
    return settings.model_copy(update={"rabbitmq_url": rabbitmq_url})


@pytest.fixture(autouse=True)
async def clean_queues(broker_settings: Settings) -> AsyncIterator[None]:
    broker = build_broker(broker_settings)
    await broker.connect()
    await declare_topology(broker, broker_settings)
    await broker.stop()
    await purge_queues(broker_settings.rabbitmq_url)
    yield
    # test_topology.py runs later in the same session and takes the first message in payments.new
    await purge_queues(broker_settings.rabbitmq_url)


async def create_payment(db: Database, *, webhook_url: str | None = HOOK) -> UUID:
    command = PaymentCommand(
        amount=Decimal("10.00"), currency=Currency.EUR, description=None, metadata=None, webhook_url=webhook_url
    )
    return (await PaymentService(db).create(command, idempotency_key=str(uuid7()))).payment.id


async def publish_outbox(db: Database, settings: Settings) -> None:
    broker = build_broker(settings)
    await broker.connect()
    try:
        await OutboxRelay(db, broker, batch_size=100, max_backoff=60).drain()
    finally:
        await broker.stop()


async def create_and_publish(db: Database, settings: Settings, *, webhook_url: str | None = HOOK) -> UUID:
    payment_id = await create_payment(db, webhook_url=webhook_url)
    await publish_outbox(db, settings)
    return payment_id


async def wait_for(predicate: Callable[[], Awaitable[bool]], within: float = 15.0) -> None:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("condition not met in time")


async def payment_status(db: Database, payment_id: UUID) -> PaymentStatus | None:
    async with db.sessionmaker() as session:
        payment = await PaymentRepository(session).get(payment_id)
    return payment.status if payment else None


async def _is(db: Database, payment_id: UUID, status: PaymentStatus) -> bool:
    return await payment_status(db, payment_id) is status


async def _calls_at_least(route: respx.Route, count: int) -> bool:
    return route.call_count >= count


async def _dead_letters_at_least(url: str, count: int) -> bool:
    return (await queue_depths(url))["payments.dead"] >= count


@respx.mock
async def test_end_to_end_success(db: Database, broker_settings: Settings) -> None:
    route = respx.post(HOOK).mock(return_value=httpx.Response(200))
    received = {"app_name": "paystream-consumer", "broker": "rabbitmq", "handler": "payments.payment.created"}
    received_before = sample("faststream_received_messages_total", **received)
    async with httpx.AsyncClient() as http:
        app = build_consumer(broker_settings, gateway=AlwaysSucceeds(), http_client=http)
        async with TestApp(app):
            payment_id = await create_and_publish(db, broker_settings)
            await wait_for(lambda: _is(db, payment_id, PaymentStatus.SUCCEEDED))
            await wait_for(lambda: _calls_at_least(route, 1))
    assert await queue_depths(broker_settings.rabbitmq_url) == depths()
    assert sample("faststream_received_messages_total", **received) == received_before + 1  # FastStream's middleware


@respx.mock
async def test_webhook_retries_with_backoff_then_dead_letters(db: Database, broker_settings: Settings) -> None:
    url = broker_settings.rabbitmq_url
    route = respx.post(HOOK).mock(return_value=httpx.Response(503))
    dead_before = sample("paystream_messages_dead_lettered_total", reason="max_attempts_exceeded")
    retried_before = {delay: sample("paystream_messages_retried_total", delay_ms=delay) for delay in ("300", "600")}
    async with httpx.AsyncClient() as http:
        app = build_consumer(broker_settings, gateway=AlwaysSucceeds(), http_client=http)
        async with TestApp(app):
            started = time.monotonic()
            await create_and_publish(db, broker_settings)
            await wait_for(lambda: _dead_letters_at_least(url, 1))
            elapsed = time.monotonic() - started
    assert route.call_count == 3  # RETRY_MAX_ATTEMPTS
    assert elapsed >= 0.9  # 300 ms + 600 ms of retry delay (settings fixture)
    assert await queue_depths(url) == depths(dead=1)
    [message] = await dead_letters(url)
    assert message.headers["x-dead-reason"] == "max_attempts_exceeded"
    assert message.headers["x-attempt"] == 3
    assert "HTTP 503" in str(message.headers["x-error"])
    assert "x-death" not in message.headers  # RabbitMQ's bookkeeping from the retry hops is not carried along
    assert sample("paystream_messages_dead_lettered_total", reason="max_attempts_exceeded") == dead_before + 1
    for delay, before in retried_before.items():  # one hop through each retry tier
        assert sample("paystream_messages_retried_total", delay_ms=delay) == before + 1


@pytest.mark.parametrize(
    ("body", "content_type"),
    [
        (b'{"garbage": true}', None),
        (b"not json", None),
        (b"[1, 2, 3]", None),
        (b"{not json", "application/json"),  # claims to be JSON: FastStream's own decoder raises before our handler
        (b"\xff\xfe", "text/plain"),  # claims to be text: so does its decoder
        (b"[" * 100_000, None),  # json.loads dies with a RecursionError, which nobody upstream catches either
        (b"[" * 100_000, "application/json"),
    ],
    ids=[
        "not-an-event",
        "not-json",
        "not-an-object",
        "undecodable-json",
        "undecodable-text",
        "too-deep",
        "too-deep-json",
    ],
)
async def test_invalid_message_goes_to_dlq(broker_settings: Settings, body: bytes, content_type: str | None) -> None:
    url = broker_settings.rabbitmq_url
    async with httpx.AsyncClient() as http:
        app = build_consumer(broker_settings, gateway=AlwaysSucceeds(), http_client=http)
        async with TestApp(app):
            await publish_raw(url, body, message_id="bad-1", correlation_id="corr-1", content_type=content_type)
            await wait_for(lambda: _dead_letters_at_least(url, 1))
    assert await queue_depths(url) == depths(dead=1)
    [message] = await dead_letters(url)
    assert (message.message_id, message.correlation_id) == ("bad-1", "corr-1")
    assert message.headers["x-dead-reason"] == "invalid_message"
    assert message.headers["x-attempt"] == 1  # the first delivery: no hop through a retry tier


async def test_malformed_event_data_goes_straight_to_dlq(broker_settings: Settings) -> None:
    url = broker_settings.rabbitmq_url
    async with httpx.AsyncClient() as http:
        app = build_consumer(broker_settings, gateway=AlwaysSucceeds(), http_client=http)
        async with TestApp(app):
            # A valid envelope whose `data` lacks the payment_id: the processor calls it a permanent failure.
            await publish_raw(url, event_bytes(amount="10.00"), message_id="bad-2")
            await wait_for(lambda: _dead_letters_at_least(url, 1))
    assert await queue_depths(url) == depths(dead=1)
    [message] = await dead_letters(url)
    assert message.headers["x-dead-reason"] == "invalid_event"
    assert message.headers["x-attempt"] == 1  # no hop through a retry tier
    assert "x-last-error" not in message.headers  # which would have stamped one


@pytest.mark.parametrize(
    "attempt",
    ["abc", None, [1, 2]],  # int() fails with a ValueError, and with a TypeError for a void and for an array
    ids=["text", "void", "array"],
)
async def test_unreadable_attempt_header_goes_straight_to_dlq(broker_settings: Settings, attempt: Any) -> None:
    url = broker_settings.rabbitmq_url
    # The event itself is fine (the processor would call it an unknown payment, a different dead-letter reason),
    # so the header is the only thing that can make it invalid.
    body = event_bytes(payment_id=str(uuid7()), amount="10.00", currency="EUR", webhook_url=None)
    async with httpx.AsyncClient() as http:
        app = build_consumer(broker_settings, gateway=AlwaysSucceeds(), http_client=http)
        async with TestApp(app):
            await publish_raw(url, body, message_id="bad-3", headers={"x-attempt": attempt})
            await wait_for(lambda: _dead_letters_at_least(url, 1))
    assert await queue_depths(url) == depths(dead=1)
    [message] = await dead_letters(url)
    assert message.headers["x-dead-reason"] == "invalid_message"
    assert str(message.headers["x-error"]).startswith("unreadable x-attempt header")  # the bad value is quoted there


async def test_messages_are_processed_concurrently(db: Database, broker_settings: Settings) -> None:
    async with httpx.AsyncClient() as http:
        app = build_consumer(broker_settings, gateway=AlwaysSucceeds(delay=1.0), http_client=http)
        async with TestApp(app):
            ids = [await create_payment(db, webhook_url=None) for _ in range(5)]
            started = time.monotonic()
            await publish_outbox(db, broker_settings)  # all five land in the queue at once
            for payment_id in ids:
                await wait_for(lambda pid=payment_id: _is(db, pid, PaymentStatus.SUCCEEDED))
            elapsed = time.monotonic() - started
    assert elapsed < 3.0, f"5 payments x 1s took {elapsed:.1f}s: handler is not concurrent"


async def test_crash_loop_is_dead_lettered_at_the_delivery_limit(broker_settings: Settings) -> None:
    url = broker_settings.rabbitmq_url
    await publish_raw(url, b"{}", message_id="loop-1")
    async with channel_to(url) as channel:
        queue = await channel.declare_queue("payments.new", passive=True)
        for _ in range(broker_settings.delivery_limit + 1):  # a consumer that keeps giving the message back
            message = await queue.get(timeout=5)
            await message.reject(requeue=True)
    await wait_for(lambda: _dead_letters_at_least(url, 1))
    assert await queue_depths(url) == depths(dead=1)
    [message] = await dead_letters(url)
    assert message.message_id == "loop-1"
    assert message.headers["x-death"][0]["reason"] == "delivery_limit"


async def test_message_that_cannot_be_dead_lettered_is_requeued_until_the_delivery_limit(
    broker_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def broker_is_down(self: RetryPublisher, **_: Any) -> None:
        raise ConnectionError("broker is down")

    monkeypatch.setattr(RetryPublisher, "dead_letter", broker_is_down)
    url = broker_settings.rabbitmq_url
    async with httpx.AsyncClient() as http:
        app = build_consumer(broker_settings, gateway=AlwaysSucceeds(), http_client=http)
        async with TestApp(app):
            await publish_raw(url, b"not json", message_id="loop-2")  # invalid, so the consumer wants to dead-letter it
            await wait_for(lambda: _dead_letters_at_least(url, 1))
    assert await queue_depths(url) == depths(dead=1)
    [message] = await dead_letters(url)
    assert message.message_id == "loop-2"
    # The broker gave up on it, not the consumer: every requeue was a reject, which the limit counts.
    assert message.headers["x-death"][0]["reason"] == "delivery_limit"
    assert "x-dead-reason" not in message.headers


async def test_consumer_refuses_to_start_without_a_signing_secret(broker_settings: Settings) -> None:
    with pytest.raises(RuntimeError, match="WEBHOOK_SIGNING_SECRET"):
        build_consumer(broker_settings.model_copy(update={"webhook_signing_secret": None}))
