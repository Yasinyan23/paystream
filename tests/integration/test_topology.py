import aio_pika
import pytest

from paystream.core.config import Settings
from paystream.messaging.broker import build_broker
from paystream.messaging.publisher import publish_json
from paystream.messaging.topology import PAYMENT_CREATED_KEY, PAYMENTS_EXCHANGE, declare_topology, payments_queue

pytestmark = pytest.mark.integration


async def test_topology_declares_all_queues(settings: Settings, rabbitmq_url: str) -> None:
    settings = settings.model_copy(update={"rabbitmq_url": rabbitmq_url})
    broker = build_broker(settings)
    await broker.connect()
    try:
        await declare_topology(broker, settings)
        await declare_topology(broker, settings)  # idempotent
    finally:
        await broker.stop()

    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        for name in ("payments.new", "payments.retry.300ms", "payments.retry.600ms", "payments.dead"):
            await channel.declare_queue(name, passive=True)


async def test_publish_is_routed_to_payments_new(settings: Settings, rabbitmq_url: str) -> None:
    settings = settings.model_copy(update={"rabbitmq_url": rabbitmq_url})
    broker = build_broker(settings)
    await broker.connect()
    try:
        await declare_topology(broker, settings)
        # The message is read back as the first in the queue: anything another module left there goes first.
        await (await broker.declare_queue(payments_queue(settings.delivery_limit))).purge()
        await publish_json(
            broker,
            {"hello": "world"},
            exchange=PAYMENTS_EXCHANGE,
            routing_key=PAYMENT_CREATED_KEY,
            message_id="msg-1",
            correlation_id="corr-1",
            headers={"x-attempt": 1},
        )
    finally:
        await broker.stop()

    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("payments.new", passive=True)
        message = await queue.get(timeout=5)
        assert message is not None
        await message.ack()
    assert message.message_id == "msg-1"
    assert message.correlation_id == "corr-1"
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert message.headers["x-attempt"] == 1


async def test_unroutable_publish_raises(settings: Settings, rabbitmq_url: str) -> None:
    settings = settings.model_copy(update={"rabbitmq_url": rabbitmq_url})
    broker = build_broker(settings)
    await broker.connect()
    try:
        await declare_topology(broker, settings)
        with pytest.raises(Exception):  # noqa: B017, PT011 — the exact type depends on aio-pika; any error is correct
            await publish_json(broker, {}, exchange=PAYMENTS_EXCHANGE, routing_key="nowhere", message_id="m")
    finally:
        await broker.stop()
