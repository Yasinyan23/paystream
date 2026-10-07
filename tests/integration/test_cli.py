import asyncio
import json
from collections.abc import AsyncIterator, Iterator

import aio_pika
import pytest
from aio_pika.exceptions import PublishError

from paystream.core.config import Settings, get_settings
from paystream.messaging.broker import build_broker
from paystream.messaging.retry import RetryPublisher
from paystream.messaging.topology import (
    PAYMENT_CREATED_KEY,
    PAYMENTS_EXCHANGE,
    RETRY_EXCHANGE,
    declare_topology,
    retry_routing_key,
)
from paystream.workers.cli import dlq_list, dlq_replay, main

pytestmark = pytest.mark.integration


async def purge_queues(url: str) -> None:
    connection = await aio_pika.connect(url)
    async with connection:
        channel = await connection.channel()
        for name in ("payments.new", "payments.dead"):
            await (await channel.declare_queue(name, passive=True)).purge()


@pytest.fixture
async def dead_letters(settings: Settings, rabbitmq_url: str) -> AsyncIterator[str]:
    settings = settings.model_copy(update={"rabbitmq_url": rabbitmq_url})
    broker = build_broker(settings)
    await broker.connect()
    await declare_topology(broker, settings)
    await purge_queues(rabbitmq_url)
    retries = RetryPublisher(broker, [])
    for n in range(2):
        await retries.dead_letter(
            body={"n": n},
            message_id=f"dead-{n}",
            correlation_id=f"corr-{n}",
            headers={"correlation_id": f"corr-{n}", "x-last-error": "HTTP 503"},  # as a real, retried event has them
            reason="max_attempts_exceeded",
            error="HTTP 503",
            attempt=3,
        )
    await broker.stop()
    yield rabbitmq_url
    await purge_queues(rabbitmq_url)  # one RabbitMQ container serves the whole session: leave both queues empty


@pytest.fixture
async def broker_dead_letter(dead_letters: str, settings: Settings) -> str:
    """`dead_letters` plus `loop-1`, dead-lettered by the broker at the delivery limit after a pass through the first
    retry tier: its first death is `expired`, its last `delivery_limit`."""
    connection = await aio_pika.connect(dead_letters)
    async with connection:
        channel = await connection.channel()
        retry_exchange = await channel.get_exchange(RETRY_EXCHANGE.name)
        await retry_exchange.publish(
            aio_pika.Message(b"{}", message_id="loop-1"), routing_key=retry_routing_key(settings.retry_delays_ms[0])
        )
        queue = await channel.declare_queue("payments.new", passive=True)
        rejected = 0
        async with asyncio.timeout(10):
            while rejected <= settings.delivery_limit:  # a consumer that keeps giving the message back
                message = await queue.get(fail=False)
                if message is None:  # still in the retry tier
                    await asyncio.sleep(0.05)
                    continue
                await message.reject(requeue=True)
                rejected += 1
    async with asyncio.timeout(10):  # the broker dead-letters asynchronously
        while True:
            if len(await dlq_list(dead_letters, limit=10)) == 3:
                return dead_letters
            await asyncio.sleep(0.1)


async def test_list_does_not_consume(dead_letters: str) -> None:
    first = await dlq_list(dead_letters, limit=10)
    second = await dlq_list(dead_letters, limit=10)
    assert [m["message_id"] for m in first] == ["dead-0", "dead-1"] == [m["message_id"] for m in second]
    assert first[0]["reason"] == "max_attempts_exceeded"
    assert (first[0]["error"], first[0]["attempts"]) == ("HTTP 503", 3)
    assert first[0]["failed_at"]


async def test_list_stops_at_the_limit(dead_letters: str) -> None:
    assert [m["message_id"] for m in await dlq_list(dead_letters, limit=1)] == ["dead-0"]


async def test_list_shows_the_delivery_limit_as_reason(broker_dead_letter: str) -> None:
    reasons = {m["message_id"]: m["reason"] for m in await dlq_list(broker_dead_letter, limit=10)}
    # the broker's dead letter has no x-dead-reason: its last death says why it is here, its first is the retry tier
    assert reasons["loop-1"] == "delivery_limit"


async def test_replay_one_message(dead_letters: str) -> None:
    assert await dlq_replay(dead_letters, message_id="dead-1", limit=100) == 1
    assert [m["message_id"] for m in await dlq_list(dead_letters, limit=10)] == ["dead-0"]
    connection = await aio_pika.connect(dead_letters)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("payments.new", passive=True)
        message = await queue.get(timeout=5)
        assert message is not None
        await message.ack()
    assert message.message_id == "dead-1"
    assert message.correlation_id == "corr-1"
    assert message.headers["correlation_id"] == "corr-1"  # the message's own headers survive the replay
    assert message.content_type == "application/json"  # the consumer decodes by it
    assert message.headers["x-attempt"] == 1
    assert message.headers["x-replayed-at"]
    assert not {"x-dead-reason", "x-error", "x-failed-at", "x-last-error"} & message.headers.keys()
    assert json.loads(message.body) == {"n": 1}


async def test_replay_drops_the_brokers_death_bookkeeping(broker_dead_letter: str) -> None:
    assert await dlq_replay(broker_dead_letter, message_id="loop-1", limit=100) == 1
    connection = await aio_pika.connect(broker_dead_letter)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("payments.new", passive=True)
        message = await queue.get(timeout=5)
        await message.ack()
    assert message.message_id == "loop-1"
    assert not [h for h in message.headers if h.startswith(("x-death", "x-first-death", "x-last-death"))]


async def test_replay_stops_at_the_limit(dead_letters: str) -> None:
    assert await dlq_replay(dead_letters, message_id=None, limit=1) == 1
    assert [m["message_id"] for m in await dlq_list(dead_letters, limit=10)] == ["dead-1"]


async def test_replay_keeps_the_dead_letter_it_cannot_route(dead_letters: str) -> None:
    connection = await aio_pika.connect(dead_letters)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("payments.new", passive=True)
        exchange = await channel.get_exchange(PAYMENTS_EXCHANGE.name)
        await queue.unbind(exchange, routing_key=PAYMENT_CREATED_KEY)
        try:
            with pytest.raises(PublishError):
                await dlq_replay(dead_letters, message_id=None, limit=100)
        finally:
            await queue.bind(exchange, routing_key=PAYMENT_CREATED_KEY)  # the topology is shared with later tests
    assert [m["message_id"] for m in await dlq_list(dead_letters, limit=10)] == ["dead-0", "dead-1"]


@pytest.fixture
def cli_env(dead_letters: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """`main()` is synchronous (it calls asyncio.run), so these tests are too. The cleared settings cache lets
    RABBITMQ_URL take effect."""
    monkeypatch.setenv("RABBITMQ_URL", dead_letters)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.mark.usefixtures("cli_env")
def test_main_replay_all(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["dlq", "replay", "--all"]) == 0
    assert "replayed 2" in capsys.readouterr().out
    assert main(["dlq", "list"]) == 0
    assert capsys.readouterr().out.strip() == "[]"
    assert main(["dlq", "replay", "--all"]) == 0  # nothing left to replay is not a failure
    assert "replayed 0" in capsys.readouterr().out


@pytest.mark.usefixtures("cli_env")
def test_main_replay_one_message(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["dlq", "replay", "--message-id", "dead-1"]) == 0
    assert "replayed 1" in capsys.readouterr().out
    assert main(["dlq", "list"]) == 0
    assert [m["message_id"] for m in json.loads(capsys.readouterr().out)] == ["dead-0"]


@pytest.mark.usefixtures("cli_env")
def test_main_replay_unknown_message_id_fails(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["dlq", "replay", "--message-id", "no-such-id"]) == 1
    assert "no-such-id" in capsys.readouterr().err
    assert main(["dlq", "list"]) == 0
    assert [m["message_id"] for m in json.loads(capsys.readouterr().out)] == ["dead-0", "dead-1"]  # nothing moved
