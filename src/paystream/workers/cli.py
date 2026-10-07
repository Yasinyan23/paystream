"""Operations CLI: inspect and replay the dead-letter queue.

paystream dlq list [--limit N]
paystream dlq replay (--message-id ID | --all) [--limit N]
"""

import argparse
import asyncio
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import aio_pika

from paystream.core.config import get_settings
from paystream.messaging.retry import ATTEMPT_HEADER
from paystream.messaging.topology import DEAD_QUEUE_NAME, PAYMENT_CREATED_KEY, PAYMENTS_EXCHANGE

_DEAD_LETTER_HEADERS = {"x-dead-reason", "x-error", "x-failed-at", "x-last-error"}


async def dlq_list(rabbitmq_url: str, *, limit: int) -> list[dict[str, Any]]:
    """Peek without consuming: messages are fetched unacked and return to the queue when the channel closes."""
    connection = await aio_pika.connect(rabbitmq_url)
    items: list[dict[str, Any]] = []
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(DEAD_QUEUE_NAME, passive=True)
        for _ in range(limit):
            message = await queue.get(no_ack=False, fail=False)
            if message is None:
                break
            headers = message.headers or {}
            items.append(
                {
                    "message_id": message.message_id,
                    # Messages the broker dead-lettered at the delivery limit carry no x-dead-reason.
                    "reason": headers.get("x-dead-reason") or headers.get("x-first-death-reason"),
                    "error": headers.get("x-error"),
                    "attempts": headers.get(ATTEMPT_HEADER),
                    "failed_at": headers.get("x-failed-at"),
                }
            )
    return items


async def dlq_replay(rabbitmq_url: str, *, message_id: str | None, limit: int) -> int:
    """Move dead letters back to payments.new with a fresh attempt counter."""
    connection = await aio_pika.connect(rabbitmq_url)
    replayed = 0
    async with connection:
        # on_return_raises: an unroutable replay must fail before the dead letter is acked, not drop the message
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        queue = await channel.declare_queue(DEAD_QUEUE_NAME, passive=True)
        exchange = await channel.get_exchange(PAYMENTS_EXCHANGE.name)
        while replayed < limit:
            message = await queue.get(no_ack=False, fail=False)
            if message is None:
                break
            if message_id is not None and message.message_id != message_id:
                continue  # stays unacked, returns to the queue when the channel closes
            headers = {k: v for k, v in (message.headers or {}).items() if k not in _DEAD_LETTER_HEADERS}
            headers[ATTEMPT_HEADER] = 1
            headers["x-replayed-at"] = datetime.now(UTC).isoformat()
            await exchange.publish(
                aio_pika.Message(
                    body=message.body,
                    headers=headers,
                    message_id=message.message_id,
                    correlation_id=message.correlation_id,
                    content_type=message.content_type,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=PAYMENT_CREATED_KEY,
            )
            await message.ack()
            replayed += 1
            if message_id is not None:
                break
    return replayed


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="paystream", description="Paystream operations")
    commands = parser.add_subparsers(dest="group", required=True)
    dlq = commands.add_parser("dlq", help="dead-letter queue").add_subparsers(dest="command", required=True)
    list_cmd = dlq.add_parser("list", help="show dead letters without consuming them")
    list_cmd.add_argument("--limit", type=int, default=20)
    replay_cmd = dlq.add_parser("replay", help="send dead letters back for processing")
    target = replay_cmd.add_mutually_exclusive_group(required=True)
    target.add_argument("--message-id")
    target.add_argument("--all", action="store_true")
    replay_cmd.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args(argv)

    url = get_settings().rabbitmq_url
    if args.command == "list":
        print(json.dumps(asyncio.run(dlq_list(url, limit=args.limit)), indent=2, default=str))
    else:
        count = asyncio.run(dlq_replay(url, message_id=args.message_id, limit=args.limit))
        print(f"replayed {count} message(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
