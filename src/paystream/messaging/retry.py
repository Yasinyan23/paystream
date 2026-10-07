import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from faststream.rabbit import RabbitBroker

from paystream.messaging.publisher import publish_json
from paystream.messaging.topology import DEAD_LETTER_EXCHANGE, DEAD_LETTER_KEY, RETRY_EXCHANGE, retry_routing_key

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"
# RabbitMQ's own bookkeeping: the dead-letter history and the redelivery counters of a quorum queue.
_BROKER_HEADER_PREFIXES = ("x-death", "x-first-death", "x-last-death", "x-delivery-count", "x-acquired-count")


def next_retry_delay_ms(attempt: int, delays_ms: Sequence[int]) -> int | None:
    """`attempt` is the 1-based number of the attempt that just failed; None means give up."""
    return delays_ms[attempt - 1] if 1 <= attempt <= len(delays_ms) else None


def _carry(headers: Mapping[str, Any]) -> dict[str, Any]:
    """Our headers survive a hop; RabbitMQ's own bookkeeping does not: it would be stale on the next queue."""
    return {k: v for k, v in headers.items() if not k.startswith(_BROKER_HEADER_PREFIXES)}


class RetryPublisher:
    def __init__(self, broker: RabbitBroker, delays_ms: Sequence[int]) -> None:
        self._broker, self._delays_ms = broker, list(delays_ms)

    async def retry_or_dead_letter(
        self,
        *,
        body: Any,
        message_id: str,
        correlation_id: str | None,
        headers: Mapping[str, Any],
        attempt: int,
        error: str,
    ) -> bool:
        delay = next_retry_delay_ms(attempt, self._delays_ms)
        if delay is None:
            await self.dead_letter(
                body=body,
                message_id=message_id,
                correlation_id=correlation_id,
                headers=headers,
                reason="max_attempts_exceeded",
                error=error,
                attempt=attempt,
            )
            return False
        await publish_json(
            self._broker,
            body,
            exchange=RETRY_EXCHANGE,
            routing_key=retry_routing_key(delay),
            message_id=message_id,
            correlation_id=correlation_id,
            headers={**_carry(headers), ATTEMPT_HEADER: attempt + 1, "x-last-error": error[:500]},
        )
        logger.info("message %s attempt %d failed, retrying in %d ms: %s", message_id, attempt, delay, error)
        return True

    async def dead_letter(
        self,
        *,
        body: Any,
        message_id: str,
        correlation_id: str | None,
        headers: Mapping[str, Any],
        reason: str,
        error: str,
        attempt: int,
    ) -> None:
        await publish_json(
            self._broker,
            body,
            exchange=DEAD_LETTER_EXCHANGE,
            routing_key=DEAD_LETTER_KEY,
            message_id=message_id,
            correlation_id=correlation_id,
            headers={
                **_carry(headers),
                ATTEMPT_HEADER: attempt,
                "x-dead-reason": reason,
                "x-error": error[:1000],
                "x-failed-at": datetime.now(UTC).isoformat(),
            },
        )
        logger.warning("message %s dead-lettered after attempt %d (%s): %s", message_id, attempt, reason, error)
