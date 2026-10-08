from typing import Any

import pytest

from paystream.messaging.retry import RetryPublisher, next_retry_delay_ms

DELAYS = [2000, 4000]  # RETRY_MAX_ATTEMPTS=3
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"  # a header of ours: it must survive a hop
# What RabbitMQ stamps on a message that a retry tier dead-lettered or a quorum queue redelivered.
FROM_BROKER = {
    "x-death": [{"queue": "payments.retry.2s", "reason": "expired", "count": 1}],
    "x-first-death-queue": "payments.retry.2s",
    "x-last-death-reason": "expired",
    "x-delivery-count": 3,
    "x-acquired-count": 3,
}
INCOMING = {"x-attempt": 2, "traceparent": TRACEPARENT, **FROM_BROKER}


class RecordingBroker:
    """Keeps what publish_json hands to the broker."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    async def publish(self, body: Any, **kwargs: Any) -> None:
        self.published.append(kwargs)


@pytest.mark.parametrize(("failed_attempt", "expected"), [(1, 2000), (2, 4000), (3, None), (7, None)])
def test_three_attempts_then_dead_letter(failed_attempt: int, expected: int | None) -> None:
    assert next_retry_delay_ms(failed_attempt, DELAYS) == expected


def test_single_attempt_never_retries() -> None:
    assert next_retry_delay_ms(1, []) is None


async def test_retry_hop_keeps_our_headers_and_drops_the_brokers() -> None:
    broker = RecordingBroker()
    retried = await RetryPublisher(broker, DELAYS).retry_or_dead_letter(
        body={}, message_id="m-1", correlation_id=None, headers=INCOMING, attempt=2, error="boom"
    )
    [published] = broker.published
    assert retried
    assert (published["exchange"].name, published["routing_key"]) == ("payments.retry", "retry.4000")
    assert published["headers"] == {"x-attempt": 3, "traceparent": TRACEPARENT, "x-last-error": "boom"}


async def test_outage_waits_in_the_longest_tier_without_spending_the_attempt() -> None:
    broker = RecordingBroker()
    retried = await RetryPublisher(broker, DELAYS).retry_or_dead_letter(
        body={}, message_id="m-1", correlation_id=None, headers=INCOMING, attempt=3, error="down", spend_attempt=False
    )
    [published] = broker.published
    assert retried  # even on the last attempt: no dead letter
    assert (published["exchange"].name, published["routing_key"]) == ("payments.retry", "retry.4000")
    assert published["headers"]["x-attempt"] == 3


async def test_dead_letter_keeps_our_headers_and_drops_the_brokers() -> None:
    broker = RecordingBroker()
    await RetryPublisher(broker, DELAYS).dead_letter(
        body={},
        message_id="m-1",
        correlation_id=None,
        headers=INCOMING,
        reason="invalid_message",
        error="boom",
        attempt=2,
    )
    [published] = broker.published
    assert (published["exchange"].name, published["routing_key"]) == ("payments.dlx", "payment.dead")
    headers = dict(published["headers"])
    headers.pop("x-failed-at")  # a timestamp, its value is not under test
    assert headers == {
        "x-attempt": 2,
        "traceparent": TRACEPARENT,
        "x-dead-reason": "invalid_message",
        "x-error": "boom",
    }
