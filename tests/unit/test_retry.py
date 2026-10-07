import pytest

from paystream.messaging.retry import next_retry_delay_ms

DELAYS = [2000, 4000]  # RETRY_MAX_ATTEMPTS=3


@pytest.mark.parametrize(("failed_attempt", "expected"), [(1, 2000), (2, 4000), (3, None), (7, None)])
def test_three_attempts_then_dead_letter(failed_attempt: int, expected: int | None) -> None:
    assert next_retry_delay_ms(failed_attempt, DELAYS) == expected


def test_single_attempt_never_retries() -> None:
    assert next_retry_delay_ms(1, []) is None
