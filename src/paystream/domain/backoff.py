import random
from collections.abc import Callable


def exponential_backoff(
    attempt: int,
    *,
    base: float,
    cap: float,
    jitter: float = 0.2,
    rand: Callable[[], float] = random.random,
) -> float:
    """Seconds to wait after the `attempt`-th failure: base·2^(attempt-1), ±jitter, never above cap."""
    if attempt < 1:
        raise ValueError("attempt starts at 1")
    # Clamp the exponent: unbounded attempt counters overflow 2.0 ** 1024, and any cap is hit long before 2**62.
    delay = min(cap, base * 2.0 ** min(attempt - 1, 62))
    return min(cap, delay * (1 - jitter + 2 * jitter * rand()))
