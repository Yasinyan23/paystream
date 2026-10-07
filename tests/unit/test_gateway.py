import random
from decimal import Decimal
from uuid import uuid7

from paystream.domain.payment import Currency
from paystream.gateway.simulated import DECLINE_REASONS, SimulatedGateway


async def test_outcome_ratio_and_latency_bounds() -> None:
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    gateway = SimulatedGateway(min_delay=2, max_delay=5, success_rate=0.9, rng=random.Random(42), sleep=fake_sleep)
    results = [await gateway.charge(uuid7(), Decimal("10.00"), Currency.RUB) for _ in range(2000)]
    success_rate = sum(r.succeeded for r in results) / len(results)
    assert 0.87 < success_rate < 0.93
    assert len(delays) == len(results)  # one sleep per charge: the bounds check below must not pass on an empty list
    assert all(2 <= d <= 5 for d in delays)
    failures = [r for r in results if not r.succeeded]
    assert all(r.failure_reason in DECLINE_REASONS for r in failures)
    assert all(r.failure_reason is None for r in results if r.succeeded)
