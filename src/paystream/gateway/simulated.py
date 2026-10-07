import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from paystream.domain.payment import Currency

DECLINE_REASONS = ("card_declined", "insufficient_funds", "do_not_honor")


@dataclass(frozen=True, slots=True)
class ChargeResult:
    succeeded: bool
    failure_reason: str | None = None


class PaymentGateway(Protocol):
    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        """A real gateway would receive payment_id as its idempotency key."""
        ...


class SimulatedGateway:
    """Emulates an external gateway: random latency and a configurable approval rate.

    A decline is a business outcome (`failed`), not an error, so it is never retried.
    """

    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay, self._max_delay, self._success_rate = min_delay, max_delay, success_rate
        self._rng = rng or random.Random()  # noqa: S311 — simulation, not cryptography
        self._sleep = sleep

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        await self._sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return ChargeResult(succeeded=True)
        return ChargeResult(succeeded=False, failure_reason=self._rng.choice(DECLINE_REASONS))
