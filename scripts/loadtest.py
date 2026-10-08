"""Load test: create N payments with C in flight, then measure time until each reaches a final status.

End-to-end time is dominated by the emulated gateway (2-5 s). For a throughput run start the stack with
GATEWAY_MIN_DELAY=0.05 GATEWAY_MAX_DELAY=0.1 and scale consumers: docker compose up -d --scale consumer=3
"""

import argparse
import asyncio
import statistics
import time
import uuid

import httpx


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


async def run(base_url: str, api_key: str, payments: int, concurrency: int) -> None:
    limits = httpx.Limits(max_connections=concurrency)
    async with httpx.AsyncClient(
        base_url=base_url, headers={"X-API-Key": api_key}, limits=limits, timeout=30
    ) as client:
        gate = asyncio.Semaphore(concurrency)
        accept: list[float] = []
        created: dict[str, float] = {}

        async def create_one(n: int) -> None:
            async with gate:
                started = time.perf_counter()
                response = await client.post(
                    "/api/v1/payments",
                    json={"amount": "10.00", "currency": "USD", "description": f"load {n}"},
                    headers={"Idempotency-Key": f"load-{uuid.uuid4()}"},
                )
                response.raise_for_status()
                accept.append(time.perf_counter() - started)
                created[response.json()["payment_id"]] = time.perf_counter()

        wall = time.perf_counter()
        await asyncio.gather(*(create_one(n) for n in range(payments)))
        accept_wall = time.perf_counter() - wall

        end_to_end: list[float] = []
        pending = dict(created)
        while pending:
            await asyncio.sleep(0.25)

            async def check(payment_id: str) -> None:
                async with gate:
                    status = (await client.get(f"/api/v1/payments/{payment_id}")).json()["status"]
                if status != "pending":
                    end_to_end.append(time.perf_counter() - pending.pop(payment_id))

            await asyncio.gather(*(check(pid) for pid in list(pending)))
        total_wall = time.perf_counter() - wall

    print(f"payments: {payments}, concurrency: {concurrency}")
    print(
        f"accept  p50 {pct(accept, 0.5) * 1000:7.1f} ms   p95 {pct(accept, 0.95) * 1000:7.1f} ms   "
        f"p99 {pct(accept, 0.99) * 1000:7.1f} ms   rate {payments / accept_wall:7.1f} req/s"
    )
    print(
        f"e2e     p50 {pct(end_to_end, 0.5):7.2f} s    p95 {pct(end_to_end, 0.95):7.2f} s    "
        f"mean {statistics.fmean(end_to_end):5.2f} s   throughput {payments / total_wall:6.1f} payments/s"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--api-key", default="dev-api-key")
    parser.add_argument("--payments", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()
    asyncio.run(run(args.base_url, args.api_key, args.payments, args.concurrency))


if __name__ == "__main__":
    main()
