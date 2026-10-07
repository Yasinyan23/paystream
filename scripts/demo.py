"""Guided tour of a running paystream stack. Run with `make demo`."""

import os
import time
import uuid
from collections.abc import Callable
from typing import Any

import httpx

API = os.environ.get("PAYSTREAM_API", "http://localhost:8000")
SINK = os.environ.get("PAYSTREAM_SINK", "http://localhost:8080")
RABBITMQ = os.environ.get("PAYSTREAM_RABBITMQ", "http://localhost:15672")
SINK_FROM_CONTAINERS = "http://webhook-sink:8080"
API_KEY = os.environ.get("API_KEY", "dev-api-key")

BOLD, GREEN, RED, CYAN, DIM, RESET = "\033[1m", "\033[32m", "\033[31m", "\033[36m", "\033[2m", "\033[0m"


def step(title: str) -> None:
    print(f"\n{BOLD}{CYAN}▶ {title}{RESET}")


def ok(message: str) -> None:
    print(f"  {GREEN}✔{RESET} {message}")


def note(message: str) -> None:
    print(f"  {DIM}{message}{RESET}")


def wait_for(
    client: httpx.Client, payment_id: str, done: Callable[[dict[str, Any]], bool], timeout: float = 45
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payment: dict[str, Any] = client.get(f"/api/v1/payments/{payment_id}").json()
        if done(payment):
            return payment
        time.sleep(0.5)
    raise SystemExit(f"{RED}timed out waiting for payment {payment_id}{RESET}")


def dead_letters() -> int:
    queue = httpx.get(f"{RABBITMQ}/api/queues/%2F/payments.dead", auth=("paystream", "paystream")).json()
    return int(queue.get("messages", 0))


def wait_for_dead_letter(before: int, timeout: float = 15) -> int:
    """RabbitMQ refreshes its management stats about every 5 s: poll until the count rises above `before`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (count := dead_letters()) > before:
            return count
        time.sleep(0.5)
    raise SystemExit(f"{RED}payments.dead still holds {before} message(s) after {timeout:.0f} s{RESET}")


def create(client: httpx.Client, key: str, mode: str, amount: str = "1500.00") -> httpx.Response:
    body = {
        "amount": amount,
        "currency": "RUB",
        "description": f"Demo order ({mode} webhook)",
        "metadata": {"demo": True},
        "webhook_url": f"{SINK_FROM_CONTAINERS}/webhooks/{mode}",
    }
    return client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": key})


def main() -> None:
    run = uuid.uuid4().hex[:8]
    with httpx.Client(base_url=API, headers={"X-API-Key": API_KEY}, timeout=10) as client:
        step("1. Create a payment: 202 Accepted, processed asynchronously")
        response = create(client, f"demo-{run}-ok", "ok")
        payment_id = response.json()["payment_id"]
        ok(f"202 {response.json()['status']}  payment_id={payment_id}")
        note("API wrote the payment + an outbox row in one transaction; the relay published it on NOTIFY")
        payment = wait_for(client, payment_id, lambda p: p["webhook"]["state"] == "delivered")
        reason = f" ({payment['failure_reason']})" if payment["failure_reason"] else ""
        ok(f"gateway result: {payment['status']}{reason}, processed_at={payment['processed_at']}")
        [delivery] = httpx.get(f"{SINK}/received", params={"payment_id": payment_id}).json()
        ok(f"signed webhook verified by the receiver: {delivery['type']} id={delivery['webhook_id']}")

        step("2. Retry the same request: idempotent replay")
        replay = create(client, f"demo-{run}-ok", "ok")
        ok(
            f"{replay.status_code}, same payment_id={replay.json()['payment_id'] == payment_id}, "
            f"Idempotent-Replayed: {replay.headers.get('idempotent-replayed')}"
        )

        step("3. Same key, different body: rejected")
        reused = create(client, f"demo-{run}-ok", "ok", amount="1.00")
        ok(f"{reused.status_code} {reused.json()['type']}")

        step("4. Flaky receiver: first delivery 503, retried after 2 s, then delivered")
        flaky_id = create(client, f"demo-{run}-flaky", "flaky").json()["payment_id"]
        flaky = wait_for(client, flaky_id, lambda p: p["webhook"]["state"] == "delivered")
        ok(f"delivered after {flaky['webhook']['attempts']} attempts (same webhook-id on every attempt)")

        step("5. Receiver down: 3 attempts (2 s, 4 s backoff), then the dead-letter queue")
        dead_before = dead_letters()
        failing_id = create(client, f"demo-{run}-fail", "fail").json()["payment_id"]
        failing = wait_for(client, failing_id, lambda p: p["webhook"]["attempts"] >= 3)
        ok(
            f"payment status kept: {failing['status']}; webhook state={failing['webhook']['state']}, "
            f"last error: {failing['webhook']['last_error']}"
        )
        held = wait_for_dead_letter(dead_before)
        ok(f"payments.dead holds {held} message(s)  →  inspect: make dlq, replay: make dlq-replay")

    print(
        f"\n{BOLD}Explore:{RESET} API docs {API}/docs · RabbitMQ {RABBITMQ} (paystream/paystream) · "
        f"make observability for Grafana and Jaeger\n"
    )


if __name__ == "__main__":
    main()
