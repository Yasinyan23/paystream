import json
import os
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.e2e

API = os.environ.get("PAYSTREAM_API", "http://localhost:8000")
SINK = os.environ.get("PAYSTREAM_SINK", "http://localhost:8080")
RABBITMQ = os.environ.get("PAYSTREAM_RABBITMQ", "http://localhost:15672")
API_KEY = os.environ.get("API_KEY", "dev-api-key")
HOOK = "http://webhook-sink:8080/webhooks"


@pytest.fixture(scope="module")
def api() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=API, headers={"X-API-Key": API_KEY}, timeout=10) as client:
        try:
            ready = client.get("/ready").status_code == 200
        except httpx.TransportError:
            ready = False
        assert ready, f"stack is not running at {API}; run `make up`"
        yield client


def create(api: httpx.Client, mode: str | None) -> str:
    body: dict[str, Any] = {"amount": "42.00", "currency": "EUR"}
    if mode:
        body["webhook_url"] = f"{HOOK}/{mode}"
    response = api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": f"e2e-{uuid.uuid4()}"})
    assert response.status_code == 202
    return str(response.json()["payment_id"])


def wait_for(api: httpx.Client, payment_id: str, done: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        payment: dict[str, Any] = api.get(f"/api/v1/payments/{payment_id}").json()
        if done(payment):
            return payment
        time.sleep(0.5)
    raise AssertionError(f"payment {payment_id} did not reach the expected state")


def is_dead_lettered(payment_id: str) -> bool:
    """Looks for the payment in the dead letters themselves: the queue's message count in the management API runs
    up to ~7 s late, which makes a count taken before and after the test unreliable."""
    response = httpx.post(
        f"{RABBITMQ}/api/queues/%2F/payments.dead/get",
        auth=("paystream", "paystream"),
        # A peek: the messages are requeued. 1000 is also as many as `dlq replay --all` moves.
        json={"count": 1000, "ackmode": "ack_requeue_true", "encoding": "auto"},
    )
    response.raise_for_status()
    return any(payment_id in message["payload"] for message in response.json())


def is_json_log_line(line: str) -> bool:
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return False
    return isinstance(record, dict) and "service" in record


def test_payment_is_processed_and_signed_webhook_delivered(api: httpx.Client) -> None:
    payment_id = create(api, "ok")
    payment = wait_for(api, payment_id, lambda p: p["webhook"]["state"] == "delivered")
    assert payment["status"] in {"succeeded", "failed"}
    assert payment["processed_at"] is not None
    # The sink lists only the deliveries whose signature it verified.
    [delivery] = httpx.get(f"{SINK}/received", params={"payment_id": payment_id}).json()
    assert delivery["type"] == f"payment.{payment['status']}"


def test_payment_without_webhook(api: httpx.Client) -> None:
    payment = wait_for(api, create(api, None), lambda p: p["status"] != "pending")
    assert payment["webhook"]["state"] == "not_configured"


def test_flaky_receiver_gets_the_webhook_on_retry(api: httpx.Client) -> None:
    payment = wait_for(api, create(api, "flaky"), lambda p: p["webhook"]["state"] == "delivered")
    assert payment["webhook"]["attempts"] == 2


def test_dead_receiver_ends_in_dlq_and_replay_retries(api: httpx.Client) -> None:
    payment_id = create(api, "fail")
    payment = wait_for(api, payment_id, lambda p: p["webhook"]["attempts"] >= 3)
    assert payment["webhook"]["state"] == "failing"
    deadline = time.monotonic() + 30
    while not is_dead_lettered(payment_id) and time.monotonic() < deadline:
        time.sleep(0.5)
    assert is_dead_lettered(payment_id)

    subprocess.run(
        ["docker", "compose", "exec", "-T", "consumer", "paystream", "dlq", "replay", "--all"],  # noqa: S607
        check=True,
    )
    wait_for(api, payment_id, lambda p: p["webhook"]["attempts"] >= 4)


def test_service_logs_are_json_lines_with_service() -> None:
    """Nothing else covers the workers' logging wiring: one stray plain-text line fails this."""
    for service in ("api", "outbox-relay", "consumer"):
        command = ["docker", "compose", "logs", "--no-log-prefix", service]
        lines = subprocess.run(command, check=True, capture_output=True, text=True).stdout.splitlines()  # noqa: S603
        assert lines, f"{service} logged nothing"
        stray = [line for line in lines if not is_json_log_line(line)]
        assert not stray, f"{service}: {len(stray)} line(s) are not JSON with a `service` field, e.g. {stray[0]!r}"
