import json
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from paystream.devtools.webhook_sink import create_sink
from paystream.webhooks.signing import sign

SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"


def signed(webhook_id: str, payload: dict[str, object], *, secret: str = SECRET) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(payload).encode()
    timestamp = 1_900_000_000
    headers = {
        "webhook-id": webhook_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": sign(secret, msg_id=webhook_id, timestamp=timestamp, body=body),
        "content-type": "application/json",
    }
    return body, headers


@pytest.fixture
async def sink(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[AsyncClient]:
    monkeypatch.setattr("time.time", lambda: 1_900_000_000)  # verify() checks the timestamp tolerance
    async with AsyncClient(transport=ASGITransport(app=create_sink(SECRET)), base_url="http://sink") as client:
        yield client


PAYLOAD = {"type": "payment.succeeded", "data": {"id": "p-1"}}


async def test_ok_mode_accepts_and_records(sink: AsyncClient) -> None:
    body, headers = signed("evt_1", PAYLOAD)
    assert (await sink.post("/webhooks/ok", content=body, headers=headers)).status_code == 204
    [record] = (await sink.get("/received", params={"payment_id": "p-1"})).json()
    assert (record["webhook_id"], record["type"], record["delivery"]) == ("evt_1", "payment.succeeded", 1)


async def test_bad_signature_is_rejected(sink: AsyncClient) -> None:
    body, headers = signed("evt_2", PAYLOAD, secret="whsec_" + "A" * 43 + "=")
    assert (await sink.post("/webhooks/ok", content=body, headers=headers)).status_code == 401
    assert (await sink.get("/received")).json() == []


async def test_fail_and_flaky_modes(sink: AsyncClient) -> None:
    body, headers = signed("evt_3", PAYLOAD)
    assert (await sink.post("/webhooks/fail", content=body, headers=headers)).status_code == 503
    body, headers = signed("evt_4", PAYLOAD)
    assert (await sink.post("/webhooks/flaky", content=body, headers=headers)).status_code == 503
    assert (await sink.post("/webhooks/flaky", content=body, headers=headers)).status_code == 204
