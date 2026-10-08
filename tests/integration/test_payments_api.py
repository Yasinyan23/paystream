import asyncio
from uuid import UUID

import asyncpg
import pytest
from httpx import AsyncClient, Response
from sqlalchemy import func, select

from paystream.core.config import Settings
from paystream.db.database import Database
from paystream.db.models import OutboxEvent, Payment
from paystream.repositories.outbox import OUTBOX_NOTIFY_CHANNEL

pytestmark = pytest.mark.integration

BODY = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://hooks.test/payments",
}


async def create(client: AsyncClient, key: str, body: dict[str, object] = BODY) -> Response:
    return await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": key})


def raw_payment(members: str) -> str:
    """JSON text of a valid payment plus extra members; `json=` cannot express NaN or lone surrogates."""
    return '{"amount":"1","currency":"RUB",' + members + "}"


NON_STORABLE = [
    pytest.param(raw_payment('"metadata":{"x":NaN}'), id="metadata-nan"),
    pytest.param(raw_payment('"metadata":{"x":[1,{"y":Infinity}]}'), id="metadata-nested-infinity"),
    pytest.param(raw_payment('"metadata":{"x":-Infinity}'), id="metadata-negative-infinity"),
    pytest.param(raw_payment('"metadata":{"x":1e999}'), id="metadata-float-overflowing-to-infinity"),
    pytest.param(raw_payment(r'"description":"a\u0000b"'), id="description-nul"),
    pytest.param(raw_payment(r'"metadata":{"k":[{"z":"a\u0000b"}]}'), id="metadata-value-nul"),
    pytest.param(raw_payment(r'"metadata":{"a\u0000b":1}'), id="metadata-key-nul"),
    pytest.param(raw_payment(r'"description":"\ud800"'), id="description-lone-surrogate"),
    pytest.param(raw_payment(r'"metadata":{"k":["\udfff"]}'), id="metadata-value-lone-surrogate"),
    pytest.param(raw_payment(r'"metadata":{"\ud800":1}'), id="metadata-key-lone-surrogate"),
    pytest.param(raw_payment(r'"\ud800":1'), id="unknown-field-lone-surrogate"),
    pytest.param('{"amount":NaN,"currency":"RUB"}', id="amount-nan"),
    pytest.param('{"amount":"1e-10000000","currency":"RUB"}', id="amount-rounding-to-zero"),
    pytest.param(raw_payment('"metadata":' + '{"a":' * 40 + "1" + "}" * 40), id="metadata-40-levels-deep"),
    pytest.param(raw_payment('"metadata":' + '{"a":' * 5000 + "1" + "}" * 5000), id="metadata-5000-levels-deep"),
]


async def test_create_returns_202_and_location(client: AsyncClient) -> None:
    response = await create(client, "order-42")
    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"
    assert response.headers["location"] == f"/api/v1/payments/{body['payment_id']}"
    assert "idempotent-replayed" not in response.headers


async def test_create_writes_outbox_event_in_same_transaction(client: AsyncClient, db: Database) -> None:
    response = await create(client, "order-43")
    payment_id = UUID(response.json()["payment_id"])
    async with db.sessionmaker() as session:
        events = list(await session.scalars(select(OutboxEvent).where(OutboxEvent.aggregate_id == payment_id)))
    assert len(events) == 1
    event = events[0]
    assert event.event_type == "payment.created"
    assert event.payload["event_id"] == str(event.id)
    assert event.payload["data"]["payment_id"] == str(payment_id)
    assert event.payload["data"]["amount"] == "1500.00"
    assert event.headers["correlation_id"] == response.headers["x-request-id"]


async def test_create_notifies_the_relay(client: AsyncClient, settings: Settings) -> None:
    woke = asyncio.Event()
    listener = await asyncpg.connect(settings.database_url.replace("+asyncpg", "", 1))
    await listener.add_listener(OUTBOX_NOTIFY_CHANNEL, lambda *_: woke.set())
    try:
        await create(client, "order-49")
        async with asyncio.timeout(5):
            await woke.wait()
    finally:
        await listener.close()


async def test_replay_returns_same_payment(client: AsyncClient) -> None:
    first = await create(client, "order-44")
    second = await create(client, "order-44")
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    assert second.headers["idempotent-replayed"] == "true"


async def test_key_reuse_with_different_body_is_422(client: AsyncClient) -> None:
    await create(client, "order-45")
    response = await create(client, "order-45", {**BODY, "amount": "1.00"})
    assert response.status_code == 422
    assert response.json()["type"] == "urn:paystream:problem:idempotency-key-reused"


async def test_concurrent_duplicates_create_one_payment(client: AsyncClient, db: Database) -> None:
    responses = await asyncio.gather(*(create(client, "order-46") for _ in range(10)))
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with db.sessionmaker() as session:
        count = len(list(await session.scalars(select(OutboxEvent))))
    assert count == 1


async def test_missing_idempotency_key_is_400(client: AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY)
    assert response.status_code == 400
    assert response.json()["type"] == "urn:paystream:problem:idempotency-key-missing"


async def test_openapi_marks_the_idempotency_key_required(client: AsyncClient) -> None:
    operation = (await client.get("/openapi.json")).json()["paths"]["/api/v1/payments"]["post"]
    [header] = [parameter for parameter in operation["parameters"] if parameter["name"] == "Idempotency-Key"]
    assert (header["in"], header["required"]) == ("header", True)


async def test_invalid_idempotency_key_is_400(client: AsyncClient) -> None:
    response = await create(client, "has spaces")
    assert response.status_code == 400
    assert response.json()["type"] == "urn:paystream:problem:idempotency-key-invalid"


async def test_validation_error_is_problem_details(client: AsyncClient) -> None:
    response = await create(client, "order-47", {"amount": -5, "currency": "RUB"})
    assert response.status_code == 422
    body = response.json()
    assert body["type"] == "urn:paystream:problem:validation-error"
    assert body["errors"][0]["loc"] == ["body", "amount"]


@pytest.mark.parametrize("raw", NON_STORABLE)
async def test_non_storable_input_is_422_and_persists_nothing(client: AsyncClient, db: Database, raw: str) -> None:
    headers = {"Idempotency-Key": "order-50", "Content-Type": "application/json"}
    response = await client.post("/api/v1/payments", content=raw, headers=headers)
    assert response.status_code == 422
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["type"] == "urn:paystream:problem:validation-error"
    async with db.sessionmaker() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


async def test_get_returns_details(client: AsyncClient) -> None:
    created = await create(client, "order-48")
    payment_id = created.json()["payment_id"]
    response = await client.get(f"/api/v1/payments/{payment_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == payment_id
    assert body["amount"] == "1500.00"
    assert body["currency"] == "RUB"
    assert body["metadata"] == {"order_id": 42}
    assert body["status"] == "pending"
    assert body["failure_reason"] is None
    assert body["idempotency_key"] == "order-48"
    assert body["webhook_url"] == "https://hooks.test/payments"
    assert body["webhook"] == {
        "state": "pending",
        "attempts": 0,
        "last_status_code": None,
        "last_error": None,
        "delivered_at": None,
    }
    assert body["processed_at"] is None


async def test_get_unknown_payment_is_404(client: AsyncClient) -> None:
    response = await client.get("/api/v1/payments/0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
    assert response.status_code == 404
    assert response.json()["type"] == "urn:paystream:problem:payment-not-found"


async def test_get_with_malformed_id_is_422(client: AsyncClient) -> None:
    response = await client.get("/api/v1/payments/not-a-uuid")
    assert response.status_code == 422


@pytest.mark.parametrize(("chars", "status"), [(3_000, 202), (9_000, 422)])
async def test_metadata_limit_counts_utf8_bytes(client: AsyncClient, chars: int, status: int) -> None:
    # Cyrillic is 2 bytes in UTF-8 and 6 as a \uXXXX escape: 3000 chars fit in 16 KB, 9000 chars (18 KB of bytes) do not
    response = await create(client, "order-51", {**BODY, "metadata": {"note": "я" * chars}})
    assert response.status_code == status


async def test_unsupported_method_is_405_with_allow_header(client: AsyncClient) -> None:
    response = await client.delete("/api/v1/payments/0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
    assert response.status_code == 405
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["type"] == "urn:paystream:problem:method-not-allowed"
    assert response.headers["allow"] == "GET"
