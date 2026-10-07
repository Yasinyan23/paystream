import pytest
from httpx import ASGITransport, AsyncClient

from paystream.api.app import create_app
from paystream.core.config import Settings
from tests.conftest import sample

pytestmark = pytest.mark.integration


async def test_metrics_endpoint_exposes_paystream_metrics(client: AsyncClient) -> None:
    response = await client.get("/metrics", headers={"X-API-Key": ""})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    for name in (
        "paystream_payments_created_total",
        "paystream_payments_processed_total",
        "paystream_webhook_deliveries_total",
        "paystream_outbox_pending_events",
        "paystream_messages_dead_lettered_total",
        "paystream_http_requests_total",
    ):
        assert name in response.text


async def test_created_and_replayed_payments_are_counted(client: AsyncClient) -> None:
    created_before = sample("paystream_payments_created_total", currency="USD")
    replays_before = sample("paystream_idempotent_replays_total")
    body = {"amount": "5.00", "currency": "USD"}
    for _ in range(2):
        await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "metrics-1"})
    assert sample("paystream_payments_created_total", currency="USD") == created_before + 1
    assert sample("paystream_idempotent_replays_total") == replays_before + 1


async def test_http_requests_are_labelled_by_route_template(client: AsyncClient) -> None:
    labels = {"method": "GET", "route": "/api/v1/payments/{payment_id}", "status": "404"}
    before = sample("paystream_http_requests_total", **labels)
    await client.get("/api/v1/payments/0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
    assert sample("paystream_http_requests_total", **labels) == before + 1


async def test_metrics_can_require_api_key(settings: Settings) -> None:
    app = create_app(settings.model_copy(update={"metrics_require_api_key": True}))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http,
    ):
        assert (await http.get("/metrics")).status_code == 401
        assert (await http.get("/metrics", headers={"X-API-Key": "test-api-key"})).status_code == 200
