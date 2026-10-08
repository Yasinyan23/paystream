import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import IntegrityError

from paystream.api.app import create_app
from paystream.core.config import Settings
from tests.conftest import API_KEY

pytestmark = pytest.mark.integration
DATABASE_DOWN = "postgresql+asyncpg://x:x@127.0.0.1:1/x"


async def test_health(client: AsyncClient) -> None:
    response = await client.get("/health", headers={"X-API-Key": ""})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_checks_database(client: AsyncClient) -> None:
    response = await client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


async def test_ready_is_503_when_database_is_down(settings: Settings) -> None:
    app = create_app(settings.model_copy(update={"database_url": DATABASE_DOWN}))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http,
    ):
        response = await http.get("/ready")
    assert response.status_code == 503
    assert response.json()["type"] == "urn:paystream:problem:service-unavailable"


async def test_requests_are_503_with_retry_after_when_database_is_down(settings: Settings) -> None:
    app = create_app(settings.model_copy(update={"database_url": DATABASE_DOWN}))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test", headers={"X-API-Key": API_KEY}) as http,
    ):
        body = {"amount": "1.00", "currency": "EUR"}
        response = await http.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "down-1"})
    assert response.status_code == 503
    assert response.headers["retry-after"] == "5"
    assert response.json()["type"] == "urn:paystream:problem:service-unavailable"


async def test_request_id_is_generated(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert len(response.headers["x-request-id"]) == 32


async def test_request_id_is_propagated(client: AsyncClient) -> None:
    response = await client.get("/health", headers={"X-Request-ID": "trace-abc.123"})
    assert response.headers["x-request-id"] == "trace-abc.123"


async def test_invalid_request_id_is_replaced(client: AsyncClient) -> None:
    response = await client.get("/health", headers={"X-Request-ID": "bad id with spaces"})
    assert response.headers["x-request-id"] != "bad id with spaces"


@pytest.mark.parametrize("api_key", [None, "", "wrong-key"])
async def test_business_endpoints_require_api_key(client: AsyncClient, api_key: str | None) -> None:
    headers = {} if api_key is None else {"X-API-Key": api_key}
    client.headers.pop("X-API-Key")
    response = await client.get("/api/v1/payments/0199a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b", headers=headers)
    assert response.status_code == 401
    assert response.headers["content-type"] == "application/problem+json"
    assert response.headers["www-authenticate"] == "ApiKey"
    body = response.json()
    assert body["type"] == "urn:paystream:problem:unauthorized"
    assert body["status"] == 401
    assert body["instance"].startswith("/api/v1/payments/")
    assert body["request_id"] == response.headers["x-request-id"]


async def test_unknown_route_is_problem_details(client: AsyncClient) -> None:
    response = await client.get("/nope")
    assert response.status_code == 404
    assert response.json()["type"] == "urn:paystream:problem:not-found"


@pytest.mark.parametrize(
    "error",
    [RuntimeError("boom"), IntegrityError("INSERT", None, Exception("duplicate key"))],
    ids=["bug", "statement-rejected-by-the-database"],  # the database answered: a 500, not an outage's 503
)
async def test_unhandled_error_is_problem_details_with_request_id(settings: Settings, error: Exception) -> None:
    app = create_app(settings)

    @app.get("/boom")
    async def boom() -> None:
        raise error

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        response = await http.get("/boom", headers={"X-Request-ID": "trace-500"})
    assert response.status_code == 500
    assert response.headers["content-type"] == "application/problem+json"
    assert response.headers["x-request-id"] == "trace-500"
    body = response.json()
    assert body["type"] == "urn:paystream:problem:internal-error"
    assert body["request_id"] == "trace-500"


def test_app_refuses_to_start_without_api_keys(settings: Settings) -> None:
    with pytest.raises(RuntimeError, match="API_KEYS"):
        create_app(settings.model_copy(update={"api_keys": []}))
