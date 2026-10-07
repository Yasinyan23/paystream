import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest
import structlog
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from paystream.api.app import create_app
from paystream.core.config import Settings
from paystream.core.logging import configure_logging
from paystream.domain.events import PAYMENT_CREATED
from paystream.workers.consumer import handle_delivery


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
    structlog.reset_defaults()


def logged(capsys: pytest.CaptureFixture[str]) -> dict[str, dict[str, object]]:
    """The JSON lines written so far, by event."""
    return {record["event"]: record for record in map(json.loads, capsys.readouterr().out.splitlines())}


def api_settings() -> Settings:
    return Settings(_env_file=None, api_keys=[SecretStr("key")], log_json=True, log_level="INFO")


def test_stdlib_logs_become_json_with_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("consumer", json_logs=True, level="INFO")
    structlog.contextvars.bind_contextvars(payment_id="p-1", attempt=2)
    try:
        logging.getLogger("paystream.test").info("payment %s processed", "p-1")
    finally:
        structlog.contextvars.clear_contextvars()
    line = capsys.readouterr().out.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "payment p-1 processed"
    assert record["service"] == "consumer"
    assert record["payment_id"] == "p-1"
    assert record["attempt"] == 2
    assert record["level"] == "info"
    assert record["logger"] == "paystream.test"
    assert "timestamp" in record


def test_level_filtering(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("api", json_logs=True, level="WARNING")
    logging.getLogger("paystream.test").info("hidden")
    logging.getLogger("paystream.test").warning("shown")
    out = capsys.readouterr().out
    assert "hidden" not in out
    assert "shown" in out


def test_console_output_when_json_is_off(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("api", json_logs=False)
    logging.getLogger("paystream.test").info("hello")
    out = capsys.readouterr().out
    assert "hello" in out
    with pytest.raises(json.JSONDecodeError):
        json.loads(out)


async def test_api_lines_carry_the_request_id_and_only_during_the_request(capsys: pytest.CaptureFixture[str]) -> None:
    app = create_app(api_settings())

    @app.get("/ping")
    async def ping() -> None:
        logging.getLogger("paystream.test").info("in handler")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        await http.get("/ping", headers={"X-Request-ID": "trace-1"})

    record = logged(capsys)["in handler"]
    assert record["request_id"] == "trace-1"
    assert record["service"] == "api"
    assert "request_id" not in structlog.contextvars.get_contextvars()  # ASGITransport runs the app in this task


async def test_unhandled_error_line_keeps_the_request_id(capsys: pytest.CaptureFixture[str]) -> None:
    app = create_app(api_settings())

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("boom")

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        await http.get("/boom", headers={"X-Request-ID": "trace-500"})

    # The 500 handler runs in ServerErrorMiddleware, after RequestIdMiddleware has unwound.
    assert logged(capsys)["unhandled error on GET /boom"]["request_id"] == "trace-500"


async def test_consumer_lines_carry_the_message_context_only_during_the_delivery(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("consumer", json_logs=True, level="INFO")
    body = {
        "event_id": str(uuid7()),
        "event_type": PAYMENT_CREATED,
        "occurred_at": datetime.now(UTC).isoformat(),
        "data": {"payment_id": "p-1"},
    }
    message = SimpleNamespace(
        headers={"x-attempt": 2}, message_id="m-1", correlation_id=None, ack=AsyncMock(), reject=AsyncMock()
    )

    await handle_delivery(SimpleNamespace(process=AsyncMock()), None, body=body, message=message)

    message.ack.assert_awaited_once()
    record = logged(capsys)["message m-1 attempt 2 processed"]
    assert (record["message_id"], record["payment_id"], record["attempt"]) == ("m-1", "p-1", 2)
    assert structlog.contextvars.get_contextvars().keys().isdisjoint({"message_id", "payment_id", "attempt"})
