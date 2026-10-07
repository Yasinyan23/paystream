import asyncio
from collections.abc import Iterator
from typing import cast
from uuid import UUID

import httpx
import pytest
import respx
from faststream import TestApp
from httpx import AsyncClient
from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import select

from paystream.core.config import Settings
from paystream.core.telemetry import instrument_database
from paystream.db.database import Database
from paystream.db.models import OutboxEvent
from paystream.workers.consumer import build_consumer
from paystream.workers.relay import run_relay

pytestmark = pytest.mark.integration
WEBHOOK_URL = "https://hooks.test/payments"


@pytest.fixture(scope="module", autouse=True)
def sdk_tracer_provider() -> None:
    # The default no-op provider passes a parent context on but records no spans, and the span assertions below need
    # some. set_tracer_provider only takes effect once per process, which is fine for every other test.
    trace.set_tracer_provider(TracerProvider())


@pytest.fixture
def spans() -> Iterator[InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    cast(TracerProvider, trace.get_tracer_provider()).add_span_processor(SimpleSpanProcessor(exporter))
    yield exporter
    exporter.shutdown()  # the provider keeps its processors for good: a stopped exporter drops what comes later


@pytest.fixture
def instrumented() -> Iterator[None]:
    """The process-wide instrumentation a consumer turns on, and off again afterwards: httpx (which `configure_tracing`
    does) and SQLAlchemy (which `build_consumer` does)."""
    HTTPXClientInstrumentor().instrument()
    yield
    HTTPXClientInstrumentor().uninstrument()
    SQLAlchemyInstrumentor().uninstrument()


async def test_outbox_row_carries_the_request_trace(client: AsyncClient, db: Database) -> None:
    tracer = TracerProvider().get_tracer("test")
    with tracer.start_as_current_span("client-call") as span:
        headers = {"Idempotency-Key": "trace-1", "traceparent": _traceparent(span)}
        response = await client.post("/api/v1/payments", json={"amount": "1.00", "currency": "EUR"}, headers=headers)
    payment_id = UUID(response.json()["payment_id"])
    async with db.sessionmaker() as session:
        event = await session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == payment_id))
    assert event is not None
    trace_id = format(span.get_span_context().trace_id, "032x")
    assert event.headers["traceparent"].split("-")[1] == trace_id


@respx.mock
async def test_the_trace_runs_from_the_api_to_the_webhook(
    client: AsyncClient, settings: Settings, rabbitmq_url: str, instrumented: None, spans: InMemorySpanExporter
) -> None:
    requests: list[httpx.Request] = []
    delivered = asyncio.Event()

    def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        delivered.set()
        return httpx.Response(200)

    respx.post(WEBHOOK_URL).mock(side_effect=receive)
    traced = settings.model_copy(
        update={"rabbitmq_url": rabbitmq_url, "otel_enabled": True, "gateway_success_rate": 1.0}
    )
    stop = asyncio.Event()
    relay = asyncio.create_task(run_relay(traced, stop))
    tracer = TracerProvider().get_tracer("test")
    try:
        async with httpx.AsyncClient() as http, TestApp(build_consumer(traced, http_client=http)):
            with tracer.start_as_current_span("client-call") as span:
                headers = {"Idempotency-Key": "trace-2", "traceparent": _traceparent(span)}
                body = {"amount": "1.00", "currency": "EUR", "webhook_url": WEBHOOK_URL}
                await client.post("/api/v1/payments", json=body, headers=headers)
            await asyncio.wait_for(delivered.wait(), timeout=15)
    finally:
        stop.set()
        await asyncio.wait_for(relay, timeout=10)
    trace_id = span.get_span_context().trace_id
    # The request's trace id reaches the webhook only through the outbox row, the relay, RabbitMQ and the consumer.
    assert requests[0].headers["traceparent"].split("-")[1] == f"{trace_id:032x}"
    in_trace = [s for s in spans.get_finished_spans() if s.context.trace_id == trace_id]
    assert trace.SpanKind.PRODUCER in {s.kind for s in in_trace}  # the relay's publish
    assert "postgresql" in {(s.attributes or {}).get("db.system") for s in in_trace}  # the consumer's queries


async def test_database_statements_become_spans(db: Database, spans: InMemorySpanExporter) -> None:
    instrument_database(db)
    try:
        await db.ping()
    finally:
        SQLAlchemyInstrumentor().uninstrument()
    assert "SELECT" in {span.name for span in spans.get_finished_spans()}


def _traceparent(span: trace.Span) -> str:
    context = span.get_span_context()
    return f"00-{context.trace_id:032x}-{context.span_id:016x}-01"
