import pytest
from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from pydantic import SecretStr

from paystream.api.app import create_app
from paystream.core.config import Settings
from paystream.core.telemetry import attached_context, configure_tracing, inject_trace_headers


def test_inject_and_attach_round_trip() -> None:
    tracer = TracerProvider().get_tracer("test")
    with tracer.start_as_current_span("api-request") as span:
        headers = inject_trace_headers({"correlation_id": "c-1"})
        trace_id = span.get_span_context().trace_id
    assert headers["correlation_id"] == "c-1"
    assert headers["traceparent"].startswith("00-")

    with attached_context(headers):
        restored = trace.get_current_span().get_span_context()
    assert restored.trace_id == trace_id


def test_inject_without_active_span_adds_nothing() -> None:
    assert inject_trace_headers({}) == {}


def test_tracing_stays_off_unless_enabled() -> None:
    assert configure_tracing("api", Settings(_env_file=None, otel_enabled=False)) is None


def test_enabled_tracing_names_the_service_and_traces_httpx(monkeypatch: pytest.MonkeyPatch) -> None:
    # The global provider can be set once per process: leave it to the tests that need an SDK one.
    monkeypatch.setattr(trace, "set_tracer_provider", lambda provider: None)
    provider = configure_tracing("consumer", Settings(_env_file=None, otel_enabled=True))
    assert provider is not None
    try:
        assert provider.resource.attributes["service.name"] == "paystream-consumer"
        assert HTTPXClientInstrumentor().is_instrumented_by_opentelemetry
    finally:
        HTTPXClientInstrumentor().uninstrument()
        provider.shutdown()


async def test_fastapi_leaves_trace_export_to_configure_tracing(monkeypatch: pytest.MonkeyPatch) -> None:
    # FastAPI reads the OTLP variable that Settings reads too and, unless told not to, adds an exporter of its own to
    # the global provider: every API span would be exported twice, and tracing would start even with OTEL_ENABLED=false.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://collector.test:4318/v1/traces")
    provider = TracerProvider()
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    app = create_app(Settings(_env_file=None, api_keys=[SecretStr("k")]))
    events = iter([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])  # its startup is where FastAPI looks

    async def receive() -> dict[str, str]:
        return next(events)

    async def send(message: dict[str, str]) -> None:
        pass

    await app({"type": "lifespan"}, receive, send)
    assert provider._active_span_processor._span_processors == ()
