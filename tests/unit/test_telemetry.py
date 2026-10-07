import pytest
from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.trace import TracerProvider

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
