from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from fastapi import FastAPI
from opentelemetry import context, propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from paystream import __version__
from paystream.core.config import Settings
from paystream.db.database import Database

# Probes and scrapes. The patterns are searched in the whole URL, host included, so they are anchored on the path:
# a bare "metrics" would also drop every request sent to a host called metrics.example.com.
_EXCLUDED_URLS = "/health$,/ready$,/metrics$"


def configure_tracing(service: str, settings: Settings) -> TracerProvider | None:
    """Install a global tracer provider exporting OTLP/HTTP, or do nothing when tracing is disabled."""
    if not settings.otel_enabled:
        return None
    provider = TracerProvider(
        resource=Resource.create({"service.name": f"paystream-{service}", "service.version": __version__})
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_traces_endpoint))
    )
    trace.set_tracer_provider(provider)
    HTTPXClientInstrumentor().instrument()
    return provider


def instrument_fastapi(app: FastAPI) -> None:
    FastAPIInstrumentor.instrument_app(app, excluded_urls=_EXCLUDED_URLS)


def instrument_database(db: Database) -> None:
    # skip_dep_check: the instrumentation declares sqlalchemy < 2.1 and on 2.1 would refuse to run, only logging an
    # error. Its engine hooks work there (test_database_statements_become_spans). Drop it once 2.1 is supported.
    SQLAlchemyInstrumentor().instrument(engine=db.engine.sync_engine, skip_dep_check=True)


def inject_trace_headers(carrier: dict[str, str]) -> dict[str, str]:
    """Add W3C `traceparent`/`tracestate` for the current span, if any.

    The trace context only: the global propagator would add the client's `baggage` too, up to 8 KB of it in every
    outbox row and message.
    """
    TraceContextTextMapPropagator().inject(carrier)
    return carrier


@contextmanager
def attached_context(headers: Mapping[str, str]) -> Iterator[None]:
    """Make the trace context stored in `headers` current for the duration of the block."""
    token = context.attach(propagate.extract(dict(headers)))
    try:
        yield
    finally:
        context.detach(token)
