import logging
from collections.abc import Sequence
from typing import Any

from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.opentelemetry import RabbitTelemetryMiddleware
from faststream.types import BrokerMiddleware
from opentelemetry import trace

from paystream.core.config import Settings


def telemetry_middlewares(settings: Settings) -> list[BrokerMiddleware[Any, Any]]:
    """The broker middlewares that trace publishes and deliveries; none unless tracing is enabled."""
    if not settings.otel_enabled:
        return []
    return [RabbitTelemetryMiddleware(tracer_provider=trace.get_tracer_provider())]


def build_broker(settings: Settings, *, middlewares: Sequence[BrokerMiddleware[Any, Any]] = ()) -> RabbitBroker:
    # Publisher confirms: `publish` returns only after RabbitMQ has the message.
    # on_return_raises: an unroutable message is an error, not a silent drop.
    channel = Channel(
        prefetch_count=settings.consumer_prefetch,
        publisher_confirms=True,
        on_return_raises=True,
    )
    # Its own logger: FastStream's default one has propagate=False and a colorized stdout handler, which would
    # bypass the JSON logging setup.
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=channel,
        graceful_timeout=30.0,
        logger=logging.getLogger("paystream.broker"),
        middlewares=middlewares,
    )
