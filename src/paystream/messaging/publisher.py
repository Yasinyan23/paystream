from collections.abc import Mapping
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitExchange


async def publish_json(
    broker: RabbitBroker,
    body: Any,
    *,
    exchange: RabbitExchange,
    routing_key: str,
    message_id: str,
    correlation_id: str | None = None,
    headers: Mapping[str, Any] | None = None,
) -> None:
    """Persistent, mandatory, confirmed publish. Raises if RabbitMQ does not accept the message."""
    await broker.publish(
        body,
        exchange=exchange,
        routing_key=routing_key,
        message_id=message_id,
        correlation_id=correlation_id,
        headers=dict(headers or {}),
        persist=True,
        mandatory=True,
        timeout=10.0,
    )
