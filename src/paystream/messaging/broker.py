from faststream.rabbit import Channel, RabbitBroker

from paystream.core.config import Settings


def build_broker(settings: Settings) -> RabbitBroker:
    # Publisher confirms: `publish` returns only after RabbitMQ has the message.
    # on_return_raises: an unroutable message is an error, not a silent drop.
    channel = Channel(
        prefetch_count=settings.consumer_prefetch,
        publisher_confirms=True,
        on_return_raises=True,
    )
    return RabbitBroker(settings.rabbitmq_url, default_channel=channel, graceful_timeout=30.0)
