from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from paystream.core.config import Settings

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENT_CREATED_KEY = "payment.created"
DEAD_LETTER_KEY = "payment.dead"
PAYMENTS_QUEUE_NAME = "payments.new"
DEAD_QUEUE_NAME = "payments.dead"

# Classic durable queue on purpose: listing the DLQ re-queues messages, and a quorum queue
# would count every listing toward its delivery limit and eventually drop them.
DEAD_QUEUE = RabbitQueue(DEAD_QUEUE_NAME, durable=True, routing_key=DEAD_LETTER_KEY)


def payments_queue(delivery_limit: int) -> RabbitQueue:
    """Main work queue. Quorum: replicated, and `x-delivery-limit` stops crash loops by dead-lettering."""
    return RabbitQueue(
        PAYMENTS_QUEUE_NAME,
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=PAYMENT_CREATED_KEY,
        arguments={
            "x-delivery-limit": delivery_limit,
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
            "x-dead-letter-routing-key": DEAD_LETTER_KEY,
        },
    )


def retry_routing_key(delay_ms: int) -> str:
    return f"retry.{delay_ms}"


def _label(delay_ms: int) -> str:
    return f"{delay_ms // 1000}s" if delay_ms % 1000 == 0 else f"{delay_ms}ms"


def retry_queue(delay_ms: int) -> RabbitQueue:
    """Hold-off queue: messages expire after `delay_ms` and dead-letter back to payments.new.

    One queue per delay tier, because per-message TTL in a single queue only expires
    at the head of the queue (a long delay would block shorter ones behind it).
    """
    return RabbitQueue(
        f"payments.retry.{_label(delay_ms)}",
        durable=True,
        routing_key=retry_routing_key(delay_ms),
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENT_CREATED_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Declare every exchange, queue and binding. Safe to call from every process, any number of times, as long as
    they all use the same DELIVERY_LIMIT: RabbitMQ refuses to declare payments.new again with another
    `x-delivery-limit` (PRECONDITION_FAILED), and the process fails to start."""
    bindings = [
        (payments_queue(settings.delivery_limit), PAYMENTS_EXCHANGE),
        (DEAD_QUEUE, DEAD_LETTER_EXCHANGE),
        *((retry_queue(delay), RETRY_EXCHANGE) for delay in settings.retry_delays_ms),
    ]
    for queue, exchange in bindings:
        declared_exchange = await broker.declare_exchange(exchange)
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(declared_exchange, routing_key=queue.routing_key)
