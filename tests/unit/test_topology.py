from paystream.messaging.topology import (
    PAYMENTS_QUEUE_NAME,
    payments_queue,
    retry_queue,
    retry_routing_key,
)


def test_payments_queue_is_quorum_with_delivery_limit() -> None:
    queue = payments_queue(delivery_limit=10)
    assert queue.name == PAYMENTS_QUEUE_NAME == "payments.new"
    assert queue.arguments["x-delivery-limit"] == 10
    assert queue.arguments["x-dead-letter-exchange"] == "payments.dlx"


def test_retry_queue_dead_letters_back_to_main_exchange() -> None:
    queue = retry_queue(2000)
    assert queue.name == "payments.retry.2s"
    assert queue.arguments == {
        "x-queue-type": "classic",
        "x-message-ttl": 2000,
        "x-dead-letter-exchange": "payments",
        "x-dead-letter-routing-key": "payment.created",
    }
    assert retry_routing_key(2000) == "retry.2000"


def test_retry_queue_label_for_non_whole_seconds() -> None:
    assert retry_queue(1500).name == "payments.retry.1500ms"
