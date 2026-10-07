import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid7

import httpx
import pytest
import respx
from pydantic import ValidationError

from paystream.db.database import Database
from paystream.domain.events import PAYMENT_CREATED, EventEnvelope, PaymentCreatedData
from paystream.domain.payment import Currency, PaymentStatus
from paystream.gateway.simulated import ChargeResult
from paystream.repositories.payments import PaymentRepository
from paystream.repositories.webhook_attempts import WebhookAttemptRepository
from paystream.services.payments import PaymentCommand, PaymentService
from paystream.services.processing import PaymentProcessor, PermanentProcessingError, TransientProcessingError
from paystream.webhooks.sender import WebhookSender
from paystream.webhooks.signing import verify
from tests.conftest import WEBHOOK_SECRET

pytestmark = pytest.mark.integration
HOOK = "https://hooks.test/payments"


class FakeGateway:
    def __init__(self, *results: ChargeResult) -> None:
        self.results = list(results)
        self.calls = 0

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        self.calls += 1
        return self.results.pop(0)


async def create_payment(db: Database, *, webhook_url: str | None = HOOK) -> UUID:
    command = PaymentCommand(
        amount=Decimal("10.00"), currency=Currency.USD, description=None, metadata=None, webhook_url=webhook_url
    )
    result = await PaymentService(db).create(command, idempotency_key=str(uuid7()))
    return result.payment.id


def event_for(payment_id: UUID) -> EventEnvelope:
    data = PaymentCreatedData(payment_id=payment_id, amount=Decimal("10.00"), currency=Currency.USD, webhook_url=HOOK)
    return EventEnvelope(
        event_id=uuid7(), event_type=PAYMENT_CREATED, occurred_at=datetime.now(UTC), data=data.model_dump(mode="json")
    )


async def load(db: Database, payment_id: UUID):  # type: ignore[no-untyped-def]
    async with db.sessionmaker() as session:
        payment = await PaymentRepository(session).get(payment_id)
        summary = await WebhookAttemptRepository(session).summary(payment_id)
    return payment, summary


@pytest.fixture
async def http():  # type: ignore[no-untyped-def]
    async with httpx.AsyncClient() as client:
        yield client


def processor(db: Database, gateway: FakeGateway, http: httpx.AsyncClient) -> PaymentProcessor:
    sender = WebhookSender(http, secret=WEBHOOK_SECRET, allowed_hosts=["hooks.test"])
    return PaymentProcessor(db, gateway, sender)


@respx.mock
async def test_success_updates_status_and_sends_signed_webhook(db: Database, http: httpx.AsyncClient) -> None:
    route = respx.post(HOOK).mock(return_value=httpx.Response(200))
    payment_id = await create_payment(db)
    await processor(db, FakeGateway(ChargeResult(True)), http).process(event_for(payment_id))

    payment, summary = await load(db, payment_id)
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert summary.attempts == 1
    request = route.calls.last.request
    verify(
        WEBHOOK_SECRET,
        msg_id=request.headers["webhook-id"],
        timestamp=request.headers["webhook-timestamp"],
        signature_header=request.headers["webhook-signature"],
        body=request.content,
    )
    body = httpx.Response(200, content=request.content).json()
    assert body["type"] == "payment.succeeded"
    assert body["data"]["id"] == str(payment_id)
    assert body["data"]["amount"] == "10.00"


@respx.mock
async def test_decline_is_business_outcome(db: Database, http: httpx.AsyncClient) -> None:
    respx.post(HOOK).mock(return_value=httpx.Response(200))
    payment_id = await create_payment(db)
    await processor(db, FakeGateway(ChargeResult(False, "card_declined")), http).process(event_for(payment_id))
    payment, _ = await load(db, payment_id)
    assert (payment.status, payment.failure_reason) == (PaymentStatus.FAILED, "card_declined")


@respx.mock
async def test_redelivery_after_success_does_nothing(db: Database, http: httpx.AsyncClient) -> None:
    route = respx.post(HOOK).mock(return_value=httpx.Response(200))
    payment_id = await create_payment(db)
    gateway = FakeGateway(ChargeResult(True))
    worker = processor(db, gateway, http)
    await worker.process(event_for(payment_id))
    await worker.process(event_for(payment_id))  # duplicate delivery
    assert gateway.calls == 1
    assert route.call_count == 1


@respx.mock
async def test_webhook_failure_is_transient_and_retry_resends_only_webhook(
    db: Database, http: httpx.AsyncClient
) -> None:
    route = respx.post(HOOK).mock(side_effect=[httpx.Response(500), httpx.Response(200)])
    payment_id = await create_payment(db)
    gateway = FakeGateway(ChargeResult(True))
    worker = processor(db, gateway, http)

    with pytest.raises(TransientProcessingError, match="HTTP 500"):
        await worker.process(event_for(payment_id))
    payment, summary = await load(db, payment_id)
    assert payment.status is PaymentStatus.SUCCEEDED  # status is kept even though the webhook failed
    assert (payment.webhook_delivered_at, summary.attempts, summary.last_status_code) == (None, 1, 500)

    await worker.process(event_for(payment_id))  # the retry
    payment, summary = await load(db, payment_id)
    assert gateway.calls == 1
    assert payment.webhook_delivered_at is not None
    assert summary.attempts == 2
    first, second = route.calls[0].request, route.calls[1].request
    assert first.headers["webhook-id"] == second.headers["webhook-id"]  # stable id for receiver dedup


async def test_payment_without_webhook(db: Database, http: httpx.AsyncClient) -> None:
    payment_id = await create_payment(db, webhook_url=None)
    await processor(db, FakeGateway(ChargeResult(True)), http).process(event_for(payment_id))
    payment, summary = await load(db, payment_id)
    assert payment.status is PaymentStatus.SUCCEEDED
    assert summary.attempts == 0


async def test_unknown_payment_is_permanent(db: Database, http: httpx.AsyncClient) -> None:
    with pytest.raises(PermanentProcessingError) as error:
        await processor(db, FakeGateway(), http).process(event_for(uuid7()))
    assert error.value.reason == "payment_not_found"


async def test_unsafe_webhook_is_permanent(db: Database, http: httpx.AsyncClient) -> None:
    payment_id = await create_payment(db, webhook_url="https://internal.example/hook")

    async def private_resolver(host: str, port: int) -> list[str]:
        return ["10.0.0.7"]

    sender = WebhookSender(http, secret=WEBHOOK_SECRET, resolver=private_resolver)
    worker = PaymentProcessor(db, FakeGateway(ChargeResult(True)), sender)
    with pytest.raises(PermanentProcessingError) as error:
        await worker.process(event_for(payment_id))
    assert error.value.reason == "webhook_rejected"
    payment, summary = await load(db, payment_id)
    assert payment.status is PaymentStatus.SUCCEEDED
    assert summary.last_error is not None
    assert summary.last_error.startswith("unsafe_url")


async def test_unsupported_event_is_permanent(db: Database, http: httpx.AsyncClient) -> None:
    event = event_for(uuid7()).model_copy(update={"event_type": "payment.refunded"})
    with pytest.raises(PermanentProcessingError) as error:
        await processor(db, FakeGateway(), http).process(event)
    assert error.value.reason == "unsupported_event"


async def test_malformed_event_data_is_permanent_and_touches_nothing(db: Database, http: httpx.AsyncClient) -> None:
    payment_id = await create_payment(db)
    gateway = FakeGateway(ChargeResult(True))
    data = {"amount": "10.00", "currency": "USD", "webhook_url": HOOK}  # a valid event minus its payment_id
    event = event_for(payment_id).model_copy(update={"data": data})
    with pytest.raises(PermanentProcessingError) as error:
        await processor(db, gateway, http).process(event)
    assert error.value.reason == "invalid_event"
    assert isinstance(error.value.__cause__, ValidationError)  # the original stays chained for the logs
    payment, summary = await load(db, payment_id)
    assert (gateway.calls, payment.status, summary.attempts) == (0, PaymentStatus.PENDING, 0)


class RendezvousGateway(FakeGateway):
    """Holds each caller until a second one arrives, so both consumers saw `pending` before either answers."""

    def __init__(self, *results: ChargeResult) -> None:
        super().__init__(*results)
        self._barrier = asyncio.Barrier(2)

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        result = await super().charge(payment_id, amount, currency)
        await self._barrier.wait()
        return result


@respx.mock
async def test_concurrent_duplicates_agree_on_one_outcome(db: Database, http: httpx.AsyncClient) -> None:
    route = respx.post(HOOK).mock(return_value=httpx.Response(200))
    payment_id = await create_payment(db)
    gateway = RendezvousGateway(ChargeResult(True), ChargeResult(False, "card_declined"))  # the copies disagree
    worker = processor(db, gateway, http)

    async with asyncio.timeout(10):  # a regression that serialises the copies would wait on the barrier forever
        await asyncio.gather(worker.process(event_for(payment_id)), worker.process(event_for(payment_id)))

    payment, _ = await load(db, payment_id)
    assert gateway.calls == 2  # both copies saw `pending` and asked the gateway
    # the copy that lost the conditional UPDATE reports the stored outcome, not its own gateway answer
    sent = {json.loads(call.request.content)["type"] for call in route.calls}
    assert sent == {f"payment.{payment.status}"}
    assert len({call.request.headers["webhook-id"] for call in route.calls}) == 1


@respx.mock
async def test_webhook_body_for_a_declined_payment(db: Database, http: httpx.AsyncClient) -> None:
    route = respx.post(HOOK).mock(return_value=httpx.Response(200))
    command = PaymentCommand(
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="Order #42",
        metadata={"order_id": 42},
        webhook_url=HOOK,
    )
    created = (await PaymentService(db).create(command, idempotency_key=str(uuid7()))).payment
    await processor(db, FakeGateway(ChargeResult(False, "card_declined")), http).process(event_for(created.id))

    payment, _ = await load(db, created.id)
    assert payment.processed_at is not None
    assert json.loads(route.calls.last.request.content) == {
        "type": "payment.failed",
        "timestamp": payment.processed_at.isoformat(),
        "data": {
            "id": str(created.id),
            "amount": "1500.00",
            "currency": "RUB",
            "status": "failed",
            "failure_reason": "card_declined",
            "description": "Order #42",
            "metadata": {"order_id": 42},
            "created_at": payment.created_at.isoformat(),
            "processed_at": payment.processed_at.isoformat(),
        },
    }


@respx.mock
async def test_receiver_text_with_nul_is_recorded(db: Database, http: httpx.AsyncClient) -> None:
    respx.post(HOOK).mock(return_value=httpx.Response(500, content=b"bad\x00gateway"))
    payment_id = await create_payment(db)
    with pytest.raises(TransientProcessingError, match="HTTP 500"):
        await processor(db, FakeGateway(ChargeResult(True)), http).process(event_for(payment_id))
    _, summary = await load(db, payment_id)
    assert (summary.attempts, summary.last_status_code) == (1, 500)  # a Postgres text column cannot hold the NUL
