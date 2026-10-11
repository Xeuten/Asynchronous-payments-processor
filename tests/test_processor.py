import uuid
from decimal import Decimal

import pytest

from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.models import Currency, Payment, PaymentStatus
from app.schemas.events import PaymentCreatedEvent, PaymentWebhook


class FakeGateway:
    def __init__(self, result: PaymentStatus) -> None:
        self.result = result
        self.calls = 0

    async def charge(self) -> PaymentStatus:
        self.calls += 1
        return self.result


class FakeWebhookSender:
    def __init__(self) -> None:
        self.sent: list[tuple[str, PaymentWebhook]] = []

    async def send(self, url: str, payload: PaymentWebhook) -> None:
        self.sent.append((url, payload))


async def create_payment(session_factory) -> uuid.UUID:
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.EUR,
        status=PaymentStatus.PENDING,
        idempotency_key=str(uuid.uuid4()),
        request_hash="x",
        webhook_url="https://example.com/hook",
    )
    async with session_factory() as session, session.begin():
        session.add(payment)
    return payment.id


async def test_processes_payment_and_sends_webhook(db):
    payment_id = await create_payment(db)
    gateway, webhook = FakeGateway(PaymentStatus.FAILED), FakeWebhookSender()

    await PaymentProcessor(db, gateway, webhook).process(PaymentCreatedEvent(payment_id=payment_id))

    async with db() as session:
        payment = await session.get(Payment, payment_id)
    assert payment.status == PaymentStatus.FAILED
    assert payment.processed_at is not None
    [(url, body)] = webhook.sent
    assert url == "https://example.com/hook"
    assert body.event == "payment.failed"
    assert body.payment_id == payment_id


async def test_duplicate_message_does_not_charge_twice(db):
    payment_id = await create_payment(db)
    gateway, webhook = FakeGateway(PaymentStatus.SUCCEEDED), FakeWebhookSender()
    processor = PaymentProcessor(db, gateway, webhook)
    event = PaymentCreatedEvent(payment_id=payment_id)

    await processor.process(event)
    await processor.process(event)

    assert gateway.calls == 1
    assert len(webhook.sent) == 2


async def test_unknown_payment_raises(db):
    processor = PaymentProcessor(db, FakeGateway(PaymentStatus.SUCCEEDED), FakeWebhookSender())
    with pytest.raises(PaymentNotFoundError):
        await processor.process(PaymentCreatedEvent(payment_id=uuid.uuid4()))
