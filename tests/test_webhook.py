import uuid
from decimal import Decimal

import httpx
import pytest

from app.consumer.webhook import WebhookDeliveryError, WebhookSender
from app.models import Currency, PaymentStatus
from app.schemas.events import PaymentWebhook

PAYLOAD = PaymentWebhook(
    event="payment.succeeded",
    payment_id=uuid.uuid4(),
    status=PaymentStatus.SUCCEEDED,
    amount=Decimal("10.00"),
    currency=Currency.USD,
    processed_at=None,
)


def make_sender(statuses: list[int], calls: list[httpx.Request]) -> WebhookSender:
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(statuses[len(calls) - 1])

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return WebhookSender(client, max_attempts=3, base_delay=0)


async def test_webhook_retries_until_success():
    calls: list[httpx.Request] = []
    sender = make_sender([500, 503, 200], calls)

    await sender.send("https://example.com/hook", PAYLOAD)

    assert len(calls) == 3


async def test_webhook_gives_up_after_max_attempts():
    calls: list[httpx.Request] = []
    sender = make_sender([500, 500, 500], calls)

    with pytest.raises(WebhookDeliveryError):
        await sender.send("https://example.com/hook", PAYLOAD)

    assert len(calls) == 3


async def test_webhook_uses_exponential_backoff(monkeypatch):
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr("app.consumer.webhook.asyncio.sleep", fake_sleep)
    sender = make_sender([500, 500, 500], [])
    sender.base_delay = 1

    with pytest.raises(WebhookDeliveryError):
        await sender.send("https://example.com/hook", PAYLOAD)

    assert delays == [1, 2]
