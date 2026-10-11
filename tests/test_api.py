import uuid

from sqlalchemy import func, select

from app.models import OutboxEvent, Payment
from tests.conftest import API_HEADERS

PAYMENT = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "https://example.com/webhook",
}


def headers(key: str = "key-1") -> dict[str, str]:
    return {**API_HEADERS, "Idempotency-Key": key}


async def count(session_factory, model) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def test_requires_api_key(client):
    response = await client.post("/api/v1/payments", json=PAYMENT)
    assert response.status_code == 401

    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


async def test_create_payment_writes_payment_and_outbox_event(client, db):
    response = await client.post("/api/v1/payments", json=PAYMENT, headers=headers())

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert set(body) == {"payment_id", "status", "created_at"}

    async with db() as session:
        event = await session.scalar(select(OutboxEvent))
    assert event.payload == {"payment_id": body["payment_id"]}
    assert event.routing_key == "payments.new"
    assert event.published_at is None


async def test_same_idempotency_key_returns_same_payment(client, db):
    first = await client.post("/api/v1/payments", json=PAYMENT, headers=headers())
    second = await client.post("/api/v1/payments", json=PAYMENT, headers=headers())

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count(db, Payment) == 1
    assert await count(db, OutboxEvent) == 1


async def test_same_idempotency_key_with_other_body_conflicts(client):
    await client.post("/api/v1/payments", json=PAYMENT, headers=headers())
    response = await client.post(
        "/api/v1/payments", json={**PAYMENT, "amount": "1.00"}, headers=headers()
    )
    assert response.status_code == 409


async def test_idempotency_key_is_required(client):
    response = await client.post("/api/v1/payments", json=PAYMENT, headers=API_HEADERS)
    assert response.status_code == 422


async def test_validation(client):
    for patch in ({"amount": "0"}, {"currency": "GBP"}, {"webhook_url": "not-a-url"}):
        response = await client.post(
            "/api/v1/payments", json={**PAYMENT, **patch}, headers=headers()
        )
        assert response.status_code == 422, patch


async def test_get_payment(client):
    created = (await client.post("/api/v1/payments", json=PAYMENT, headers=headers())).json()

    response = await client.get(f"/api/v1/payments/{created['payment_id']}", headers=API_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == created["payment_id"]
    assert body["amount"] == "100.50"
    assert body["metadata"] == {"order_id": 1}
    assert body["idempotency_key"] == "key-1"
    assert body["processed_at"] is None


async def test_get_unknown_payment(client):
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=API_HEADERS)
    assert response.status_code == 404
