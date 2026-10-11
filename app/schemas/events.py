import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from app.models.enums import Currency, PaymentStatus


class PaymentCreatedEvent(BaseModel):
    """Сообщение в очереди payments.new."""

    payment_id: uuid.UUID


class PaymentWebhook(BaseModel):
    """Тело webhook-уведомления клиенту."""

    event: str
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None
