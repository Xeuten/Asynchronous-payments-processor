import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

from app.models.enums import Currency, PaymentStatus

Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]


class PaymentCreate(BaseModel):
    amount: Amount
    currency: Currency
    description: str | None = Field(default=None, max_length=1000)
    metadata: dict[str, Any] | None = None
    webhook_url: AnyHttpUrl


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] | None = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
