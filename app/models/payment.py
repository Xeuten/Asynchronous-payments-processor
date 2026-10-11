import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import DateTime, Enum, Numeric, String, Text, UniqueConstraint, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import Currency, PaymentStatus
from app.models.types import JSONType


def _enum(enum_cls: type, name: str) -> Enum:
    return Enum(enum_cls, name=name, values_callable=lambda e: [m.value for m in e])


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(_enum(Currency, "currency"))
    description: Mapped[str | None] = mapped_column(Text)
    # "metadata" зарезервировано в DeclarativeBase, поэтому атрибут называется metadata_
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONType)
    status: Mapped[PaymentStatus] = mapped_column(
        _enum(PaymentStatus, "payment_status"), default=PaymentStatus.PENDING, index=True
    )
    idempotency_key: Mapped[str] = mapped_column(String(255))
    # Хеш тела запроса: позволяет отличить повтор от другого запроса с тем же ключом
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),)
