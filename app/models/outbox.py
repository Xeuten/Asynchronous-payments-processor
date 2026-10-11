from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.types import JSONType


class OutboxEvent(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer(), "sqlite"), primary_key=True, autoincrement=True
    )
    event_type: Mapped[str] = mapped_column(String(100))
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        # Частичный индекс: relay ищет только неопубликованные события
        Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where=published_at.is_(None),
        ),
    )
