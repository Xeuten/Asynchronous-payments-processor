from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def add(self, event_type: str, routing_key: str, payload: dict[str, Any]) -> None:
        self.session.add(
            OutboxEvent(event_type=event_type, routing_key=routing_key, payload=payload)
        )

    async def lock_unpublished(self, limit: int) -> list[OutboxEvent]:
        """Берёт пачку неопубликованных событий с блокировкой строк.

        SKIP LOCKED позволяет нескольким экземплярам relay работать параллельно,
        не публикуя одно и то же событие одновременно.
        """
        result = await self.session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars())

    @staticmethod
    def mark_published(event: OutboxEvent) -> None:
        event.published_at = datetime.now(UTC)
