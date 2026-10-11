"""Outbox relay: переносит события из таблицы outbox в RabbitMQ.

Гарантия at-least-once: событие помечается опубликованным только после
подтверждения от брокера (publisher confirms) в той же транзакции, что и
блокировка строки. При сбое между publish и commit событие будет отправлено
повторно, поэтому consumer идемпотентен.
"""

import asyncio
import logging

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker.topology import declare_topology, payments_exchange
from app.core.config import get_settings
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        broker: RabbitBroker,
        session_factory: async_sessionmaker[AsyncSession],
        poll_interval: float | None = None,
        batch_size: int | None = None,
    ) -> None:
        settings = get_settings()
        self.broker = broker
        self.session_factory = session_factory
        self.poll_interval = poll_interval or settings.outbox_poll_interval_seconds
        self.batch_size = batch_size or settings.outbox_batch_size
        self._connected = False
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        logger.info("Outbox relay started")
        while not self._stopping.is_set():
            published = 0
            try:
                await self._ensure_connected()
                published = await self.publish_batch()
            except Exception as exc:
                # Брокер или БД недоступны: события остаются в outbox до следующей итерации
                logger.warning("Outbox relay iteration failed, will retry: %r", exc)
            # Если пачка была полной, сразу берём следующую, иначе ждём
            if published < self.batch_size:
                try:
                    await asyncio.wait_for(self._stopping.wait(), self.poll_interval)
                except TimeoutError:
                    pass
        logger.info("Outbox relay stopped")

    def stop(self) -> None:
        self._stopping.set()

    async def _ensure_connected(self) -> None:
        if not self._connected:
            await self.broker.connect()
            await declare_topology(self.broker)
            self._connected = True
            logger.info("Outbox relay connected to RabbitMQ")

    async def publish_batch(self) -> int:
        async with self.session_factory() as session, session.begin():
            repo = OutboxRepository(session)
            events = await repo.lock_unpublished(self.batch_size)
            for event in events:
                await self.broker.publish(
                    event.payload,
                    exchange=payments_exchange,
                    routing_key=event.routing_key,
                    persist=True,
                    message_id=str(event.id),
                    message_type=event.event_type,
                )
                repo.mark_published(event)
                logger.info("Published outbox event %s (%s)", event.id, event.event_type)
            return len(events)
