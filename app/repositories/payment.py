import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        result = await self.session.execute(select(Payment).where(Payment.idempotency_key == key))
        return result.scalar_one_or_none()

    def add(self, payment: Payment) -> None:
        self.session.add(payment)

    async def finalize(self, payment_id: uuid.UUID, status: PaymentStatus) -> bool:
        """Переводит платёж из pending в финальный статус.

        Возвращает False, если платёж уже был обработан раньше (повторное сообщение).
        """
        result = await self.session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=datetime.now(UTC))
        )
        return result.rowcount == 1
