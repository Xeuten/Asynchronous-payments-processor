import hashlib
import json
import uuid

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.topology import PAYMENTS_NEW_ROUTING_KEY
from app.models import Payment, PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payment import PaymentCreate

PAYMENT_CREATED_EVENT = "payment.created"


class IdempotencyConflictError(Exception):
    """Idempotency-Key уже использован с другим телом запроса."""


class PaymentNotFoundError(Exception):
    pass


def _request_hash(data: PaymentCreate) -> str:
    raw = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        """Создаёт платёж и событие outbox в одной транзакции.

        Повторный запрос с тем же ключом и телом возвращает уже созданный платёж.
        """
        request_hash = _request_hash(data)

        existing = await self.payments.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return self._check_replay(existing, request_hash)

        payment = Payment(
            id=uuid.uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            webhook_url=str(data.webhook_url),
        )
        self.payments.add(payment)
        self.outbox.add(
            event_type=PAYMENT_CREATED_EVENT,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            payload=PaymentCreatedEvent(payment_id=payment.id).model_dump(mode="json"),
        )
        try:
            await self.session.commit()
        except IntegrityError:
            # Гонка: параллельный запрос с тем же ключом успел закоммитить раньше
            await self.session.rollback()
            existing = await self.payments.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            return self._check_replay(existing, request_hash)

        await self.session.refresh(payment)
        return payment

    async def get(self, payment_id: uuid.UUID) -> Payment:
        payment = await self.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    @staticmethod
    def _check_replay(existing: Payment, request_hash: str) -> Payment:
        if existing.request_hash != request_hash:
            raise IdempotencyConflictError
        return existing
