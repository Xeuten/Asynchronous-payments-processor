import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import PaymentGatewayEmulator
from app.consumer.webhook import WebhookSender
from app.models import PaymentStatus
from app.repositories.payment import PaymentRepository
from app.schemas.events import PaymentCreatedEvent, PaymentWebhook

logger = logging.getLogger(__name__)


class PaymentNotFoundError(Exception):
    pass


class PaymentProcessor:
    """Обработка события payments.new: шлюз -> статус в БД -> webhook."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGatewayEmulator,
        webhook_sender: WebhookSender,
    ) -> None:
        self.session_factory = session_factory
        self.gateway = gateway
        self.webhook_sender = webhook_sender

    async def process(self, event: PaymentCreatedEvent) -> None:
        async with self.session_factory() as session:
            payment = await PaymentRepository(session).get(event.payment_id)
        if payment is None:
            raise PaymentNotFoundError(event.payment_id)

        # Идемпотентность: повторное сообщение не «списывает» деньги ещё раз,
        # а только повторяет отправку webhook
        if payment.status == PaymentStatus.PENDING:
            result = await self.gateway.charge()
            async with self.session_factory() as session, session.begin():
                updated = await PaymentRepository(session).finalize(payment.id, result)
            if updated:
                logger.info("Payment %s processed: %s", payment.id, result)
            else:
                logger.info("Payment %s was already processed concurrently", payment.id)
        else:
            logger.info("Payment %s already %s, skipping gateway", payment.id, payment.status)

        async with self.session_factory() as session:
            payment = await PaymentRepository(session).get(event.payment_id)

        await self.webhook_sender.send(
            payment.webhook_url,
            PaymentWebhook(
                event=f"payment.{payment.status}",
                payment_id=payment.id,
                status=payment.status,
                amount=payment.amount,
                currency=payment.currency,
                processed_at=payment.processed_at,
            ),
        )
