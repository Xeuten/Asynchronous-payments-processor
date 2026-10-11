"""Consumer очереди payments.new (запуск: `faststream run app.consumer.main:app`)."""

import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage

from app.broker.broker import create_broker
from app.broker.topology import declare_topology, payments_exchange, payments_new_queue
from app.consumer.gateway import PaymentGatewayEmulator
from app.consumer.processor import PaymentProcessor
from app.consumer.webhook import WebhookSender
from app.core.config import get_settings
from app.core.logging import setup_logging
from app.db.session import engine, session_factory
from app.schemas.events import PaymentCreatedEvent

setup_logging()
logger = logging.getLogger(__name__)

broker = create_broker()
app = FastStream(broker)

http_client = httpx.AsyncClient(timeout=get_settings().webhook_timeout_seconds)
processor = PaymentProcessor(
    session_factory=session_factory,
    gateway=PaymentGatewayEmulator(),
    webhook_sender=WebhookSender(http_client),
)


@app.on_startup
async def on_startup() -> None:
    # Объявляем DLX/DLQ до старта подписчика
    await broker.connect()
    await declare_topology(broker)


@app.after_shutdown
async def on_shutdown() -> None:
    await http_client.aclose()
    await engine.dispose()


@broker.subscriber(
    payments_new_queue,
    payments_exchange,
    ack_policy=AckPolicy.MANUAL,
)
async def handle_payment_created(event: PaymentCreatedEvent, msg: RabbitMessage) -> None:
    try:
        await processor.process(event)
    except Exception:
        # После исчерпания повторов (или при неожиданной ошибке) сообщение
        # отклоняется без возврата в очередь, и RabbitMQ перекладывает его в DLQ
        logger.exception("Failed to handle payment %s, sending to DLQ", event.payment_id)
        await msg.nack(requeue=False)
        return
    await msg.ack()
