"""Топология RabbitMQ.

payments (direct) --payments.new--> [payments.new] --nack--> payments.dlx --> [payments.new.dlq]
"""

from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

PAYMENTS_NEW_ROUTING_KEY = "payments.new"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
payments_dlx = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

payments_new_queue = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        # Отклонённые (nack без requeue) сообщения RabbitMQ перекладывает в DLX
        "x-dead-letter-exchange": payments_dlx.name,
        "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
    },
)

payments_dlq = RabbitQueue(
    "payments.new.dlq",
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
)


async def declare_topology(broker) -> None:
    """Объявляет exchange'и и очереди и связывает их (операция идемпотентна)."""
    exchange = await broker.declare_exchange(payments_exchange)
    dlx = await broker.declare_exchange(payments_dlx)

    queue = await broker.declare_queue(payments_new_queue)
    await queue.bind(exchange, routing_key=payments_new_queue.routing_key)

    dlq = await broker.declare_queue(payments_dlq)
    await dlq.bind(dlx, routing_key=payments_dlq.routing_key)
