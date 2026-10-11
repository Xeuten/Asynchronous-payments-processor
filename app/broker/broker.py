from faststream.rabbit import RabbitBroker

from app.core.config import get_settings


def create_broker() -> RabbitBroker:
    return RabbitBroker(get_settings().rabbitmq_url)
