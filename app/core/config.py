from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str = "super-secret-api-key"
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Outbox relay
    outbox_relay_enabled: bool = True
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 100

    # Эмуляция платёжного шлюза
    gateway_min_delay_seconds: float = 2.0
    gateway_max_delay_seconds: float = 5.0
    gateway_success_rate: float = 0.9

    # Webhook
    webhook_timeout_seconds: float = 5.0
    webhook_max_attempts: int = 3
    webhook_retry_base_delay_seconds: float = 1.0

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
