import asyncio
import logging

import httpx

from app.core.config import get_settings
from app.schemas.events import PaymentWebhook

logger = logging.getLogger(__name__)


class WebhookDeliveryError(Exception):
    pass


class WebhookSender:
    """Отправляет webhook с повторами и экспоненциальной задержкой (1 с, 2 с, 4 с…)."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        max_attempts: int | None = None,
        base_delay: float | None = None,
    ) -> None:
        settings = get_settings()
        self.client = client
        self.max_attempts = max_attempts or settings.webhook_max_attempts
        self.base_delay = (
            settings.webhook_retry_base_delay_seconds if base_delay is None else base_delay
        )

    async def send(self, url: str, payload: PaymentWebhook) -> None:
        body = payload.model_dump(mode="json")
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = await self.client.post(url, json=body)
                response.raise_for_status()
                logger.info("Webhook for %s delivered (attempt %d)", payload.payment_id, attempt)
                return
            except httpx.HTTPError as exc:
                logger.warning(
                    "Webhook for %s failed (attempt %d/%d): %r",
                    payload.payment_id,
                    attempt,
                    self.max_attempts,
                    exc,
                )
                if attempt == self.max_attempts:
                    raise WebhookDeliveryError(
                        f"Webhook to {url} failed after {attempt} attempts"
                    ) from exc
                await asyncio.sleep(self.base_delay * 2 ** (attempt - 1))
