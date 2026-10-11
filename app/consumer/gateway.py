import asyncio
import random

from app.core.config import get_settings
from app.models import PaymentStatus


class PaymentGatewayEmulator:
    """Эмуляция внешнего платёжного шлюза: 2–5 с, 90 % успех."""

    def __init__(
        self,
        min_delay: float | None = None,
        max_delay: float | None = None,
        success_rate: float | None = None,
    ) -> None:
        settings = get_settings()
        self.min_delay = settings.gateway_min_delay_seconds if min_delay is None else min_delay
        self.max_delay = settings.gateway_max_delay_seconds if max_delay is None else max_delay
        self.success_rate = settings.gateway_success_rate if success_rate is None else success_rate

    async def charge(self) -> PaymentStatus:
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
        if random.random() < self.success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
