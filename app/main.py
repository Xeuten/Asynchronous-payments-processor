import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import router as api_v1_router
from app.broker.broker import create_broker
from app.core.config import get_settings
from app.core.logging import setup_logging
from app.db.session import engine, session_factory
from app.outbox.relay import OutboxRelay

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    relay_task = None
    broker = None
    if get_settings().outbox_relay_enabled:
        broker = create_broker()
        relay = OutboxRelay(broker, session_factory)
        relay_task = asyncio.create_task(relay.run())
    yield
    if relay_task is not None:
        relay.stop()
        with contextlib.suppress(asyncio.CancelledError):
            await relay_task
    if broker is not None:
        await broker.stop()
    await engine.dispose()


app = FastAPI(title="Async Payments Processor", version="1.0.0", lifespan=lifespan)
app.include_router(api_v1_router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
