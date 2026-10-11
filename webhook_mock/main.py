"""Тестовый приёмник webhook'ов для локальной проверки.

POST /webhook       -> 200, запрос логируется и сохраняется в памяти
POST /webhook/fail  -> 500 всегда (для проверки retry и DLQ)
GET  /webhook/received -> список полученных уведомлений
"""

import logging
from typing import Any

from fastapi import FastAPI, HTTPException, Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("webhook-mock")

app = FastAPI(title="Webhook mock")
received: list[dict[str, Any]] = []


@app.post("/webhook")
async def webhook(request: Request) -> dict[str, str]:
    body = await request.json()
    received.append(body)
    logger.info("Webhook received: %s", body)
    return {"status": "ok"}


@app.post("/webhook/fail")
async def webhook_fail(request: Request) -> None:
    logger.info("Webhook rejected on purpose: %s", await request.json())
    raise HTTPException(status_code=500, detail="Simulated failure")


@app.get("/webhook/received")
async def list_received() -> list[dict[str, Any]]:
    return received
