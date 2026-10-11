import os
import tempfile
from pathlib import Path

# Настройки задаются до импорта приложения: тесты используют SQLite и не требуют
# запущенных PostgreSQL/RabbitMQ
_db_path = Path(tempfile.gettempdir()) / "payments_test.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{_db_path}"
os.environ["API_KEY"] = "test-key"
os.environ["OUTBOX_RELAY_ENABLED"] = "false"

import httpx  # noqa: E402
import pytest  # noqa: E402

from app.db.base import Base  # noqa: E402
from app.db.session import engine, session_factory  # noqa: E402
from app.main import app  # noqa: E402

API_HEADERS = {"X-API-Key": "test-key"}


@pytest.fixture(autouse=True)
async def db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield session_factory
    await engine.dispose()


@pytest.fixture
async def client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
