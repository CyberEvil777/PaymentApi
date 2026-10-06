import os
from uuid import uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import create_app
from app.config import Settings

API_KEY = "test-key-with-at-least-16-chars"
BODY = {
    "amount": "12.34",
    "currency": "RUB",
    "description": "Test payment",
    "metadata": {"order": "42"},
    "webhook_url": "http://receiver.test/hook",
}
HEADERS = {"X-API-Key": API_KEY, "Idempotency-Key": "test-key"}


@pytest.fixture
async def sessions():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to a real PostgreSQL test database")
    schema = "test_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})

    def upgrade(connection):
        config = Config("alembic.ini")
        config.attributes["connection"] = connection
        command.upgrade(config, "head")

    try:
        async with engine.begin() as connection:
            await connection.run_sync(upgrade)
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
async def client(sessions):
    app = create_app(Settings(api_key=API_KEY), sessions)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client
