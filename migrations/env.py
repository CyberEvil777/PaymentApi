import asyncio
import os

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from app.models import Base


def migrate(connection):
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def online():
    engine = create_async_engine(os.environ["DATABASE_URL"])
    async with engine.connect() as connection:
        await connection.run_sync(migrate)
    await engine.dispose()


if context.config.attributes.get("connection") is not None:
    migrate(context.config.attributes["connection"])
elif context.is_offline_mode():
    context.configure(
        url=os.environ["DATABASE_URL"], target_metadata=Base.metadata, literal_binds=True
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(online())
