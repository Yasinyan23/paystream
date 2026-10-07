import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import create_async_engine

from paystream.core.config import get_settings
from paystream.db import models  # noqa: F401 — registers tables on Base.metadata
from paystream.db.base import Base

config = context.config
if config.config_file_name and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> str:
    return config.get_main_option("sqlalchemy.url") or get_settings().database_url


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def _run_online() -> None:
    engine = create_async_engine(_database_url(), poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


if context.is_offline_mode():
    context.configure(url=_database_url(), target_metadata=target_metadata, literal_binds=True, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(_run_online())
