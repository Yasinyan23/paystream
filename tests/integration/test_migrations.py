import asyncio

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Connection, inspect
from sqlalchemy.ext.asyncio import create_async_engine

from paystream.db import models  # noqa: F401 — registers tables on Base.metadata
from paystream.db.base import Base
from tests.conftest import alembic_config

pytestmark = pytest.mark.integration


def _diff(connection: Connection) -> list[object]:
    context = MigrationContext.configure(connection, opts={"compare_type": True})
    return list(compare_metadata(context, Base.metadata))


async def table_names(url: str) -> set[str]:
    engine = create_async_engine(url)
    async with engine.connect() as connection:
        names = await connection.run_sync(lambda c: set(inspect(c).get_table_names()))
    await engine.dispose()
    return names


async def test_migration_matches_models(migrated_db_url: str) -> None:
    engine = create_async_engine(migrated_db_url)
    async with engine.connect() as connection:
        tables = await connection.run_sync(lambda c: set(inspect(c).get_table_names()))
        diff = await connection.run_sync(_diff)
    await engine.dispose()
    assert {"payments", "outbox", "webhook_attempts", "alembic_version"} <= tables
    assert diff == []


async def test_downgrade_and_upgrade_again(migrated_db_url: str) -> None:
    config = alembic_config(migrated_db_url)
    # env.py runs its own event loop (asyncio.run), so Alembic must not be called on the test's loop.
    await asyncio.to_thread(command.downgrade, config, "base")
    assert await table_names(migrated_db_url) == {"alembic_version"}
    await asyncio.to_thread(command.upgrade, config, "head")
    assert {"payments", "outbox", "webhook_attempts"} <= await table_names(migrated_db_url)
