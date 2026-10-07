from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from prometheus_client import REGISTRY
from pydantic import SecretStr
from sqlalchemy import text
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer

from paystream.api.app import create_app
from paystream.core.config import Settings
from paystream.db.database import Database

ROOT = Path(__file__).resolve().parents[1]
API_KEY = "test-api-key"
# Secret from the Standard Webhooks reference test vector (see tests/unit/test_webhooks.py).
WEBHOOK_SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"


def alembic_config(url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", url)
    config.attributes["configure_logger"] = False
    return config


def sample(name: str, **labels: str) -> float:
    """One Prometheus sample from the process-wide registry; 0 until it is first touched. Assert on deltas."""
    return REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:18-alpine", driver=None) as container:
        yield container.get_connection_url().replace("postgresql://", "postgresql+asyncpg://", 1)


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    with RabbitMqContainer("rabbitmq:4-management-alpine") as container:
        host, port = container.get_container_host_ip(), container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture(scope="session")
def migrated_db_url(postgres_url: str) -> str:
    command.upgrade(alembic_config(postgres_url), "head")
    return postgres_url


@pytest.fixture
def settings(migrated_db_url: str) -> Settings:
    return Settings(
        _env_file=None,
        database_url=migrated_db_url,
        rabbitmq_url="amqp://guest:guest@127.0.0.1:1/",  # tests that need RabbitMQ override this
        api_keys=[SecretStr(API_KEY)],
        webhook_signing_secret=SecretStr(WEBHOOK_SECRET),
        webhook_allowed_hosts=["hooks.test"],
        gateway_min_delay=0.0,
        gateway_max_delay=0.0,
        retry_base_delay_ms=300,
        outbox_poll_interval=0.2,
    )


@pytest_asyncio.fixture
async def db(settings: Settings) -> AsyncIterator[Database]:
    database = Database(settings.database_url)
    async with database.engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox, webhook_attempts CASCADE"))
    yield database
    await database.dispose()


@pytest_asyncio.fixture
async def client(settings: Settings, db: Database) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test", headers={"X-API-Key": API_KEY}) as http:
            yield http
