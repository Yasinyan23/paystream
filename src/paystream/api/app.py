from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from paystream import __version__
from paystream.api.errors import install_error_handlers
from paystream.api.middleware import RequestIdMiddleware
from paystream.api.routes.health import router as health_router
from paystream.api.routes.payments import router as payments_router
from paystream.core.config import Settings, get_settings
from paystream.db.database import Database

DESCRIPTION = """
Accepts payments, processes them asynchronously through an emulated gateway and notifies clients
with signed webhooks. Guarantees: idempotent creation, transactional outbox, at-least-once
processing with an idempotent consumer, exponential retries and a dead-letter queue.
"""


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    if not settings.api_keys:
        raise RuntimeError("API_KEYS must contain at least one key")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.db = Database(settings.database_url)
        try:
            yield
        finally:
            await app.state.db.dispose()

    app = FastAPI(title="Paystream", version=__version__, description=DESCRIPTION, lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    install_error_handlers(app)
    app.include_router(health_router)
    app.include_router(payments_router)
    return app
