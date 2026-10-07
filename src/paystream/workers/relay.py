import asyncio
import contextlib
import logging
import signal
import time
from datetime import timedelta

import asyncpg
from sqlalchemy.engine import make_url

from paystream.core.config import Settings, get_settings
from paystream.db.database import Database
from paystream.messaging.broker import build_broker
from paystream.messaging.topology import declare_topology
from paystream.repositories.outbox import OUTBOX_NOTIFY_CHANNEL
from paystream.services.outbox_relay import OutboxRelay

logger = logging.getLogger(__name__)
PURGE_EVERY_SECONDS = 3600


def _asyncpg_dsn(database_url: str) -> str:
    return make_url(database_url).set(drivername="postgresql").render_as_string(hide_password=False)


async def _wait_any(events: tuple[asyncio.Event, ...], seconds: float) -> None:
    waiters = [asyncio.create_task(event.wait()) for event in events]
    try:
        await asyncio.wait(waiters, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()


async def run_relay(settings: Settings, stop: asyncio.Event) -> None:
    db = Database(settings.database_url)
    broker = build_broker(settings)
    await broker.connect()
    await declare_topology(broker, settings)
    relay = OutboxRelay(db, broker, batch_size=settings.outbox_batch_size, max_backoff=settings.outbox_max_backoff)

    wakeup = asyncio.Event()
    listener = await asyncpg.connect(_asyncpg_dsn(settings.database_url))
    await listener.add_listener(OUTBOX_NOTIFY_CHANNEL, lambda *_: wakeup.set())
    logger.info(
        "outbox relay started (poll every %.1fs, LISTEN %s)", settings.outbox_poll_interval, OUTBOX_NOTIFY_CHANNEL
    )

    last_purge = float("-inf")  # time.monotonic() has an arbitrary origin, so 0.0 would delay the first purge
    try:
        while not stop.is_set():
            wakeup.clear()
            try:
                await relay.drain()
                if time.monotonic() - last_purge > PURGE_EVERY_SECONDS:
                    purged = await relay.purge_published(timedelta(days=settings.outbox_retention_days))
                    last_purge = time.monotonic()
                    if purged:
                        logger.info("outbox purged %d published event(s)", purged)
            except Exception:
                logger.exception("outbox relay iteration failed")
            # NOTIFY gives near-zero latency; the timeout is the safety net if a notification is lost.
            await _wait_any((wakeup, stop), seconds=settings.outbox_poll_interval)
    finally:
        with contextlib.suppress(Exception):
            await listener.close()
        await broker.stop()
        await db.dispose()
        logger.info("outbox relay stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await run_relay(get_settings(), stop)

    asyncio.run(_run())


if __name__ == "__main__":
    main()
