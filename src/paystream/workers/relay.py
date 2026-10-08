import asyncio
import contextlib
import logging
import signal
import time
from datetime import timedelta

import asyncpg
from prometheus_client import start_http_server
from sqlalchemy.engine import make_url

from paystream.core.config import Settings, get_settings
from paystream.core.logging import configure_logging
from paystream.core.telemetry import configure_tracing
from paystream.db.database import Database
from paystream.messaging.broker import build_broker, telemetry_middlewares
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


async def _listen(settings: Settings, wakeup: asyncio.Event, stop: asyncio.Event) -> asyncpg.Connection:
    connection = await asyncpg.connect(_asyncpg_dsn(settings.database_url))
    await connection.add_listener(OUTBOX_NOTIFY_CHANNEL, lambda *_: wakeup.set())

    def lost(_: asyncpg.Connection) -> None:
        if not stop.is_set():  # asyncpg calls this when run_relay closes the connection too
            logger.warning("LISTEN connection lost, reconnecting")
        wakeup.set()  # reconnect now, not after the rest of a possibly long poll interval

    connection.add_termination_listener(lost)
    return connection


async def run_relay(settings: Settings, stop: asyncio.Event) -> None:
    db = Database(settings.database_url)
    broker = build_broker(settings, middlewares=telemetry_middlewares(settings))
    await broker.connect()
    await declare_topology(broker, settings)
    relay = OutboxRelay(db, broker, batch_size=settings.outbox_batch_size, max_backoff=settings.outbox_max_backoff)

    wakeup = asyncio.Event()
    listener = await _listen(settings, wakeup, stop)
    logger.info(
        "outbox relay started (poll every %.1fs, LISTEN %s)", settings.outbox_poll_interval, OUTBOX_NOTIFY_CHANNEL
    )

    last_purge = float("-inf")  # time.monotonic() has an arbitrary origin, so 0.0 would delay the first purge
    warned = False  # one warning per failed-reconnect streak, not one per poll
    try:
        while not stop.is_set():
            if listener.is_closed():  # a database restart kills LISTEN; until it is back, the poll is all there is
                try:
                    listener = await _listen(settings, wakeup, stop)
                except Exception as exc:  # noqa: BLE001 — however the database is down, keep polling and retry
                    if not warned:
                        logger.warning("LISTEN reconnect failed, polling until it succeeds: %r", exc)
                    warned = True
                else:
                    warned = False
                    logger.info("LISTEN re-established")  # the drain below picks up whatever was missed meanwhile
            wakeup.clear()
            try:
                await relay.drain()
                await relay.refresh_pending_gauge()
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


def main() -> None:  # pragma: no cover
    settings = get_settings()
    configure_logging("outbox-relay", json_logs=settings.log_json, level=settings.log_level)
    configure_tracing("outbox-relay", settings)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await run_relay(settings, stop)

    start_http_server(settings.metrics_port)
    asyncio.run(_run())


if __name__ == "__main__":
    main()
