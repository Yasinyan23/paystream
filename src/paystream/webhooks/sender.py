import asyncio
import json
import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from paystream import __version__
from paystream.core.config import Settings
from paystream.webhooks.signing import sign
from paystream.webhooks.ssrf import Resolver, UnsafeWebhookURLError, ensure_public_url

_MAX_ERROR_CHARS = 1024


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    ok: bool
    status_code: int | None
    error: str | None
    duration_ms: int
    permanent: bool = False


def build_http_client(settings: Settings) -> httpx.AsyncClient:
    timeout = httpx.Timeout(settings.webhook_read_timeout, connect=settings.webhook_connect_timeout)
    # A redirect or a proxy taken from the environment would send the request somewhere the SSRF check never looked.
    return httpx.AsyncClient(timeout=timeout, follow_redirects=False, trust_env=False)


async def _read_excerpt(response: httpx.Response) -> str:
    """Up to _MAX_ERROR_CHARS body bytes, undecoded: the receiver is untrusted and may stream or inflate forever."""
    excerpt = b""
    async for chunk in response.aiter_raw():
        excerpt += chunk
        if len(excerpt) >= _MAX_ERROR_CHARS:
            break
    return excerpt[:_MAX_ERROR_CHARS].decode(errors="replace")


class WebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        secret: str,
        allowed_hosts: Collection[str] = (),
        resolver: Resolver | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client, self._secret = client, secret
        self._allowed_hosts, self._resolver, self._clock = allowed_hosts, resolver, clock
        connect, read = client.timeout.connect, client.timeout.read
        # One budget for the whole delivery. The client's timeouts apply per operation, so a receiver that drips
        # a byte every few seconds would otherwise hold the worker for as long as it likes.
        self._budget = None if connect is None or read is None else connect + read

    async def deliver(self, url: str, *, event_id: str, payload: Mapping[str, Any]) -> DeliveryResult:
        started = time.perf_counter()

        def result(ok: bool, status: int | None, error: str | None, *, permanent: bool = False) -> DeliveryResult:
            elapsed = int((time.perf_counter() - started) * 1000)
            text = error.replace("\x00", "")[:_MAX_ERROR_CHARS] if error else None  # Postgres text rejects NUL
            return DeliveryResult(ok, status, text, elapsed, permanent)

        try:
            async with asyncio.timeout(self._budget):
                try:
                    await ensure_public_url(url, allowed_hosts=self._allowed_hosts, resolver=self._resolver)
                except UnsafeWebhookURLError as exc:
                    return result(False, None, f"unsafe_url: {exc}", permanent=True)
                except OSError as exc:
                    return result(False, None, f"dns_error: {exc}")

                body = json.dumps(payload, separators=(",", ":"), default=str).encode()
                timestamp = int(self._clock())
                headers = {
                    "content-type": "application/json",
                    "user-agent": f"paystream-webhooks/{__version__}",
                    "accept-encoding": "identity",  # the error excerpt is read as it came off the wire
                    "webhook-id": event_id,
                    "webhook-timestamp": str(timestamp),
                    "webhook-signature": sign(self._secret, msg_id=event_id, timestamp=timestamp, body=body),
                }
                try:
                    async with self._client.stream("POST", url, content=body, headers=headers) as response:
                        # Read for a 2xx too: a short body read to its end lets the connection go back to the pool.
                        excerpt = await _read_excerpt(response)
                        if response.is_success:
                            return result(True, response.status_code, None)
                        return result(False, response.status_code, f"HTTP {response.status_code}: {excerpt}")
                except httpx.HTTPError as exc:
                    return result(False, None, f"{type(exc).__name__}: {exc}")
        except TimeoutError:
            return result(False, None, f"TimeoutError: no complete response within {self._budget}s")
