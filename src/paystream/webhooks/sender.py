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
    return httpx.AsyncClient(timeout=timeout, follow_redirects=False)


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

    async def deliver(self, url: str, *, event_id: str, payload: Mapping[str, Any]) -> DeliveryResult:
        started = time.perf_counter()

        def result(ok: bool, status: int | None, error: str | None, *, permanent: bool = False) -> DeliveryResult:
            elapsed = int((time.perf_counter() - started) * 1000)
            return DeliveryResult(ok, status, error[:_MAX_ERROR_CHARS] if error else None, elapsed, permanent)

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
            "webhook-id": event_id,
            "webhook-timestamp": str(timestamp),
            "webhook-signature": sign(self._secret, msg_id=event_id, timestamp=timestamp, body=body),
        }
        try:
            response = await self._client.post(url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            return result(False, None, f"{type(exc).__name__}: {exc}")
        if response.is_success:
            return result(True, response.status_code, None)
        return result(False, response.status_code, f"HTTP {response.status_code}: {response.text}")
