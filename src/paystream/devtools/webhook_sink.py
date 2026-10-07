"""Demo webhook receiver for `make demo` and the e2e tests. Shows how a client verifies paystream webhooks.

POST /webhooks/ok     verify the signature, answer 204
POST /webhooks/fail   verify, answer 503 every time (drives retries into the DLQ)
POST /webhooks/flaky  verify, answer 503 on the first delivery of each webhook-id, then 204
GET  /received        the last 200 verified deliveries (filter with ?payment_id=)
"""

import json
import logging
import os
from collections import Counter, deque
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import FastAPI, Request, Response

from paystream.core.logging import configure_logging
from paystream.webhooks.signing import InvalidSignatureError, verify

logger = logging.getLogger(__name__)


def create_sink(secret: str) -> FastAPI:
    app = FastAPI(title="Paystream webhook sink", docs_url=None, redoc_url=None)
    received: deque[dict[str, Any]] = deque(maxlen=200)
    deliveries: Counter[str] = Counter()

    @app.post("/webhooks/{mode}")
    async def receive(mode: Literal["ok", "fail", "flaky"], request: Request) -> Response:
        body = await request.body()
        webhook_id = request.headers.get("webhook-id", "")
        try:
            verify(
                secret,
                msg_id=webhook_id,
                timestamp=request.headers.get("webhook-timestamp", ""),
                signature_header=request.headers.get("webhook-signature", ""),
                body=body,
            )
        except InvalidSignatureError as exc:
            logger.warning("rejected webhook %s: %s", webhook_id, exc)
            return Response(status_code=401)

        deliveries[webhook_id] += 1
        payload = json.loads(body)
        received.append(
            {
                "webhook_id": webhook_id,
                "mode": mode,
                "type": payload.get("type"),
                "payment_id": (payload.get("data") or {}).get("id"),
                "delivery": deliveries[webhook_id],
                "received_at": datetime.now(UTC).isoformat(),
            }
        )
        logger.info(
            "webhook %s (%s) delivery #%d in %s mode", webhook_id, payload.get("type"), deliveries[webhook_id], mode
        )
        if mode == "fail" or (mode == "flaky" and deliveries[webhook_id] == 1):
            return Response(status_code=503)
        return Response(status_code=204)

    @app.get("/received")
    async def list_received(payment_id: str | None = None) -> list[dict[str, Any]]:
        return [r for r in received if payment_id is None or r["payment_id"] == payment_id]

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def create_app_from_env() -> FastAPI:  # pragma: no cover — process entry point
    configure_logging("webhook-sink")
    return create_sink(os.environ["WEBHOOK_SIGNING_SECRET"])
