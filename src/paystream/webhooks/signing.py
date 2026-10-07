"""Webhook signatures per the Standard Webhooks specification (https://www.standardwebhooks.com)."""

import base64
import hashlib
import hmac
import secrets
import time

SIGNATURE_VERSION = "v1"
SECRET_PREFIX = "whsec_"  # noqa: S105 — key prefix, not a secret


class InvalidSignatureError(Exception):
    pass


def _key(secret: str) -> bytes:
    return base64.b64decode(secret.removeprefix(SECRET_PREFIX))


def sign(secret: str, *, msg_id: str, timestamp: int, body: bytes) -> str:
    signed_content = f"{msg_id}.{timestamp}.".encode() + body
    digest = hmac.new(_key(secret), signed_content, hashlib.sha256).digest()
    return f"{SIGNATURE_VERSION},{base64.b64encode(digest).decode()}"


def verify(
    secret: str,
    *,
    msg_id: str,
    timestamp: str,
    signature_header: str,
    body: bytes,
    tolerance_seconds: int = 300,
    now: float | None = None,
) -> None:
    """Raise InvalidSignatureError unless one of the space-separated signatures matches."""
    try:
        sent_at = int(timestamp)
    except ValueError as exc:
        raise InvalidSignatureError("invalid timestamp") from exc
    current = time.time() if now is None else now
    if abs(current - sent_at) > tolerance_seconds:
        raise InvalidSignatureError("timestamp outside tolerance")
    expected = sign(secret, msg_id=msg_id, timestamp=sent_at, body=body).split(",", 1)[1]
    for candidate in signature_header.split():
        version, _, value = candidate.partition(",")
        if version == SIGNATURE_VERSION and hmac.compare_digest(value, expected):
            return
    raise InvalidSignatureError("no matching signature")


def generate_secret() -> str:
    return SECRET_PREFIX + base64.b64encode(secrets.token_bytes(32)).decode()
