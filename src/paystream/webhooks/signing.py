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
    # surrogatepass: verify() feeds network text through here, and encoding it must never raise.
    signed_content = f"{msg_id}.{timestamp}.".encode(errors="surrogatepass") + body
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
    """Raise InvalidSignatureError unless one of the space-separated signatures matches.

    Id, timestamp and signature come from the network: whatever they contain, only InvalidSignatureError is raised.
    """
    try:
        sent_at = int(timestamp)
        skew = abs((time.time() if now is None else now) - sent_at)  # a digit string beyond float range overflows here
    except (ValueError, OverflowError) as exc:
        raise InvalidSignatureError("invalid timestamp") from exc
    if skew > tolerance_seconds:
        raise InvalidSignatureError("timestamp outside tolerance")
    expected = sign(secret, msg_id=msg_id, timestamp=sent_at, body=body).split(",", 1)[1].encode()
    for candidate in signature_header.split():
        version, _, value = candidate.partition(",")
        # Bytes, not str: compare_digest raises TypeError on a non-ASCII str.
        if version == SIGNATURE_VERSION and hmac.compare_digest(value.encode(errors="surrogatepass"), expected):
            return
    raise InvalidSignatureError("no matching signature")


def generate_secret() -> str:
    return SECRET_PREFIX + base64.b64encode(secrets.token_bytes(32)).decode()
