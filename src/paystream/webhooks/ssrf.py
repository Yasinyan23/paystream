import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Collection

import httpx

Resolver = Callable[[str, int], Awaitable[list[str]]]


class UnsafeWebhookURLError(ValueError):
    """The URL violates the outbound policy. Permanent: retrying will not help."""


async def _system_resolver(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


async def ensure_public_url(
    url: str,
    *,
    allowed_hosts: Collection[str] = (),
    resolver: Resolver | None = None,
) -> None:
    """Allow only http(s) URLs whose host resolves exclusively to public addresses.

    Checked right before every delivery, so a host that is re-pointed to an internal address
    is caught on the next attempt. DNS failures raise OSError and are treated as transient;
    a URL that cannot be parsed, or a host that cannot be encoded for DNS, can never resolve and is rejected.
    """
    try:
        parsed = httpx.URL(url)
        host = parsed.host  # decoding an xn-- host happens here and can fail too
    except (httpx.InvalidURL, UnicodeError) as exc:
        raise UnsafeWebhookURLError(f"invalid URL: {exc}") from exc
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeWebhookURLError(f"scheme {parsed.scheme!r} is not allowed")
    if not host:
        raise UnsafeWebhookURLError("URL has no host")
    if host in allowed_hosts:
        return
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        addresses = await (resolver or _system_resolver)(host, port)
    except UnicodeError as exc:  # an empty or over-long DNS label (the API accepts them) is a ValueError, not OSError
        raise UnsafeWebhookURLError(f"{host!r} is not a valid DNS name") from exc
    for address in addresses:
        ip = ipaddress.ip_address(address.split("%", 1)[0])
        if not ip.is_global:
            raise UnsafeWebhookURLError(f"{host!r} resolves to non-public address {ip}")
