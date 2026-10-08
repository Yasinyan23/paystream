import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from paystream.core.config import Settings
from paystream.webhooks.sender import WebhookSender, build_http_client
from paystream.webhooks.signing import InvalidSignatureError, generate_secret, sign, verify
from paystream.webhooks.ssrf import UnsafeWebhookURLError, ensure_public_url

# Reference vector used by the Standard Webhooks libraries' own tests.
SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
MSG_ID = "msg_p5jXN8AQM9LWM0D4loKWxJek"
TIMESTAMP = 1614265330
BODY = b'{"test": 2432232314}'
SIGNATURE = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="


def test_sign_matches_reference_vector() -> None:
    assert sign(SECRET, msg_id=MSG_ID, timestamp=TIMESTAMP, body=BODY) == SIGNATURE


def test_verify_accepts_valid_signature_among_several() -> None:
    header = f"v1,bm90LXRoaXMtb25l {SIGNATURE}"
    verify(SECRET, msg_id=MSG_ID, timestamp=str(TIMESTAMP), signature_header=header, body=BODY, now=TIMESTAMP)


@pytest.mark.parametrize(
    ("body", "timestamp", "now"),
    [
        (b'{"test": 1}', str(TIMESTAMP), TIMESTAMP),  # tampered body
        (BODY, str(TIMESTAMP), TIMESTAMP + 301),  # replayed too late
        (BODY, str(TIMESTAMP), TIMESTAMP - 301),  # stamped in the future
        (BODY, "not-a-number", TIMESTAMP),
    ],
)
def test_verify_rejects(body: bytes, timestamp: str, now: float) -> None:
    with pytest.raises(InvalidSignatureError):
        verify(SECRET, msg_id=MSG_ID, timestamp=timestamp, signature_header=SIGNATURE, body=body, now=now)


# Header values come from the network: whatever they hold, the only failure is InvalidSignatureError.
@pytest.mark.parametrize(
    ("msg_id", "timestamp", "signature_header", "now"),
    [
        pytest.param(MSG_ID, str(TIMESTAMP), "v1,é", TIMESTAMP, id="non-ascii-signature"),
        pytest.param(MSG_ID, str(TIMESTAMP), "v1,\ud800", TIMESTAMP, id="lone-surrogate-signature"),
        pytest.param("\ud800", str(TIMESTAMP), SIGNATURE, TIMESTAMP, id="lone-surrogate-msg-id"),
        pytest.param(MSG_ID, "9" * 400, SIGNATURE, None, id="timestamp-beyond-float-range"),
    ],
)
def test_verify_rejects_hostile_headers(msg_id: str, timestamp: str, signature_header: str, now: float | None) -> None:
    with pytest.raises(InvalidSignatureError):
        verify(SECRET, msg_id=msg_id, timestamp=timestamp, signature_header=signature_header, body=BODY, now=now)


def test_generated_secret_roundtrip() -> None:
    secret = generate_secret()
    assert secret.startswith("whsec_")
    signature = sign(secret, msg_id="m", timestamp=1, body=b"{}")
    verify(secret, msg_id="m", timestamp="1", signature_header=signature, body=b"{}", now=1)


def resolver_returning(*addresses: str):  # type: ignore[no-untyped-def]
    async def resolve(host: str, port: int) -> list[str]:
        return list(addresses)

    return resolve


@pytest.mark.parametrize(
    "address",
    ["127.0.0.1", "10.0.0.5", "172.16.0.1", "192.168.1.1", "169.254.169.254", "::1", "fc00::1", "100.64.0.1"],
)
async def test_non_public_addresses_are_rejected(address: str) -> None:
    with pytest.raises(UnsafeWebhookURLError, match="non-public"):
        await ensure_public_url("https://evil.example/hook", resolver=resolver_returning(address))


async def test_public_address_is_accepted() -> None:
    await ensure_public_url("https://example.com/hook", resolver=resolver_returning("93.184.215.14"))


async def test_one_bad_address_poisons_the_host() -> None:
    with pytest.raises(UnsafeWebhookURLError):
        await ensure_public_url("https://mixed.example/", resolver=resolver_returning("93.184.215.14", "10.0.0.1"))


async def test_scheme_and_allowlist() -> None:
    with pytest.raises(UnsafeWebhookURLError, match="scheme"):
        await ensure_public_url("ftp://example.com/x")
    await ensure_public_url("http://webhook-sink:8080/hook", allowed_hosts=["webhook-sink"])  # no DNS needed


async def test_resolver_gets_the_name_httpx_connects_to() -> None:
    seen: list[str] = []

    async def spy(host: str, port: int) -> list[str]:
        seen.append(host)
        return ["93.184.215.14"]

    # The stdlib's IDNA 2003 codec would turn the decoded "faß" into "fass" (a name the attacker may control
    # separately); httpx connects to the IDNA 2008 A-label, and that is the name that has to be checked.
    await ensure_public_url("https://faß.example/", resolver=spy)
    assert seen == ["xn--fa-hia.example"]


def make_sender(client: httpx.AsyncClient) -> WebhookSender:
    return WebhookSender(client, secret=SECRET, allowed_hosts=["hooks.test"], clock=lambda: TIMESTAMP)


@respx.mock
async def test_deliver_signs_the_exact_body() -> None:
    route = respx.post("https://hooks.test/pay").mock(return_value=httpx.Response(204))
    async with httpx.AsyncClient() as client:
        result = await make_sender(client).deliver(
            "https://hooks.test/pay", event_id="evt_1", payload={"type": "payment.succeeded"}
        )
    assert (result.ok, result.status_code, result.error) == (True, 204, None)
    request = route.calls.last.request
    assert request.headers["webhook-id"] == "evt_1"
    assert request.headers["webhook-timestamp"] == str(TIMESTAMP)
    verify(
        SECRET,
        msg_id="evt_1",
        timestamp=request.headers["webhook-timestamp"],
        signature_header=request.headers["webhook-signature"],
        body=request.content,
        now=TIMESTAMP,
    )
    assert json.loads(request.content) == {"type": "payment.succeeded"}


@respx.mock
async def test_deliver_reports_http_errors_as_transient() -> None:
    respx.post("https://hooks.test/pay").mock(return_value=httpx.Response(503, text="maintenance"))
    async with httpx.AsyncClient() as client:
        result = await make_sender(client).deliver("https://hooks.test/pay", event_id="evt_1", payload={})
    assert (result.ok, result.status_code, result.permanent) == (False, 503, False)
    assert result.error == "HTTP 503: maintenance"


@respx.mock
async def test_deliver_reports_timeouts_as_transient() -> None:
    respx.post("https://hooks.test/pay").mock(side_effect=httpx.ConnectTimeout("timed out"))
    async with httpx.AsyncClient() as client:
        result = await make_sender(client).deliver("https://hooks.test/pay", event_id="evt_1", payload={})
    assert (result.ok, result.status_code, result.permanent) == (False, None, False)
    assert result.error is not None
    assert result.error.startswith("ConnectTimeout")


@respx.mock
async def test_redirects_are_not_followed() -> None:
    # The destination was vetted once; a 3xx to an internal address must not be chased.
    redirect = httpx.Response(302, headers={"location": "http://169.254.169.254/latest/meta-data"})
    route = respx.post("https://hooks.test/pay").mock(return_value=redirect)
    async with build_http_client(Settings(_env_file=None)) as client:
        result = await make_sender(client).deliver("https://hooks.test/pay", event_id="evt_1", payload={})
    assert (result.ok, result.status_code, result.permanent, route.call_count) == (False, 302, False, 1)


async def test_http_client_ignores_proxy_environment() -> None:
    # A proxy from HTTP(S)_PROXY would resolve and connect on its own, past the SSRF check.
    async with build_http_client(Settings(_env_file=None)) as client:
        assert client.trust_env is False


@respx.mock
async def test_error_body_is_read_only_up_to_the_cap() -> None:
    served = 0

    async def endless() -> AsyncIterator[bytes]:
        nonlocal served
        for _ in range(1000):
            served += 1
            yield b"x" * 512

    respx.post("https://hooks.test/pay").mock(return_value=httpx.Response(500, content=endless()))
    async with httpx.AsyncClient() as client:
        result = await make_sender(client).deliver("https://hooks.test/pay", event_id="evt_1", payload={})
    assert result.error == "HTTP 500: " + "x" * 1014  # the error text is capped at 1024 characters
    assert served <= 3  # and the receiver's stream is abandoned at the cap, not drained


@pytest.mark.parametrize(
    ("chunk", "count", "pause", "then", "drained"),
    [
        pytest.param(b"x" * 512, 1, 0.0, None, True, id="short"),  # read to its end: the connection is reusable
        pytest.param(b"x" * 512, 40_960, 0.0, None, False, id="20-mb"),  # abandoned at the cap
        pytest.param(b"x", 1, 0.0, httpx.ReadError("connection reset"), False, id="body-breaks-off"),
        pytest.param(b".", 100, 0.2, None, False, id="body-outlasts-the-deadline"),
    ],
)
@respx.mock
async def test_a_2xx_is_a_delivery_however_its_body_ends(
    chunk: bytes, count: int, pause: float, then: Exception | None, drained: bool
) -> None:
    served, finished = 0, False

    async def body() -> AsyncIterator[bytes]:
        nonlocal served, finished
        for _ in range(count):
            await asyncio.sleep(pause)
            served += 1
            yield chunk
        if then is not None:
            raise then
        finished = True  # only reached when the reader asks for more after the last chunk

    respx.post("https://hooks.test/pay").mock(return_value=httpx.Response(200, content=body()))
    async with httpx.AsyncClient(timeout=httpx.Timeout(0.25)) as client:  # budget: connect + read = 0.5 s
        result = await make_sender(client).deliver("https://hooks.test/pay", event_id="evt_1", payload={})
    # The status line is the receiver's acknowledgment (Standard Webhooks): nothing its body does takes it back.
    assert (result.ok, result.status_code, result.error) == (True, 200, None)
    assert finished is drained
    assert served <= 3  # never read past the cap


@respx.mock
async def test_error_text_never_contains_nul() -> None:
    respx.post("https://hooks.test/pay").mock(return_value=httpx.Response(500, content=b"bad\x00gateway"))
    async with httpx.AsyncClient() as client:
        result = await make_sender(client).deliver("https://hooks.test/pay", event_id="evt_1", payload={})
    assert result.error == "HTTP 500: badgateway"  # Postgres text columns reject NUL


@respx.mock
async def test_slow_drip_is_cut_off_by_the_overall_deadline() -> None:
    async def drip() -> AsyncIterator[bytes]:
        for _ in range(100):  # every read is quick, the response as a whole is not
            await asyncio.sleep(0.05)
            yield b"."

    respx.post("https://hooks.test/pay").mock(return_value=httpx.Response(500, content=drip()))
    async with httpx.AsyncClient(timeout=httpx.Timeout(0.05)) as client:  # budget: connect + read = 0.1 s
        result = await make_sender(client).deliver("https://hooks.test/pay", event_id="evt_1", payload={})
    assert (result.ok, result.status_code, result.permanent) == (False, None, False)
    assert result.error is not None
    assert result.error.startswith("TimeoutError")


async def test_unsafe_url_is_permanent_and_never_sent() -> None:
    async with httpx.AsyncClient() as client:
        sender = WebhookSender(client, secret=SECRET, resolver=resolver_returning("10.0.0.1"))
        result = await sender.deliver("https://internal.example/hook", event_id="evt_1", payload={})
    assert (result.ok, result.permanent) == (False, True)
    assert result.error is not None
    assert result.error.startswith("unsafe_url")


@pytest.mark.parametrize(
    "url",
    [
        "https://a..example/hook",  # empty DNS label: the API accepts it, getaddrinfo raises UnicodeError
        "https://xn--/hook",  # IDNA parsing fails inside httpx.URL
        "http://[::1",  # not a URL at all
    ],
)
async def test_malformed_url_is_permanent_not_an_exception(url: str) -> None:
    async with httpx.AsyncClient() as client:
        result = await WebhookSender(client, secret=SECRET).deliver(url, event_id="evt_1", payload={})
    assert (result.ok, result.permanent) == (False, True)
    assert result.error is not None
    assert result.error.startswith("unsafe_url")
