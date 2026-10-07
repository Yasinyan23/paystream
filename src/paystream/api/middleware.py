import re
from contextvars import ContextVar
from uuid import uuid4

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

request_id_ctx: ContextVar[str | None] = ContextVar("request_id", default=None)
_VALID_REQUEST_ID = re.compile(r"[A-Za-z0-9._-]{1,128}")


class RequestIdMiddleware:
    """Accepts a sane incoming X-Request-ID or generates one; echoes it and exposes it via a contextvar."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        incoming = Headers(scope=scope).get("x-request-id")
        request_id = incoming if incoming and _VALID_REQUEST_ID.fullmatch(incoming) else uuid4().hex
        token = request_id_ctx.set(request_id)

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message).append("X-Request-ID", request_id)
            await send(message)

        # No finally: when an error escapes, the outer ServerErrorMiddleware builds the 500 response
        # after this frame unwinds and still needs the id. Each request runs in its own task, so a
        # skipped reset leaks nothing.
        await self.app(scope, receive, send_with_request_id)
        request_id_ctx.reset(token)
