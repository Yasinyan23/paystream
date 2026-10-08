import logging
from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from paystream.api.middleware import request_id_ctx
from paystream.domain.errors import IdempotencyKeyReusedError

logger = logging.getLogger(__name__)

PROBLEM_JSON = "application/problem+json"
_HTTP_SLUGS = {404: "not-found", 405: "method-not-allowed"}


class ApiProblem(Exception):  # noqa: N818 — RFC 9457 "problem"
    """An RFC 9457 problem raised from route code."""

    def __init__(
        self,
        status: int,
        slug: str,
        title: str,
        detail: str | None = None,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(detail or title)
        self.status, self.slug, self.title, self.detail = status, slug, title, detail
        self.headers = dict(headers or {})


def problem_response(
    request: Request,
    *,
    status: int,
    slug: str,
    title: str,
    detail: str | None = None,
    headers: Mapping[str, str] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": f"urn:paystream:problem:{slug}",
        "title": title,
        "status": status,
        "detail": detail,
        "instance": request.url.path,
        "request_id": request_id_ctx.get(),
        **(extra or {}),
    }
    return JSONResponse(body, status_code=status, headers=dict(headers or {}), media_type=PROBLEM_JSON)


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiProblem)
    async def _api_problem(request: Request, exc: ApiProblem) -> JSONResponse:
        return problem_response(
            request, status=exc.status, slug=exc.slug, title=exc.title, detail=exc.detail, headers=exc.headers
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        if any(e["type"] == "missing" and tuple(e["loc"]) == ("header", "Idempotency-Key") for e in exc.errors()):
            return problem_response(
                request, status=400, slug="idempotency-key-missing", title="Idempotency-Key header is required"
            )
        # The rejected `input` is not echoed: client data of any shape (NaN, lone surrogates, deep nesting) must not
        # decide whether the 422 can be rendered (Starlette's JSONResponse refuses NaN, cannot encode surrogates).
        errors = [{key: value for key, value in error.items() if key != "input"} for error in exc.errors()]
        return problem_response(
            request,
            status=422,
            slug="validation-error",
            title="Request validation failed",
            detail="One or more fields are invalid",
            extra={"errors": jsonable_encoder(errors)},
        )

    @app.exception_handler(IdempotencyKeyReusedError)
    async def _key_reused(request: Request, exc: IdempotencyKeyReusedError) -> JSONResponse:
        return problem_response(
            request, status=422, slug="idempotency-key-reused", title="Idempotency key reused", detail=str(exc)
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return problem_response(
            request,
            status=exc.status_code,
            slug=_HTTP_SLUGS.get(exc.status_code, "http-error"),
            title=str(exc.detail),
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        # This handler runs in ServerErrorMiddleware, outside RequestIdMiddleware: set the header here.
        return problem_response(
            request,
            status=500,
            slug="internal-error",
            title="Internal server error",
            headers={"X-Request-ID": rid} if (rid := request_id_ctx.get()) else None,
        )
