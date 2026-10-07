import re
import secrets
from typing import Annotated

from fastapi import Header, Request, Security
from fastapi.security import APIKeyHeader

from paystream.api.errors import ApiProblem
from paystream.services.payments import PaymentService

_api_key_header = APIKeyHeader(
    name="X-API-Key", scheme_name="ApiKeyAuth", auto_error=False, description="Static API key"
)
_IDEMPOTENCY_KEY = re.compile(r"[\x21-\x7e]{1,255}")


async def require_api_key(request: Request, api_key: Annotated[str | None, Security(_api_key_header)]) -> None:
    candidate = (api_key or "").encode()
    matched = False
    for key in request.app.state.settings.api_keys:
        # Compare against every key so timing does not reveal which one matched.
        matched |= secrets.compare_digest(candidate, key.get_secret_value().encode())
    if not api_key or not matched:
        raise ApiProblem(
            401,
            "unauthorized",
            "Unauthorized",
            "Missing or invalid X-API-Key header",
            headers={"WWW-Authenticate": "ApiKey"},
        )


async def idempotency_key(
    key: Annotated[str | None, Header(alias="Idempotency-Key", description="Unique key for safe retries")] = None,
) -> str:
    if key is None:
        raise ApiProblem(400, "idempotency-key-missing", "Idempotency-Key header is required")
    if not _IDEMPOTENCY_KEY.fullmatch(key):
        raise ApiProblem(
            400,
            "idempotency-key-invalid",
            "Invalid Idempotency-Key",
            "Use 1-255 printable ASCII characters without spaces",
        )
    return key


def get_payment_service(request: Request) -> PaymentService:
    return PaymentService(request.app.state.db)
