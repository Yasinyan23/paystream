from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status

from paystream.api.deps import get_payment_service, idempotency_key, require_api_key
from paystream.api.errors import ApiProblem
from paystream.api.middleware import request_id_ctx
from paystream.api.schemas import PaymentAccepted, PaymentCreateRequest, PaymentDetail
from paystream.services.payments import PaymentService

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)])

ServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
_PROBLEM: dict[str, Any] = {"content": {"application/problem+json": {}}}


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a payment",
    response_description="Accepted for asynchronous processing",
    responses={
        400: {"description": "Idempotency-Key missing or invalid", **_PROBLEM},
        401: {"description": "Missing or invalid X-API-Key", **_PROBLEM},
        422: {"description": "Invalid body, or Idempotency-Key reused with a different body", **_PROBLEM},
    },
)
async def create_payment(
    body: PaymentCreateRequest,
    response: Response,
    key: Annotated[str, Depends(idempotency_key)],
    service: ServiceDep,
) -> PaymentAccepted:
    """Repeating the request with the same `Idempotency-Key` and body returns the same payment
    with `Idempotent-Replayed: true`; the same key with a different body returns 422."""
    result = await service.create(body.to_command(), idempotency_key=key, correlation_id=request_id_ctx.get())
    payment = result.payment
    response.headers["Location"] = f"/api/v1/payments/{payment.id}"
    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


@router.get(
    "/{payment_id}",
    summary="Get a payment",
    responses={404: {"description": "Payment not found", **_PROBLEM}, 401: {"description": "Unauthorized", **_PROBLEM}},
)
async def get_payment(payment_id: UUID, service: ServiceDep) -> PaymentDetail:
    view = await service.get(payment_id)
    if view is None:
        raise ApiProblem(404, "payment-not-found", "Payment not found", f"No payment with id {payment_id}")
    return PaymentDetail.from_view(view)
