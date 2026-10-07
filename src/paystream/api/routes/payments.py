from uuid import UUID

from fastapi import APIRouter, Depends

from paystream.api.deps import require_api_key
from paystream.api.errors import ApiProblem

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)])


@router.get("/{payment_id}")
async def get_payment(payment_id: UUID) -> None:
    raise ApiProblem(404, "payment-not-found", "Payment not found", f"No payment with id {payment_id}")
