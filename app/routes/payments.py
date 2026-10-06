from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app import payments
from app.dependencies import SessionDependency, require_api_key
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail

router = APIRouter(
    prefix="/api/v1/payments",
    dependencies=[Depends(require_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={
        status.HTTP_409_CONFLICT: {"description": "Idempotency key was used with a different body"}
    },
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=200)],
    session: SessionDependency,
) -> PaymentAccepted:
    try:
        payment = await payments.create_payment(session, body, idempotency_key)
    except payments.IdempotencyConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key was used with a different body",
        ) from exc
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    responses={status.HTTP_404_NOT_FOUND: {"description": "Payment not found"}},
)
async def get_payment(payment_id: UUID, session: SessionDependency) -> PaymentDetail:
    payment = await payments.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentDetail(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
    )
