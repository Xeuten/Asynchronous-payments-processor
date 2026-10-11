import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.schemas.payment import PaymentAccepted, PaymentCreate, PaymentRead
from app.services.payment_service import (
    IdempotencyConflictError,
    PaymentNotFoundError,
    PaymentService,
)

router = APIRouter(prefix="/payments", tags=["payments"])


def get_payment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentService:
    return PaymentService(session)


ServiceDep = Annotated[PaymentService, Depends(get_payment_service)]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={409: {"description": "Idempotency-Key reused with a different body"}},
)
async def create_payment(
    data: PaymentCreate,
    service: ServiceDep,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
):
    try:
        payment = await service.create(data, idempotency_key)
    except IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None
    return PaymentAccepted.model_validate(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    responses={404: {"description": "Payment not found"}},
)
async def get_payment(payment_id: uuid.UUID, service: ServiceDep):
    try:
        payment = await service.get(payment_id)
    except PaymentNotFoundError:
        raise HTTPException(status_code=404, detail="Payment not found") from None
    return PaymentRead.model_validate(payment)
