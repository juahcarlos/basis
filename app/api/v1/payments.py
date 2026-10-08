"""Version 1 payment endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, status

from app.api.auth import verify_api_key
from app.api.deps import PaymentServiceDep
from app.schemas.payment import PaymentCreate, PaymentCreated, PaymentRead
from app.services.payment_service import PaymentData

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreated,
    summary="Create a payment",
    responses={
        401: {"description": "Missing or invalid API key"},
        409: {"description": "Idempotency key conflicts with existing payment"},
    },
)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    service: PaymentServiceDep,
) -> PaymentCreated:
    """Accept a payment request and return its identifier."""
    # Translate the HTTP contract into the service's transport-independent input.
    payment_data = PaymentData(
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata=data.metadata,
        webhook_url=str(data.webhook_url),
    )
    payment = await service.create_payment(payment_data, idempotency_key)
    # Return only the acceptance fields defined by the create-response contract.
    return PaymentCreated(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    summary="Get a payment",
    responses={
        401: {"description": "Missing or invalid API key"},
        404: {"description": "Payment not found"},
    },
)
async def get_payment(
    payment_id: UUID,
    service: PaymentServiceDep,
) -> PaymentRead:
    """Return the stored payment with the requested UUID."""
    # The service raises the domain not-found error when this ID is absent.
    payment = await service.get_payment(payment_id)
    # Validate ORM values against the public response contract.
    return PaymentRead.model_validate(payment)
