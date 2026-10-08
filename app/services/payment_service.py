"""Payment creation and retrieval business logic."""

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from app.db.uow import UnitOfWork
from app.exceptions import IdempotencyConflictError, PaymentNotFoundError
from app.models.enums import Currency
from app.models.outbox import Outbox
from app.models.payment import Payment


@dataclass(frozen=True)
class PaymentData:
    """Validated, API-independent input for creating a payment."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, object]
    webhook_url: str


class PaymentService:
    """Coordinate payment persistence and transactional outbox creation."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def create_payment(
        self,
        data: PaymentData,
        idempotency_key: str,
    ) -> Payment:
        """Create a payment once per key and enqueue its initial event atomically."""
        async with self._uow_factory() as uow:
            payment = await uow.payments.insert_if_absent(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata=data.metadata,
                idempotency_key=idempotency_key,
                webhook_url=data.webhook_url,
            )

            if payment is None:
                # A uniqueness conflict means this key was handled by another request.
                existing = await uow.payments.get_by_idempotency_key(idempotency_key)
                if existing is None:
                    # The unique conflict without a visible row indicates an unexpected race/state.
                    raise RuntimeError("Payment was not found after idempotency conflict")

                # Reusing a key is safe only when every client-controlled field matches.
                if not self._matches_request(
                    existing,
                    amount=data.amount,
                    currency=data.currency,
                    description=data.description,
                    metadata=data.metadata,
                    webhook_url=data.webhook_url,
                ):
                    raise IdempotencyConflictError(idempotency_key)

                return existing

            # Add the event to the outbox along with the new payment.
            await uow.outbox.add(
                Outbox(
                    event_type="payment.new",
                    payload={"payment_id": str(payment.id)},
                )
            )
            await uow.commit()
            return payment

    async def get_payment(self, payment_id: UUID) -> Payment:
        """Return a payment or raise a domain exception when it is missing."""
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_by_id(payment_id)
            if payment is None:
                # Keep missing-payment behavior consistent across API operations.
                raise PaymentNotFoundError(str(payment_id))

            return payment

    @staticmethod
    def _matches_request(
        payment: Payment,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, object],
        webhook_url: str,
    ) -> bool:
        """Compare all client-controlled fields used for idempotency."""
        return (
            payment.amount == amount
            and payment.currency == currency
            and payment.description == description
            and payment.metadata_ == metadata
            and payment.webhook_url == webhook_url
        )
