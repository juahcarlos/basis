"""Payment-specific persistence operations."""

from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import Currency, PaymentStatus
from app.models.payment import Payment
from app.repositories.base import BaseRepository


class PaymentRepository(BaseRepository[Payment]):
    """Query and update payment records without committing transactions."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Payment)

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        """Find a payment by its UUID."""
        return await self.session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        """Find the payment associated with an idempotency key."""
        # The unique index keeps this lookup deterministic.
        statement = select(Payment).where(Payment.idempotency_key == idempotency_key)
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()

    async def insert_if_absent(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, object],
        idempotency_key: str,
        webhook_url: str,
    ) -> Payment | None:
        """Insert a payment unless its idempotency key already exists."""
        statement = (
            # Let the unique constraint choose one winner without a read-before-write race.
            insert(Payment)
            .values(
                amount=amount,
                currency=currency,
                description=description,
                metadata_=metadata,
                idempotency_key=idempotency_key,
                webhook_url=webhook_url,
            )
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(Payment)
        )
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()

    async def mark_processed(self, payment_id: UUID, status: PaymentStatus) -> None:
        """Set a payment's final status and database processing timestamp."""
        statement = (
            update(Payment)
            # A concurrent worker must not overwrite an already-final status.
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
        )
        await self.session.execute(statement)

    async def get_for_update(self, payment_id: UUID) -> Payment | None:
        """Load a payment while locking its row for the current transaction."""
        # Callers use this only for the short final-state transition.
        statement = select(Payment).where(Payment.id == payment_id).with_for_update()
        result = await self.session.execute(statement)
        return result.scalar_one_or_none()
