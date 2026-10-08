"""Payment ORM model."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, Enum, Numeric, String, Text, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import Currency, PaymentStatus


class Payment(Base):
    """Stored payment request and its processing state."""

    __tablename__ = "payments"
    __table_args__ = (
        # Keep core payment invariants enforced for non-API database writes too.
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="currency_values"),
        CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="payment_status_values"
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        Enum(
            Currency,
            native_enum=False,
            create_constraint=False,
            # Persist public currency codes rather than Python enum member names.
            values_callable=lambda values: [value.value for value in values],
        ),
        nullable=False,
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict
    )
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(
            PaymentStatus,
            native_enum=False,
            create_constraint=False,
            # Keep persisted values stable if enum member names are refactored.
            values_callable=lambda values: [value.value for value in values],
        ),
        nullable=False,
        default=PaymentStatus.PENDING,
        server_default=PaymentStatus.PENDING.value,
    )
    # The unique constraint arbitrates concurrent requests using the same key.
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    # Let PostgreSQL provide a consistent timestamp across API instances.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
