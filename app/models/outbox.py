"""Transactional outbox ORM model."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, Enum, Index, String, Uuid, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import OutboxStatus


class Outbox(Base):
    """Event record waiting to be published to the message broker."""

    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint("status IN ('pending', 'published')", name="outbox_status_values"),
        # Limit polling index entries to events that still need publication.
        Index(
            "ix_outbox_pending_created_at",
            "created_at",
            postgresql_where=text("status = 'pending'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    # Store the complete broker body so the publisher can replay it unchanged.
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[OutboxStatus] = mapped_column(
        Enum(
            OutboxStatus,
            native_enum=False,
            create_constraint=False,
            values_callable=lambda values: [value.value for value in values],
        ),
        nullable=False,
        default=OutboxStatus.PENDING,
        server_default=OutboxStatus.PENDING.value,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Set only after RabbitMQ confirms publication.
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
