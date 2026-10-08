"""Outbox-specific persistence operations."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import OutboxStatus
from app.models.outbox import Outbox
from app.repositories.base import BaseRepository


class OutboxRepository(BaseRepository[Outbox]):
    """Query and update outbox records without committing transactions."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Outbox)

    async def fetch_pending_batch(self, limit: int) -> list[Outbox]:
        """Return the oldest pending events up to the requested limit."""
        # Publisher is configured as a singleton; row locks do not span the network publish.
        # Keep the query ordered so older events are attempted first.
        statement = (
            select(Outbox)
            .where(Outbox.status == OutboxStatus.PENDING)
            .order_by(Outbox.created_at)
            .limit(limit)
        )
        result = await self.session.execute(statement)
        return list(result.scalars().all())

    async def mark_published(self, event_ids: list[UUID]) -> None:
        """Mark the selected events as published at the current database time."""
        if not event_ids:
            # Avoid emitting an UPDATE with an empty identifier set.
            return

        # Callers pass only events confirmed by the broker.
        statement = (
            update(Outbox)
            .where(Outbox.id.in_(event_ids))
            .values(status=OutboxStatus.PUBLISHED, published_at=func.now())
        )
        await self.session.execute(statement)

    async def delete_published_before(self, cutoff: datetime) -> None:
        """Delete events that were published before the cutoff time."""
        # Pending events are never removed, only broker-confirmed ones.
        statement = delete(Outbox).where(
            Outbox.status == OutboxStatus.PUBLISHED,
            Outbox.published_at < cutoff,
        )
        await self.session.execute(statement)
