"""Outbox cleanup tests against PostgreSQL."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.uow import UnitOfWork
from app.models.enums import OutboxStatus
from app.models.outbox import Outbox


@pytest.mark.asyncio
async def test_cleanup_deletes_only_old_published_events(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Remove published events older than the cutoff and keep everything else."""
    now = datetime.now(UTC)
    old_published = Outbox(
        event_type="payment.new",
        payload={},
        status=OutboxStatus.PUBLISHED,
        published_at=now - timedelta(days=10),
    )
    recent_published = Outbox(
        event_type="payment.new",
        payload={},
        status=OutboxStatus.PUBLISHED,
        published_at=now,
    )
    pending = Outbox(event_type="payment.new", payload={})
    async with UnitOfWork(test_session_factory) as uow:
        # Store one event of each kind.
        for event in (old_published, recent_published, pending):
            await uow.outbox.add(event)
        await uow.commit()

    async with UnitOfWork(test_session_factory) as uow:
        await uow.outbox.delete_published_before(now - timedelta(days=7))
        await uow.commit()

    async with test_session_factory() as session:
        remaining = {row.id for row in (await session.execute(select(Outbox))).scalars()}

    # Only the old published event is gone; pending events are never deleted.
    assert remaining == {recent_published.id, pending.id}
