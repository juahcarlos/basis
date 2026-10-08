"""Outbox publisher integration tests using FastStream's Rabbit test broker."""

from uuid import uuid4

import pytest
from faststream.rabbit import Channel, RabbitBroker, TestRabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker.publisher import OutboxPublisher
from app.broker.topology import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE
from app.config import Settings
from app.models.enums import OutboxStatus
from app.models.outbox import Outbox


@pytest.mark.asyncio
async def test_publisher_marks_event_only_after_broker_publish(
    test_session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    """Publish a pending outbox row through TestRabbitBroker and mark it published."""
    event = Outbox(
        event_type="payment.new",
        payload={"payment_id": str(uuid4())},
    )
    async with test_session_factory() as session:
        session.add(event)
        await session.commit()
        event_id = event.id

    broker = RabbitBroker(default_channel=Channel(publisher_confirms=True))
    received_messages: list[dict[str, str]] = []

    @broker.subscriber(PAYMENTS_QUEUE, exchange=PAYMENTS_EXCHANGE)
    async def receive_payment(body: dict[str, str]) -> None:
        """Capture the published test event from the declared queue."""
        received_messages.append(body)

    publisher = OutboxPublisher.__new__(OutboxPublisher)
    publisher._broker = broker
    publisher._settings = settings
    publisher._session_factory = test_session_factory

    async with TestRabbitBroker(broker):
        await publisher._publish_batch()

    async with test_session_factory() as session:
        published_event = await session.scalar(select(Outbox).where(Outbox.id == event_id))

    assert published_event is not None
    assert published_event.status == OutboxStatus.PUBLISHED
    assert published_event.published_at is not None
    assert received_messages == [{"payment_id": event.payload["payment_id"]}]
