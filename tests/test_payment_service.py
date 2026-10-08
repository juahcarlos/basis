"""Payment service integration tests against PostgreSQL."""

import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.uow import UnitOfWork
from app.exceptions import IdempotencyConflictError
from app.models.enums import Currency
from app.models.outbox import Outbox
from app.models.payment import Payment
from app.services.payment_service import PaymentData, PaymentService


def make_service(session_factory: async_sessionmaker[AsyncSession]) -> PaymentService:
    """Build the service with a fresh unit of work for each operation."""
    return PaymentService(lambda: UnitOfWork(session_factory))


def payment_data(amount: str = "15.25") -> PaymentData:
    """Return a valid payment request for database scenarios."""
    return PaymentData(
        amount=Decimal(amount),
        currency=Currency.USD,
        description="Integration test payment",
        metadata={"test": True},
        webhook_url="https://example.test/webhook",
    )


@pytest.mark.asyncio
async def test_create_payment_and_outbox_are_committed_together(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Persist one payment and exactly one initial outbox event."""
    service = make_service(test_session_factory)
    payment = await service.create_payment(payment_data(), f"atomic-{uuid4()}")

    async with test_session_factory() as session:
        stored_payment = await session.get(Payment, payment.id)
        event_count = await session.scalar(select(func.count()).select_from(Outbox))

    assert stored_payment is not None
    assert event_count == 1
    assert stored_payment.amount == Decimal("15.25")


@pytest.mark.asyncio
async def test_idempotent_repeat_returns_existing_payment_without_new_event(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Return the original payment and avoid duplicate outbox events for matching input."""
    service = make_service(test_session_factory)
    idempotency_key = f"repeat-{uuid4()}"
    data = payment_data()

    first = await service.create_payment(data, idempotency_key)
    repeated = await service.create_payment(data, idempotency_key)

    async with test_session_factory() as session:
        event_count = await session.scalar(select(func.count()).select_from(Outbox))

    assert repeated.id == first.id
    assert event_count == 1


@pytest.mark.asyncio
async def test_idempotency_key_with_changed_input_raises_conflict(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Reject reuse of an idempotency key with different request fields."""
    service = make_service(test_session_factory)
    idempotency_key = f"conflict-{uuid4()}"
    await service.create_payment(payment_data(), idempotency_key)

    with pytest.raises(IdempotencyConflictError):
        await service.create_payment(payment_data("16.25"), idempotency_key)


@pytest.mark.asyncio
async def test_concurrent_same_idempotency_key_creates_one_payment_and_one_outbox_event(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Multiple concurrent requests with the same idempotency key must create exactly one row."""
    service = make_service(test_session_factory)
    data = payment_data()
    idempotency_key = f"race-{uuid4()}"

    results = await asyncio.gather(
        service.create_payment(data, idempotency_key),
        service.create_payment(data, idempotency_key),
        service.create_payment(data, idempotency_key),
    )

    async with test_session_factory() as session:
        payment_count = await session.scalar(select(func.count()).select_from(Payment))
        outbox_count = await session.scalar(select(func.count()).select_from(Outbox))
        unique_ids = {result.id for result in results}

    assert payment_count == 1
    assert outbox_count == 1
    assert len(unique_ids) == 1
