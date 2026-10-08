"""Consumer integration tests with PostgreSQL and injected collaborators."""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker.handlers import handle_payment_message
from app.config import get_settings
from app.db.uow import UnitOfWork
from app.exceptions import UnsafeWebhookDestinationError, WebhookDeliveryError
from app.models.enums import Currency, PaymentStatus
from app.models.payment import Payment
from app.services.gateway import PaymentGateway
from app.services.payment_service import PaymentData, PaymentService
from app.services.webhook import WebhookService


class FixedGateway:
    """Return a selected gateway result without sleeping."""

    def __init__(self, status: PaymentStatus) -> None:
        self.status = status
        self.calls = 0
        self.idempotency_keys: list[str | None] = []

    async def process(
        self,
        payment: Payment,
        *,
        idempotency_key: str | None = None,
    ) -> PaymentStatus:
        """Record the call and return the configured status."""
        self.calls += 1
        self.idempotency_keys.append(idempotency_key)
        return self.status


class BlockingGateway(FixedGateway):
    """Hold the first gateway call open so a second worker races on the same row."""

    def __init__(self, status: PaymentStatus) -> None:
        super().__init__(status)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def process(
        self,
        payment: Payment,
        *,
        idempotency_key: str | None = None,
    ) -> PaymentStatus:
        self.calls += 1
        self.idempotency_keys.append(idempotency_key)
        self.started.set()
        await self.release.wait()
        return self.status


class RecordingWebhook:
    """Record webhook attempts and optionally simulate delivery failure."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.payment_ids: list[str] = []

    async def deliver(self, payment: Payment) -> None:
        """Record the payment ID and raise the configured delivery error."""
        self.payment_ids.append(str(payment.id))
        if self.fail:
            raise WebhookDeliveryError("simulated webhook failure")


async def make_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> Payment:
    """Create a pending payment and its outbox event for consumer processing."""
    service = PaymentService(lambda: UnitOfWork(session_factory))
    return await service.create_payment(
        PaymentData(
            amount=Decimal("10.00"),
            currency=Currency.RUB,
            description="Consumer integration payment",
            metadata={},
            webhook_url="https://example.test/webhook",
        ),
        f"consumer-{uuid4()}",
    )


async def handle(
    payment: Payment,
    *,
    session_factory: async_sessionmaker[AsyncSession],
    gateway: PaymentGateway,
    webhook: WebhookService,
    broker: RabbitBroker,
    attempt: int = 0,
) -> None:
    """Run one delivery through the public message handler."""
    await handle_payment_message(
        {"payment_id": str(payment.id)},
        headers={"x-attempt": attempt},
        message_id=str(uuid4()),
        broker=broker,
        session_factory=session_factory,
        gateway=gateway,
        webhook=webhook,
        settings=get_settings(),
    )


@pytest.mark.asyncio
async def test_consumer_processes_gateway_and_delivers_webhook(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Persist the gateway result and call the webhook without retrying."""
    payment = await make_payment(test_session_factory)
    gateway = FixedGateway(PaymentStatus.SUCCEEDED)
    webhook = RecordingWebhook()
    broker = AsyncMock(spec=RabbitBroker)

    await handle(
        payment,
        session_factory=test_session_factory,
        gateway=gateway,
        webhook=webhook,
        broker=broker,
    )

    async with test_session_factory() as session:
        stored_payment = await session.get(Payment, payment.id)

    assert stored_payment is not None
    assert stored_payment.status == PaymentStatus.SUCCEEDED
    assert stored_payment.processed_at is not None
    assert webhook.payment_ids == [str(payment.id)]
    assert gateway.calls == 1
    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_consumer_retries_webhook_then_routes_third_failure_to_dlq(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Retry a failed webhook twice, skip the completed gateway, then publish to DLQ."""
    payment = await make_payment(test_session_factory)
    gateway = FixedGateway(PaymentStatus.FAILED)
    webhook = RecordingWebhook(fail=True)
    broker = AsyncMock(spec=RabbitBroker)

    await handle(
        payment,
        session_factory=test_session_factory,
        gateway=gateway,
        webhook=webhook,
        broker=broker,
        attempt=0,
    )
    await handle(
        payment,
        session_factory=test_session_factory,
        gateway=gateway,
        webhook=webhook,
        broker=broker,
        attempt=1,
    )
    await handle(
        payment,
        session_factory=test_session_factory,
        gateway=gateway,
        webhook=webhook,
        broker=broker,
        attempt=2,
    )

    async with test_session_factory() as session:
        stored_payment = await session.get(Payment, payment.id)

    assert stored_payment is not None
    assert stored_payment.status == PaymentStatus.FAILED
    assert gateway.calls == 1
    assert broker.publish.await_count == 3
    first_call = broker.publish.await_args_list[0]
    second_call = broker.publish.await_args_list[1]
    final_call = broker.publish.await_args_list[2]
    assert first_call.kwargs["routing_key"] == "payments.retry.1"
    assert second_call.kwargs["routing_key"] == "payments.retry.2"
    assert final_call.kwargs["routing_key"] == "payments.dlq"
    assert final_call.kwargs["headers"]["x-error-reason"] == "simulated webhook failure"


@pytest.mark.asyncio
async def test_final_payment_redelivery_skips_gateway_and_retries_only_webhook(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Avoid gateway reprocessing after a final status has already been stored."""
    payment = await make_payment(test_session_factory)
    gateway = FixedGateway(PaymentStatus.SUCCEEDED)
    webhook = RecordingWebhook()
    broker = AsyncMock(spec=RabbitBroker)

    await handle(
        payment,
        session_factory=test_session_factory,
        gateway=gateway,
        webhook=webhook,
        broker=broker,
    )
    await handle(
        payment,
        session_factory=test_session_factory,
        gateway=gateway,
        webhook=webhook,
        broker=broker,
        attempt=1,
    )

    assert gateway.calls == 1
    assert webhook.payment_ids == [str(payment.id), str(payment.id)]
    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_two_workers_use_same_gateway_idempotency_key_for_payment(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Two parallel consumers must use the same idempotency key for the same payment."""
    payment = await make_payment(test_session_factory)
    gateway = BlockingGateway(PaymentStatus.SUCCEEDED)
    webhook = RecordingWebhook()
    broker = AsyncMock(spec=RabbitBroker)

    first_worker = asyncio.create_task(
        handle(
            payment,
            session_factory=test_session_factory,
            gateway=gateway,
            webhook=webhook,
            broker=broker,
        )
    )
    await gateway.started.wait()

    second_worker = asyncio.create_task(
        handle(
            payment,
            session_factory=test_session_factory,
            gateway=gateway,
            webhook=webhook,
            broker=broker,
            attempt=1,
        )
    )
    await asyncio.sleep(0)
    gateway.release.set()
    await asyncio.gather(first_worker, second_worker)

    async with test_session_factory() as session:
        stored_payment = await session.get(Payment, payment.id)

    assert stored_payment is not None
    assert stored_payment.status == PaymentStatus.SUCCEEDED
    assert gateway.idempotency_keys == [str(payment.id), str(payment.id)]
    assert webhook.payment_ids == [str(payment.id), str(payment.id)]


@pytest.mark.asyncio
async def test_unsafe_webhook_destination_is_sent_directly_to_dlq(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await make_payment(test_session_factory)
    gateway = FixedGateway(PaymentStatus.SUCCEEDED)
    broker = AsyncMock(spec=RabbitBroker)

    class UnsafeWebhook:
        async def deliver(self, payment: Payment) -> None:
            raise UnsafeWebhookDestinationError("webhook resolves to a private IP")

    await handle(
        payment,
        session_factory=test_session_factory,
        gateway=gateway,
        webhook=UnsafeWebhook(),  # type: ignore[arg-type]
        broker=broker,
    )

    assert broker.publish.await_count == 1
    assert broker.publish.await_args.kwargs["routing_key"] == "payments.dlq"
    assert broker.publish.await_args.kwargs["headers"]["x-error-reason"] == (
        "webhook resolves to a private IP"
    )


class HangingGateway(FixedGateway):
    """Never return, so only the gateway timeout can end the call."""

    async def process(
        self,
        payment: Payment,
        *,
        idempotency_key: str | None = None,
    ) -> PaymentStatus:
        """Sleep far longer than any test timeout."""
        self.calls += 1
        await asyncio.sleep(60)
        return self.status


@pytest.mark.asyncio
async def test_invalid_payment_id_is_sent_directly_to_dlq(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Route a non-UUID payment_id to the DLQ without calling the gateway."""
    gateway = FixedGateway(PaymentStatus.SUCCEEDED)
    broker = AsyncMock(spec=RabbitBroker)

    await handle_payment_message(
        {"payment_id": "not-a-uuid"},
        headers={},
        message_id=str(uuid4()),
        broker=broker,
        session_factory=test_session_factory,
        gateway=gateway,
        webhook=RecordingWebhook(),
        settings=get_settings(),
    )

    # A permanent error must skip the retry queues entirely.
    broker.publish.assert_awaited_once()
    kwargs = broker.publish.await_args.kwargs
    assert kwargs["routing_key"] == "payments.dlq"
    assert kwargs["headers"]["x-error-reason"] == "invalid payment_id"
    assert gateway.calls == 0


@pytest.mark.asyncio
async def test_gateway_timeout_routes_message_to_first_retry_queue(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Abort a hung gateway call and schedule a retry instead of blocking."""
    payment = await make_payment(test_session_factory)
    settings = get_settings().model_copy(update={"gateway_timeout_seconds": 0.05})
    broker = AsyncMock(spec=RabbitBroker)

    await handle_payment_message(
        {"payment_id": str(payment.id)},
        headers={},
        message_id=str(uuid4()),
        broker=broker,
        session_factory=test_session_factory,
        gateway=HangingGateway(PaymentStatus.SUCCEEDED),
        webhook=RecordingWebhook(),
        settings=settings,
    )

    async with test_session_factory() as session:
        stored_payment = await session.get(Payment, payment.id)

    # The payment stays pending and the message goes to the first delay queue.
    assert stored_payment is not None
    assert stored_payment.status == PaymentStatus.PENDING
    kwargs = broker.publish.await_args.kwargs
    assert kwargs["routing_key"] == "payments.retry.1"
    assert kwargs["headers"]["x-attempt"] == 1
