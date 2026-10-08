"""Unit tests for payment business rules without a database."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.exceptions import IdempotencyConflictError, PaymentNotFoundError
from app.models.enums import Currency
from app.models.outbox import Outbox
from app.services.payment_service import PaymentData, PaymentService

pytestmark = pytest.mark.unit


class UnitOfWorkStub:
    """Provide async repository doubles for service-level tests."""

    def __init__(
        self,
        *,
        inserted_payment: object | None = None,
        existing_payment: object | None = None,
        payment_by_id: object | None = None,
    ) -> None:
        self.payments = SimpleNamespace(
            insert_if_absent=AsyncMock(return_value=inserted_payment),
            get_by_idempotency_key=AsyncMock(return_value=existing_payment),
            get_by_id=AsyncMock(return_value=payment_by_id),
        )
        self.outbox = SimpleNamespace(add=AsyncMock())
        self.commit = AsyncMock()

    async def __aenter__(self) -> "UnitOfWorkStub":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


def make_payment_data() -> PaymentData:
    return PaymentData(
        amount=Decimal("15.25"),
        currency=Currency.USD,
        description="Unit test payment",
        metadata={"source": "test"},
        webhook_url="https://example.test/webhook",
    )


def make_stored_payment(data: PaymentData) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata_=data.metadata,
        webhook_url=data.webhook_url,
    )


def make_service(uow: UnitOfWorkStub) -> PaymentService:
    return PaymentService(lambda: uow)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_create_payment_adds_outbox_event_and_commits() -> None:
    data = make_payment_data()
    payment = make_stored_payment(data)
    uow = UnitOfWorkStub(inserted_payment=payment)

    result = await make_service(uow).create_payment(data, "unit-key")

    assert result is payment
    uow.payments.insert_if_absent.assert_awaited_once_with(
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata=data.metadata,
        idempotency_key="unit-key",
        webhook_url=data.webhook_url,
    )
    uow.outbox.add.assert_awaited_once()
    event = uow.outbox.add.await_args.args[0]
    assert isinstance(event, Outbox)
    assert event.event_type == "payment.new"
    assert event.payload == {"payment_id": str(payment.id)}
    uow.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_matching_idempotency_repeat_returns_existing_without_writes() -> None:
    data = make_payment_data()
    payment = make_stored_payment(data)
    uow = UnitOfWorkStub(existing_payment=payment)

    result = await make_service(uow).create_payment(data, "unit-key")

    assert result is payment
    uow.payments.get_by_idempotency_key.assert_awaited_once_with("unit-key")
    uow.outbox.add.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", Decimal("16.25")),
        ("currency", Currency.RUB),
        ("description", "Changed description"),
        ("metadata", {"source": "changed"}),
        ("webhook_url", "https://other.example/webhook"),
    ],
)
async def test_changed_idempotency_request_raises_conflict(
    field: str,
    value: object,
) -> None:
    data = make_payment_data()
    payment = make_stored_payment(data)
    uow = UnitOfWorkStub(existing_payment=payment)

    with pytest.raises(IdempotencyConflictError):
        await make_service(uow).create_payment(replace(data, **{field: value}), "unit-key")

    uow.outbox.add.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_idempotency_conflict_without_existing_row_raises_runtime_error() -> None:
    uow = UnitOfWorkStub()

    with pytest.raises(RuntimeError, match="Payment was not found"):
        await make_service(uow).create_payment(make_payment_data(), "unit-key")

    uow.outbox.add.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_payment_returns_existing_payment() -> None:
    data = make_payment_data()
    payment = make_stored_payment(data)
    uow = UnitOfWorkStub(payment_by_id=payment)

    result = await make_service(uow).get_payment(payment.id)

    assert result is payment
    uow.payments.get_by_id.assert_awaited_once_with(payment.id)


@pytest.mark.asyncio
async def test_get_missing_payment_raises_domain_error() -> None:
    payment_id = uuid4()
    uow = UnitOfWorkStub()

    with pytest.raises(PaymentNotFoundError):
        await make_service(uow).get_payment(payment_id)

    uow.payments.get_by_id.assert_awaited_once()
    assert uow.payments.get_by_id.await_args.args == (payment_id,)
