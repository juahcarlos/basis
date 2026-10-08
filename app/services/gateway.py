"""Payment gateway protocol and deterministic-shape fake implementation."""

import asyncio
import random
from typing import Protocol

from app.models.enums import PaymentStatus
from app.models.payment import Payment


class PaymentGateway(Protocol):
    """Define the asynchronous contract for payment processing gateways."""

    async def process(
        self,
        payment: Payment,
        *,
        idempotency_key: str | None = None,
    ) -> PaymentStatus:
        """Return the final status assigned by the payment gateway."""
        ...


class FakeGateway:
    """Simulate gateway latency and a 90/10 success/failure outcome."""

    async def process(
        self,
        payment: Payment,
        *,
        idempotency_key: str | None = None,
    ) -> PaymentStatus:
        """Wait for simulated processing and return a random final status."""
        await asyncio.sleep(random.uniform(2, 5))
        # The fake intentionally ignores idempotency; real gateways must honor it.
        if random.random() < 0.9:
            return PaymentStatus.SUCCEEDED

        return PaymentStatus.FAILED
