"""Payment message processing and retry routing."""

import asyncio
import logging
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker.topology import DLX_EXCHANGE, RETRY_EXCHANGE
from app.config import Settings
from app.db.uow import UnitOfWork
from app.exceptions import PaymentNotFoundError, UnsafeWebhookDestinationError
from app.models.enums import PaymentStatus
from app.services.gateway import PaymentGateway
from app.services.webhook import WebhookService

logger = logging.getLogger(__name__)


async def handle_payment_message(
    body: Mapping[str, Any],
    *,
    headers: Mapping[str, Any],
    message_id: str | None,
    broker: RabbitBroker,
    session_factory: async_sessionmaker[AsyncSession],
    gateway: PaymentGateway,
    webhook: WebhookService,
    settings: Settings,
) -> None:
    """Process a payment event and route failures to retry or dead-letter queues."""
    attempt = _read_attempt(headers)

    if "payment_id" not in body:
        # Drop malformed message.
        await _publish_dead_letter(
            broker,
            body=body,
            headers=headers,
            message_id=message_id,
            reason="missing payment_id",
        )
        return

    try:
        # Check if UUID valid.
        UUID(str(body["payment_id"]))
    except ValueError as exception:
        # Cannot retry invalid ID.
        logger.error(
            "\n\n !!! --- ERROR --- !!! %s handle_payment_message invalid payment_id e=%s\n\n",
            __file__,
            exception,
        )
        await _publish_dead_letter(
            broker,
            body=body,
            headers=headers,
            message_id=message_id,
            reason="invalid payment_id",
        )
        return

    try:
        await _process_payment(
            body,
            session_factory=session_factory,
            gateway=gateway,
            webhook=webhook,
            gateway_timeout_seconds=settings.gateway_timeout_seconds,
        )
    except UnsafeWebhookDestinationError as exception:
        # A policy rejection is permanent, so do not spend retry attempts on it.
        logger.error(
            "\n\n !!! --- ERROR --- !!! %s handle_payment_message "
            "unsafe webhook destination e=%s\n\n",
            __file__,
            exception,
        )
        await _publish_dead_letter(
            broker,
            body=body,
            headers=headers,
            message_id=message_id,
            reason=str(exception),
        )
    except Exception as exception:
        # Retry on gateway or delivery failure.
        logger.error(
            "\n\n !!! --- ERROR --- !!! %s handle_payment_message "
            "payment message processing failed e=%s\n\n",
            __file__,
            exception,
        )
        await _publish_retry_or_dead_letter(
            broker,
            body=body,
            headers=headers,
            message_id=message_id,
            attempt=attempt,
            reason=str(exception),
            settings=settings,
        )


async def _publish_dead_letter(
    broker: RabbitBroker,
    *,
    body: Mapping[str, Any],
    headers: Mapping[str, Any],
    message_id: str | None,
    reason: str,
) -> None:
    """Send malformed messages directly to the DLQ with a diagnostic reason."""
    outgoing_headers = dict(headers)
    # Preserve broker metadata and attach the reason for operator inspection.
    outgoing_headers["x-error-reason"] = reason
    await broker.publish(
        dict(body),
        exchange=DLX_EXCHANGE,
        routing_key="payments.dlq",
        headers=outgoing_headers,
        persist=True,
        message_id=message_id,
    )


async def _process_payment(
    body: Mapping[str, Any],
    *,
    session_factory: async_sessionmaker[AsyncSession],
    gateway: PaymentGateway,
    webhook: WebhookService,
    gateway_timeout_seconds: float,
) -> None:
    """Keep network calls outside transactions and recheck state under a short row lock."""
    payment_id = UUID(str(body["payment_id"]))

    async with UnitOfWork(session_factory) as uow:
        payment = await uow.payments.get_by_id(payment_id)
        if payment is None:
            # Payment does not exist in DB.
            raise PaymentNotFoundError(str(payment_id))

    if payment.status == PaymentStatus.PENDING:
        # Run the slow gateway call outside the transaction.
        async with asyncio.timeout(gateway_timeout_seconds):
            gateway_status = await gateway.process(payment, idempotency_key=str(payment.id))

        async with UnitOfWork(session_factory) as uow:
            locked_payment = await uow.payments.get_for_update(payment_id)
            if locked_payment is None:
                # Raise error if the payment is missing from the DB.
                raise PaymentNotFoundError(str(payment_id))

            if locked_payment.status == PaymentStatus.PENDING:
                # Protect against race conditions if another worker handled this payment
                # during the gateway call.
                await uow.payments.mark_processed(payment_id, gateway_status)
                await uow.commit()

    async with UnitOfWork(session_factory) as uow:
        processed_payment = await uow.payments.get_by_id(payment_id)
        if processed_payment is None:
            # Check again in the new session to guarantee that the result
            # of the operation is correct and even errors will be handled properly.
            raise PaymentNotFoundError(str(payment_id))

    await webhook.deliver(processed_payment)


def _read_attempt(headers: Mapping[str, Any]) -> int:
    """Get the retry count or default to 0 if the header is missing."""
    try:
        attempt = int(headers.get("x-attempt", 0))
    except (TypeError, ValueError) as exception:
        # Treat malformed retry metadata as the first delivery.
        logger.error(
            "\n\n !!! --- ERROR --- !!! %s _read_attempt invalid x-attempt header e=%s\n\n",
            __file__,
            exception,
        )
        return 0

    # Ensure the retry count is never negative.
    return max(attempt, 0)


async def _publish_retry_or_dead_letter(
    broker: RabbitBroker,
    *,
    body: Mapping[str, Any],
    headers: Mapping[str, Any],
    message_id: str | None,
    attempt: int,
    reason: str,
    settings: Settings,
) -> None:
    """Publish failures to the next TTL queue or DLQ before source ack."""
    next_attempt = attempt + 1
    outgoing_headers = dict(headers)
    # Retry attempt is carried with the message through TTL queues.
    outgoing_headers["x-attempt"] = next_attempt

    # Once the configured number of processing attempts is exhausted, send to DLQ.
    if next_attempt >= settings.max_attempts:
        # Do not schedule another retry after the configured final attempt.
        outgoing_headers["x-error-reason"] = reason
        await broker.publish(
            dict(body),
            exchange=DLX_EXCHANGE,
            routing_key="payments.dlq",
            headers=outgoing_headers,
            persist=True,
            message_id=message_id,
        )
        return

    retry_queue = f"payments.retry.{next_attempt}"
    await broker.publish(
        dict(body),
        exchange=RETRY_EXCHANGE,
        routing_key=retry_queue,
        headers=outgoing_headers,
        persist=True,
        expiration=settings.retry_base_delay_seconds * (2 ** (next_attempt - 1)),
        message_id=message_id,
    )
