"""Publish pending outbox records to RabbitMQ."""

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker.topology import PAYMENTS_EXCHANGE
from app.config import Settings
from app.db.uow import UnitOfWork

logger = logging.getLogger(__name__)

# How often published events are purged from the outbox table.
CLEANUP_INTERVAL_SECONDS = 3600
# File touched on every publisher cycle so the container healthcheck can see liveness.
HEARTBEAT_PATH = Path("/tmp/outbox_heartbeat")


class OutboxPublisher:
    """Publish outbox batches and mark them after RabbitMQ confirmation."""

    def __init__(
        self,
        broker: RabbitBroker,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._broker = broker
        self._settings = settings
        self._session_factory = session_factory
        # Monotonic time of the next cleanup; zero runs it on the first cycle.
        self._next_cleanup_at = 0.0

    async def run(self, stop_event: asyncio.Event) -> None:
        """Poll and publish until the stop event is set."""
        # Stop only between batches; an in-flight publish must finish or fail first.
        while not stop_event.is_set():
            try:
                await self._publish_batch()
            except Exception as exception:
                # Keep later events moving when one event fails to publish.
                logger.error(
                    "\n\n !!! --- ERROR --- !!! %s OutboxPublisher.run "
                    "outbox batch publication failed e=%s\n\n",
                    __file__,
                    exception,
                )

            try:
                await self._cleanup_published()
            except Exception as exception:
                # A failed cleanup must never stop event publication.
                logger.error(
                    "\n\n !!! --- ERROR --- !!! %s OutboxPublisher.run "
                    "outbox cleanup failed e=%s\n\n",
                    __file__,
                    exception,
                )

            # Report that the publisher loop is still alive.
            self._write_heartbeat()

            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self._settings.outbox_poll_interval_seconds,
                )
            except TimeoutError:
                # Start the next polling cycle after the configured interval.
                continue

    def _write_heartbeat(self) -> None:
        """Touch the heartbeat file; a failure must not stop the publisher."""
        try:
            HEARTBEAT_PATH.touch()
        except OSError as exception:
            # The healthcheck will report the stale file, so only log here.
            logger.error(
                "\n\n !!! --- ERROR --- !!! %s OutboxPublisher._write_heartbeat "
                "heartbeat file update failed e=%s\n\n",
                __file__,
                exception,
            )

    async def _cleanup_published(self) -> None:
        """Delete old published events, at most once per cleanup interval."""
        # Skip cleanup until the interval since the previous one has passed.
        if time.monotonic() < self._next_cleanup_at:
            return

        # Schedule the next run first so a failure does not retry every cycle.
        self._next_cleanup_at = time.monotonic() + CLEANUP_INTERVAL_SECONDS
        cutoff = datetime.now(UTC) - timedelta(seconds=self._settings.outbox_retention_seconds)
        async with UnitOfWork(self._session_factory) as uow:
            await uow.outbox.delete_published_before(cutoff)
            await uow.commit()

    async def _publish_batch(self) -> None:
        """Read candidates, then publish and commit each event independently."""
        # Release the read transaction before any broker network call.
        async with UnitOfWork(self._session_factory) as uow:
            events = await uow.outbox.fetch_pending_batch(self._settings.outbox_batch_size)
            candidates = [(event.id, dict(event.payload), event.event_type) for event in events]

        for event_id, payload, event_type in candidates:
            try:
                await self._publish_event(event_id, payload, event_type)
                async with UnitOfWork(self._session_factory) as uow:
                    await uow.outbox.mark_published([event_id])
                    await uow.commit()
            except Exception as exception:
                # Retain the pending event for a later polling cycle.
                logger.error(
                    "\n\n !!! --- ERROR --- !!! %s OutboxPublisher._publish_batch "
                    "outbox event %s publication failed e=%s\n\n",
                    __file__,
                    event_id,
                    exception,
                )

    async def _publish_event(
        self,
        event_id: UUID,
        payload: dict[str, object],
        event_type: str,
    ) -> None:
        """Publish one durable event with identifying AMQP metadata."""
        # The event is marked published only after this durable publish is confirmed.
        await self._broker.publish(
            payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key="payments.new",
            headers={"event_type": event_type},
            persist=True,
            message_id=str(event_id),
        )
