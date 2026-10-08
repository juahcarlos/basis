"""FastStream consumer process and outbox publisher lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from faststream import Context, ContextRepo, FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.broker.handlers import handle_payment_message
from app.broker.publisher import OutboxPublisher
from app.broker.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    declare_topology,
)
from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory
from app.logging import configure_logging
from app.services.gateway import FakeGateway, PaymentGateway
from app.services.webhook import WebhookService

CONSUMER_RESOURCES_CONTEXT = Context("consumer_resources")


@dataclass
class ConsumerResources:
    """Hold all resources owned by one consumer process lifecycle."""

    # Shared collaborators are created once and disposed with the application.
    settings: Settings
    broker: RabbitBroker
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    http_client: httpx.AsyncClient
    webhook: WebhookService
    gateway: PaymentGateway
    publisher: OutboxPublisher
    stop_event: asyncio.Event
    publisher_task: asyncio.Task[None] | None = None


def create_consumer_app() -> FastStream:
    """Build the FastStream app without opening process resources at import time."""

    @asynccontextmanager
    async def lifespan(context: ContextRepo) -> AsyncIterator[None]:
        """Connect resources and run the outbox publisher with the app lifecycle."""
        # Defer environment parsing until startup so importing this module stays side-effect free.
        settings = get_settings()
        configure_logging(settings)
        broker = RabbitBroker(
            settings.rabbitmq_url.get_secret_value(),
            default_channel=Channel(publisher_confirms=True),
        )
        engine = create_engine(settings.database_url.get_secret_value())
        session_factory = create_session_factory(engine)
        http_client = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds)
        webhook = WebhookService(http_client, settings)
        gateway: PaymentGateway = FakeGateway()
        publisher = OutboxPublisher(broker, settings, session_factory)
        stop_event = asyncio.Event()
        resources = ConsumerResources(
            settings=settings,
            broker=broker,
            engine=engine,
            session_factory=session_factory,
            http_client=http_client,
            webhook=webhook,
            gateway=gateway,
            publisher=publisher,
            stop_event=stop_event,
        )
        # Make lifecycle-owned dependencies available to message handlers.
        context.set_global("consumer_resources", resources)
        application.set_broker(broker)

        @broker.subscriber(
            PAYMENTS_QUEUE,
            exchange=PAYMENTS_EXCHANGE,
            channel=Channel(prefetch_count=10, publisher_confirms=True),
        )
        async def process_payment_message(
            body: dict[str, object],
            message: RabbitMessage,
            current_resources: ConsumerResources = CONSUMER_RESOURCES_CONTEXT,
        ) -> None:
            """Delegate each broker delivery to resources supplied through FastStream context."""
            # Pass the process-scoped dependencies through the message boundary.
            await handle_payment_message(
                body,
                headers=message.headers or {},
                message_id=message.message_id,
                broker=current_resources.broker,
                session_factory=current_resources.session_factory,
                gateway=current_resources.gateway,
                webhook=current_resources.webhook,
                settings=current_resources.settings,
            )

        try:
            await broker.connect()
            await declare_topology(broker, settings)
            resources.publisher_task = asyncio.create_task(publisher.run(stop_event))
            yield
        finally:
            # Signal the worker before waiting for it to finish.
            stop_event.set()
            # Await the worker before releasing its database engine.
            # Startup may fail before the publisher task has been created.
            if resources.publisher_task is not None:
                await resources.publisher_task
            await http_client.aclose()
            await engine.dispose()

    application = FastStream(lifespan=lifespan)
    return application


app = create_consumer_app()
