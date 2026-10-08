"""RabbitMQ exchanges, queues, and bindings for payment events."""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import Settings

PAYMENTS_EXCHANGE = RabbitExchange(
    "payments",
    type=ExchangeType.DIRECT,
    durable=True,
)
RETRY_EXCHANGE = RabbitExchange(
    "payments.retry",
    type=ExchangeType.DIRECT,
    durable=True,
)
DLX_EXCHANGE = RabbitExchange(
    "payments.dlx",
    type=ExchangeType.DIRECT,
    durable=True,
)
PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dlq",
    },
)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True)


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Declare durable exchanges and bind payment and dead-letter queues."""
    # Declare exchanges before queues so every binding target exists.
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry_exchange = await broker.declare_exchange(RETRY_EXCHANGE)
    dlx_exchange = await broker.declare_exchange(DLX_EXCHANGE)

    payments_queue = await broker.declare_queue(PAYMENTS_QUEUE)
    dlq_queue = await broker.declare_queue(DLQ_QUEUE)

    await payments_queue.bind(payments_exchange, routing_key="payments.new")
    await dlq_queue.bind(dlx_exchange, routing_key="payments.dlq")

    # Number retry queues from one so the consumer can use x-attempt directly.
    for attempt in range(1, settings.max_attempts):
        # Expired messages are routed back to the primary payment queue.
        queue_name = f"payments.retry.{attempt}"
        retry_queue = await broker.declare_queue(
            RabbitQueue(
                queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "payments",
                    "x-dead-letter-routing-key": "payments.new",
                },
            )
        )
        await retry_queue.bind(retry_exchange, routing_key=queue_name)
