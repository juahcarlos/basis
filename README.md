# Asynchronous Payment Processing Service

FastAPI accepts payments, PostgreSQL stores them together with a transactional outbox, RabbitMQ delivers events to a consumer process, and the processing result is sent to `webhook_url`.

## Processing Flow

1. The client sends an API request with an `Idempotency-Key`.
2. Within a single transaction, the API writes the payment to `payments` and a `payment.new` event to `outbox`.
3. The outbox publisher selects pending events, publishes them to `payments.new`, and marks them as published only after receiving a publisher confirm from RabbitMQ.
4. The consumer receives the event, calls FakeGateway, and stores the payment's final status.
5. The consumer sends a webhook with the processing result.
6. A processing or webhook delivery failure triggers a retry; after all attempts are exhausted, the message is sent to the DLQ.

The outbox and webhook provide **at-least-once** delivery, not exactly-once delivery: messages and webhooks may be delivered more than once. The consumer saves the final status before sending the webhook, so redelivery of an already completed payment retries the webhook but does not call the gateway again. Two workers that read `pending` at the same time may both call the gateway; a DB lock is acquired only for a short recheck and for saving the result. There is no shared transaction between the external gateway and PostgreSQL, so a real gateway must deduplicate calls using the idempotency key; the consumer passes the payment ID to it. FakeGateway accepts this key but does not use it.

## Running the Service

Docker Engine and Docker Compose are required. The application, Python tools, tests, and migrations run inside containers.

Copy the environment template:

```bash
cp .env.example .env
```

On the first run, generate `uv.lock` using a one-off container with uv. This command does not build the project or create a `.venv` on the host:

```bash
docker run --rm -v "$PWD":/work -w /work ghcr.io/astral-sh/uv:python3.13-bookworm-slim uv lock
```

Start the full stack:

```bash
docker compose up --build -d
```

Compose waits for PostgreSQL and RabbitMQ, runs the migration through the `migrate` service, and then starts the API and consumer.

- Swagger: <http://localhost:8001/docs>
- Health: <http://localhost:8001/health>
- RabbitMQ Management: <http://localhost:15672>

In the local environment, the API is published on host port `8001` and listens on port `8000` inside the container. Service credentials are configured in `.env`; the template uses `payments` and `change-me`.

Stop the services without deleting data:

```bash
docker compose down
```

`docker compose down -v` also deletes the PostgreSQL volume and RabbitMQ state.

## API

Protected endpoints require `X-API-Key`, whose value must match `API_KEY` in `.env`. The examples below use the template value `change-me`.

### Create a Payment

```bash
curl -i -X POST http://localhost:8001/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change-me' \
  -H 'Idempotency-Key: example-payment-001' \
  -d '{"amount":"25.50","currency":"USD","description":"Invoice 001","metadata":{},"webhook_url":"https://example.test/webhook"}'
```

The expected response is `202 Accepted`, containing `payment_id`, `status`, and `created_at`.

### Retry Creation with the Same Key

Send the same request with the same body and `Idempotency-Key`: the API returns the same `payment_id` without creating another outbox record. If the body differs, the response is `409 Conflict`.

### Retrieve a Payment

Replace the placeholder with the `payment_id` returned by the create request:

```bash
curl -i -H 'X-API-Key: change-me' \
  http://localhost:8001/api/v1/payments/<payment_id>
```

The `200 OK` response contains the payment fields. The amount is serialized as a JSON string, for example `"25.50"`.

### Request Without an Idempotency Key

```bash
curl -i -X POST http://localhost:8001/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change-me' \
  -d '{"amount":"25.50","currency":"USD","description":"Invoice 001","metadata":{},"webhook_url":"https://example.test/webhook"}'
```

Without an `Idempotency-Key`, FastAPI returns `422 Unprocessable Content`. Protected endpoints return `401 Unauthorized` if the API key is missing or invalid; a payment that does not exist returns `404 Not Found`.

`/health` does not require an API key. It checks PostgreSQL with a query and RabbitMQ using a separate AMQP connection; if either dependency is unavailable, it returns `503`.

`description` is limited to 500 characters. `webhook_url` is limited to 2048 characters by the API schema and is not truncated. `metadata` is limited to 4096 bytes after compact JSON serialization.

Webhook URLs are accepted only for public host/IP addresses. Before sending a request, the service resolves the hostname again and rejects addresses that are not public. This permanent SSRF rejection sends the message directly to the DLQ, while temporary DNS errors follow the regular retry flow. To allow a strictly trusted local address, set `WEBHOOK_ALLOWED_HOSTS` to a JSON array of exact `host:port` pairs, for example `WEBHOOK_ALLOWED_HOSTS=["127.0.0.1:9000"]`; the list is empty by default. Wildcards and CIDR ranges are not supported. The allowlist bypasses the public-address check only for an exact host-and-port match. DNS validation is performed before the HTTP request, but the client resolves the hostname independently afterward; this does not protect against DNS rebinding between validation and connection.

Each webhook is signed with the secret configured in `WEBHOOK_SIGNING_SECRET`. The headers are `X-Webhook-Timestamp` and `X-Webhook-Signature: sha256=<hex>`, where the signature is an HMAC-SHA256 of the string `<timestamp>.<request body>`. The receiver must verify the signature and reject requests with stale timestamps. The 64 KiB request-body size limit is checked using the `Content-Length` header; requests without this header (chunked requests) are not subject to this limit.

## Outbox, Retries, and DLQ

The outbox publisher polls the database at intervals defined by `OUTBOX_POLL_INTERVAL_SECONDS`, selects up to `OUTBOX_BATCH_SIZE` pending events, and publishes them with durable delivery. The `message_id` is set to the ID of the outbox record. The RabbitMQ confirmation is recorded in a separate, short transaction. In the current implementation, the publisher must run as a single instance: multiple instances may select and publish the same record concurrently. If RabbitMQ is unavailable or a publication is not confirmed, the record remains `pending` for the next cycle.

With the template settings `RETRY_BASE_DELAY_SECONDS=2` and `MAX_ATTEMPTS=3`, a message is published to `payments.retry.1` with `expiration=2_000`, then to `payments.retry.2` with `expiration=4_000`. After the third failed processing attempt, it is sent to `payments.dlq` with the `x-error-reason` header. If webhook delivery fails, the payment status remains final.

Messages can be inspected in RabbitMQ Management: **Queues and Streams → payments.dlq → Get messages**. To retry processing, publish the message body back to the `payments` exchange with the routing key `payments.new`; remove the message from the DLQ only after the publication succeeds. The `payments.new` and `payments.retry.*` queues are created by the consumer at startup.

To test a successful webhook, use an available HTTPS endpoint that can receive requests, such as a URL provided by webhook.site. The service sends an outbound POST to that URL from inside its container.

## Tests and Local Development

The development environment and all project commands run in Docker. Integration and E2E tests use a separate PostgreSQL database named `payments_test`. When pytest starts, this database is recreated and Alembic migrations are applied to it. SQLite and Testcontainers are not used. Tables are cleared before and after each DB-backed test. E2E tests are included in the regular test run: pytest starts the API and consumer as subprocesses, uses a separate RabbitMQ vhost, and runs a local HTTP webhook receiver.

```bash
docker compose run --rm app pytest -q tests
docker compose run --rm app ruff check .
docker compose run --rm app mypy app
```

For a faster run without E2E scenarios, use:

```bash
docker compose run --rm app pytest -q tests -m "not e2e"
```

Apply migrations manually:

```bash
docker compose run --rm app alembic upgrade head
```

## Design Decisions

- The incoming `description` is limited to 1–500 characters.
- Decimal values are returned as strings in JSON, for example `"25.50"`.
- The API key used in the `.env.example` examples is a test value, not a production secret.
- FakeGateway waits for 2–5 seconds and returns `succeeded` with a 90% probability or `failed` with a 10% probability.
- The maximum of three processing attempts includes the initial delivery; after the third failure, the message is sent to the DLQ.
