"""End-to-end payment processing tests against real PostgreSQL and RabbitMQ."""

import json
import os
from urllib.parse import quote
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.e2e


def dead_letter_contains_payment(
    messages: list[dict[str, object]],
    payment_id: str,
) -> bool:
    """Match a DLQ entry whether RabbitMQ returns its payload as text or JSON."""
    for message in messages:
        payload = message.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                continue
        if isinstance(payload, dict) and payload.get("payment_id") == payment_id:
            return True
    return False


@pytest.mark.asyncio
async def test_payment_traverses_api_outbox_consumer_and_rabbitmq_dlq(
    e2e_runtime,
    poll_until,
) -> None:
    """Verify successful processing and webhook retry exhaustion through the full stack."""
    async with httpx.AsyncClient(base_url=e2e_runtime.api_url, timeout=5) as api:

        async def read_dead_letters() -> list[dict[str, object]]:
            response = await e2e_runtime.management.post(
                f"/api/queues/{quote(e2e_runtime.vhost, safe='')}/payments.dlq/get",
                json={
                    "count": 10,
                    "ackmode": "ack_requeue_true",
                    "encoding": "auto",
                },
            )
            response.raise_for_status()
            result = response.json()
            assert isinstance(result, list)
            return result

        base_payload = {
            "amount": "8.75",
            "currency": "USD",
            "description": "Docker end-to-end payment",
            "metadata": {"scenario": "rabbitmq-e2e"},
        }
        headers = {"X-API-Key": e2e_runtime.api_key}

        successful_payload = {
            **base_payload,
            "webhook_url": f"{e2e_runtime.webhook_url}/success",
        }
        success_key = f"e2e-success-{uuid4()}"
        success_headers = {**headers, "Idempotency-Key": success_key}
        created = await api.post(
            "/api/v1/payments",
            headers=success_headers,
            json=successful_payload,
        )
        assert created.status_code == httpx.codes.ACCEPTED
        payment_id = created.json()["payment_id"]

        repeated = await api.post(
            "/api/v1/payments",
            headers=success_headers,
            json=successful_payload,
        )
        assert repeated.status_code == httpx.codes.ACCEPTED
        assert repeated.json()["payment_id"] == payment_id

        async def read_payment(payment_id: str) -> dict[str, object]:
            response = await api.get(
                f"/api/v1/payments/{payment_id}",
                headers=headers,
            )
            response.raise_for_status()
            return response.json()

        successful_payment = await poll_until(
            lambda: read_payment(payment_id),
            lambda payment: (
                payment["status"] in {"succeeded", "failed"} and payment["processed_at"] is not None
            ),
            timeout_seconds=45,
        )
        assert successful_payment["id"] == payment_id

        async def successful_webhook_received() -> list[dict[str, object]]:
            return [call for call in e2e_runtime.webhook_calls if call["path"] == "/success"]

        successful_calls = await poll_until(
            successful_webhook_received,
            bool,
            timeout_seconds=15,
        )
        assert successful_calls[0]["status"] == 200
        assert successful_calls[0]["payload"]["payment_id"] == payment_id

        failed_payload = {
            **base_payload,
            "description": "Webhook failure E2E payment",
            "webhook_url": f"{e2e_runtime.webhook_url}/fail",
        }
        failed = await api.post(
            "/api/v1/payments",
            headers={**headers, "Idempotency-Key": f"e2e-failure-{uuid4()}"},
            json=failed_payload,
        )
        assert failed.status_code == httpx.codes.ACCEPTED
        failed_payment_id = failed.json()["payment_id"]

        dead_letters = await poll_until(
            read_dead_letters,
            lambda messages: dead_letter_contains_payment(messages, failed_payment_id),
            timeout_seconds=90,
        )

        failed_payment = await read_payment(failed_payment_id)
        assert failed_payment["status"] in {"succeeded", "failed"}
        assert failed_payment["processed_at"] is not None
        failed_calls = [call for call in e2e_runtime.webhook_calls if call["path"] == "/fail"]
        assert len(failed_calls) >= int(os.environ.get("MAX_ATTEMPTS", "3"))
        assert dead_letter_contains_payment(dead_letters, failed_payment_id)
