"""Deliver final payment states to merchant webhook endpoints."""

import hashlib
import hmac
import json
import time

import httpx

from app.config import Settings, get_settings
from app.exceptions import WebhookDeliveryError
from app.models.payment import Payment
from app.webhook_security import validate_webhook_destination


class WebhookService:
    """Send payment result payloads using a process-scoped HTTP client."""

    def __init__(self, client: httpx.AsyncClient, settings: Settings | None = None) -> None:
        # Permit explicit settings in tests while using cached settings in production.
        self._client = client
        self._settings = settings or get_settings()

    async def deliver(self, payment: Payment) -> None:
        """POST a payment result and fail on network errors or non-2xx responses."""
        # Recheck the destination immediately before opening the outbound connection.
        await self._validate_destination(payment.webhook_url)

        payload = {
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "processed_at": (
                payment.processed_at.isoformat() if payment.processed_at is not None else None
            ),
        }
        # Serialize once so the signed bytes are exactly the bytes that are sent.
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = {"Content-Type": "application/json", **self._signature_headers(body)}

        try:
            response = await self._client.post(payment.webhook_url, content=body, headers=headers)
            response.raise_for_status()
        except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError) as exception:
            # The message handler owns logging and retry routing for this failure.
            raise WebhookDeliveryError(str(exception)) from exception

    def _signature_headers(self, body: bytes) -> dict[str, str]:
        """Build HMAC signature headers for every outgoing webhook."""
        # Bind the signature to a timestamp so captured requests can be rejected as stale.
        timestamp = str(int(time.time()))
        secret_bytes = self._settings.webhook_signing_secret.get_secret_value().encode()
        digest = hmac.new(secret_bytes, timestamp.encode() + b"." + body, hashlib.sha256)
        return {
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": f"sha256={digest.hexdigest()}",
        }

    async def _validate_destination(self, url: str) -> None:
        """Resolve the destination immediately before sending and reject internal IPs."""
        # Reuse the shared policy that is also applied when a payment is accepted.
        await validate_webhook_destination(url, self._settings.webhook_allowed_hosts)
