"""Webhook SSRF protection tests."""

import asyncio
import hashlib
import hmac
import socket
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

from app.config import Settings
from app.exceptions import WebhookDeliveryError
from app.models.enums import Currency, PaymentStatus
from app.schemas.payment import PaymentCreate
from app.services.webhook import WebhookService


@pytest.mark.asyncio
async def test_webhook_rejects_public_hostname_resolving_to_loopback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject DNS rebinding to loopback before the HTTP client sends a request."""
    sent_requests: list[httpx.Request] = []

    async def capture_request(request: httpx.Request) -> httpx.Response:
        sent_requests.append(request)
        return httpx.Response(200)

    async def resolve_to_loopback(
        self: asyncio.BaseEventLoop,
        host: str,
        port: int,
        *args: object,
        **kwargs: object,
    ) -> list[tuple[int, int, int, str, tuple[str, int]]]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]

    monkeypatch.setattr(asyncio.BaseEventLoop, "getaddrinfo", resolve_to_loopback)
    async with httpx.AsyncClient(transport=httpx.MockTransport(capture_request)) as client:
        webhook = WebhookService(client)
        payment = SimpleNamespace(
            id="6c13feef-1b95-42c7-827c-7d151fe60cae",
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("1.00"),
            currency=Currency.RUB,
            processed_at=None,
            webhook_url="https://public-looking.example/hook",
        )

        with pytest.raises(WebhookDeliveryError, match="non-public IP"):
            await webhook.deliver(payment)  # type: ignore[arg-type]

    assert sent_requests == []


def make_settings(allowed_host: str) -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://payments:secret@postgres/payments",
        rabbitmq_url="amqp://payments:secret@rabbitmq/",
        api_key="test-key",
        webhook_signing_secret="test-secret",
        webhook_allowed_hosts=[allowed_host],
    )


def test_payment_create_accepts_only_exact_allowlisted_host_and_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = make_settings("127.0.0.1:43210")
    monkeypatch.setattr("app.webhook_security.get_settings", lambda: settings)
    request = {
        "amount": "1.00",
        "currency": "RUB",
        "description": "Local webhook test",
        "webhook_url": "http://127.0.0.1:43210/hook",
    }

    assert PaymentCreate.model_validate(request).webhook_url.port == 43210

    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**request, "webhook_url": "http://127.0.0.1:43211/hook"})


def test_settings_reject_wildcard_or_network_allowlist_entries() -> None:
    with pytest.raises(ValidationError):
        make_settings("127.0.0.0/8:43210")

    with pytest.raises(ValidationError):
        make_settings("*.example.test:43210")


def test_payment_create_rejects_metadata_over_4096_serialized_bytes() -> None:
    with pytest.raises(ValidationError, match="4096 serialized bytes"):
        PaymentCreate.model_validate(
            {
                "amount": "1.00",
                "currency": "RUB",
                "description": "Metadata size test",
                "metadata": {"value": "é" * 2050},
                "webhook_url": "https://example.test/hook",
            }
        )


def test_payment_create_accepts_url_up_to_2048_characters() -> None:
    url_prefix = "https://example.test/"
    webhook_url = url_prefix + "a" * (2048 - len(url_prefix))
    request = {
        "amount": "1.00",
        "currency": "RUB",
        "description": "URL length test",
        "webhook_url": webhook_url,
    }

    assert len(str(PaymentCreate.model_validate(request).webhook_url)) == 2048


@pytest.mark.asyncio
async def test_webhook_service_allows_exact_configured_loopback_endpoint() -> None:
    sent_requests: list[httpx.Request] = []

    async def capture_request(request: httpx.Request) -> httpx.Response:
        sent_requests.append(request)
        return httpx.Response(200)

    settings = make_settings("127.0.0.1:43210")
    async with httpx.AsyncClient(transport=httpx.MockTransport(capture_request)) as client:
        webhook = WebhookService(client, settings)
        payment = SimpleNamespace(
            id="6c13feef-1b95-42c7-827c-7d151fe60cae",
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("1.00"),
            currency=Currency.RUB,
            processed_at=None,
            webhook_url="http://127.0.0.1:43210/hook",
        )

        await webhook.deliver(payment)  # type: ignore[arg-type]

    assert len(sent_requests) == 1
    assert str(sent_requests[0].url) == "http://127.0.0.1:43210/hook"


@pytest.mark.asyncio
async def test_webhook_service_rejects_same_host_on_unlisted_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent_requests: list[httpx.Request] = []

    async def capture_request(request: httpx.Request) -> httpx.Response:
        sent_requests.append(request)
        return httpx.Response(200)

    async def resolve_to_loopback(
        self: asyncio.BaseEventLoop,
        host: str,
        port: int,
        *args: object,
        **kwargs: object,
    ) -> list[tuple[int, int, int, str, tuple[str, int]]]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]

    monkeypatch.setattr(asyncio.BaseEventLoop, "getaddrinfo", resolve_to_loopback)
    settings = make_settings("127.0.0.1:43210")
    async with httpx.AsyncClient(transport=httpx.MockTransport(capture_request)) as client:
        webhook = WebhookService(client, settings)
        payment = SimpleNamespace(
            id="6c13feef-1b95-42c7-827c-7d151fe60cae",
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("1.00"),
            currency=Currency.RUB,
            processed_at=None,
            webhook_url="http://127.0.0.1:43211/hook",
        )

        with pytest.raises(WebhookDeliveryError, match="non-public IP"):
            await webhook.deliver(payment)  # type: ignore[arg-type]

    assert sent_requests == []


@pytest.mark.asyncio
async def test_webhook_request_is_signed_with_configured_secret() -> None:
    """Send a timestamp and an HMAC signature that the receiver can verify."""
    sent_requests: list[httpx.Request] = []

    async def capture_request(request: httpx.Request) -> httpx.Response:
        sent_requests.append(request)
        return httpx.Response(200)

    settings = make_settings("127.0.0.1:43210")
    async with httpx.AsyncClient(transport=httpx.MockTransport(capture_request)) as client:
        webhook = WebhookService(client, settings)
        payment = SimpleNamespace(
            id="6c13feef-1b95-42c7-827c-7d151fe60cae",
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("1.00"),
            currency=Currency.RUB,
            processed_at=None,
            webhook_url="http://127.0.0.1:43210/hook",
        )
        await webhook.deliver(payment)  # type: ignore[arg-type]

    # Recompute the signature exactly as a receiver would.
    request = sent_requests[0]
    timestamp = request.headers["X-Webhook-Timestamp"]
    expected = hmac.new(
        b"test-secret",
        timestamp.encode() + b"." + request.content,
        hashlib.sha256,
    ).hexdigest()
    assert request.headers["X-Webhook-Signature"] == f"sha256={expected}"


def test_payment_create_checks_ipv4_wrapped_in_nat64(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply IPv4 rules to NAT64 addresses: loopback is rejected, a public one is allowed."""
    settings = make_settings("127.0.0.1:43210")
    monkeypatch.setattr("app.webhook_security.get_settings", lambda: settings)
    request = {
        "amount": "1.00",
        "currency": "RUB",
        "description": "NAT64 webhook test",
        "webhook_url": "http://[64:ff9b::7f00:1]/hook",
    }

    # 64:ff9b::7f00:1 embeds 127.0.0.1.
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(request)

    # 64:ff9b::808:808 embeds 8.8.8.8.
    request["webhook_url"] = "http://[64:ff9b::808:808]/hook"
    assert PaymentCreate.model_validate(request).webhook_url.host == "[64:ff9b::808:808]"
