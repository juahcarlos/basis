"""HTTP integration tests using ASGITransport and PostgreSQL."""

from uuid import uuid4

import pytest
from httpx import AsyncClient

API_KEY = "change-me"
AUTH_HEADERS = {"X-API-Key": API_KEY}


def create_payload(amount: str = "25.50") -> dict[str, object]:
    """Return valid API input for payment creation."""
    return {
        "amount": amount,
        "currency": "USD",
        "description": "API integration payment",
        "metadata": {},
        "webhook_url": "https://example.test/webhook",
    }


@pytest.mark.asyncio
async def test_create_payment_and_read_it(api_client: AsyncClient) -> None:
    """Create a payment and retrieve the stored representation."""
    created = await api_client.post(
        "/api/v1/payments",
        headers={**AUTH_HEADERS, "Idempotency-Key": f"api-{uuid4()}"},
        json=create_payload(),
    )

    assert created.status_code == 202
    payment_id = created.json()["payment_id"]

    received = await api_client.get(
        f"/api/v1/payments/{payment_id}",
        headers=AUTH_HEADERS,
    )

    assert received.status_code == 200
    assert received.json()["amount"] == "25.50"
    assert received.json()["status"] == "pending"


@pytest.mark.asyncio
async def test_api_idempotency_repeat_and_conflict(api_client: AsyncClient) -> None:
    """Return the same ID for matching requests and 409 for a changed body."""
    key = f"api-idempotency-{uuid4()}"
    first = await api_client.post(
        "/api/v1/payments",
        headers={**AUTH_HEADERS, "Idempotency-Key": key},
        json=create_payload(),
    )
    repeated = await api_client.post(
        "/api/v1/payments",
        headers={**AUTH_HEADERS, "Idempotency-Key": key},
        json=create_payload(),
    )
    conflict = await api_client.post(
        "/api/v1/payments",
        headers={**AUTH_HEADERS, "Idempotency-Key": key},
        json=create_payload("26.50"),
    )

    assert first.status_code == repeated.status_code == 202
    assert first.json()["payment_id"] == repeated.json()["payment_id"]
    assert conflict.status_code == 409


@pytest.mark.asyncio
async def test_authentication_and_required_idempotency_header(api_client: AsyncClient) -> None:
    """Require API key and idempotency header for protected create requests."""
    missing_key = await api_client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": "missing-auth"},
        json=create_payload(),
    )
    invalid_key = await api_client.post(
        "/api/v1/payments",
        headers={"X-API-Key": "wrong", "Idempotency-Key": "invalid-auth"},
        json=create_payload(),
    )
    missing_idempotency = await api_client.post(
        "/api/v1/payments",
        headers=AUTH_HEADERS,
        json=create_payload(),
    )

    assert missing_key.status_code == 401
    assert invalid_key.status_code == 401
    assert missing_idempotency.status_code == 422


@pytest.mark.asyncio
async def test_get_missing_payment_returns_404(api_client: AsyncClient) -> None:
    """Map a valid but unknown UUID to the domain 404 response."""
    response = await api_client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000",
        headers=AUTH_HEADERS,
    )

    assert response.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {
            "amount": "0",
            "currency": "USD",
            "description": "x",
            "webhook_url": "https://example.test",
        },
        {
            "amount": "1",
            "currency": "GBP",
            "description": "x",
            "webhook_url": "https://example.test",
        },
        {
            "amount": "1",
            "currency": "USD",
            "description": "x",
            "webhook_url": "not-a-url",
        },
        {
            "amount": "1",
            "currency": "USD",
            "description": "x",
            "webhook_url": "http://localhost:8080/test",
        },
    ],
)
async def test_create_validation_errors_return_422(
    api_client: AsyncClient,
    payload: dict[str, str],
) -> None:
    """Reject non-positive amounts, unsupported currencies, and malformed webhook URLs."""
    response = await api_client.post(
        "/api/v1/payments",
        headers={**AUTH_HEADERS, "Idempotency-Key": f"invalid-{uuid4()}"},
        json=payload,
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_health_and_openapi_include_api_key_security(api_client: AsyncClient) -> None:
    """Keep health public and advertise API-key auth in OpenAPI."""
    health = await api_client.get("/health")
    openapi = await api_client.get("/openapi.json")

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert "X-API-Key" in str(openapi.json())


@pytest.mark.asyncio
async def test_non_ascii_api_key_returns_401(api_client: AsyncClient) -> None:
    """Reject a non-ASCII API key with 401 instead of failing with a server error."""
    response = await api_client.get(
        f"/api/v1/payments/{uuid4()}",
        headers={"X-API-Key": "é".encode()},
    )

    # The key must be rejected, never crash the request.
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_oversized_request_body_returns_413(api_client: AsyncClient) -> None:
    """Refuse a body above the size limit before it is parsed."""
    response = await api_client.post(
        "/api/v1/payments",
        headers={
            **AUTH_HEADERS,
            "Idempotency-Key": f"api-{uuid4()}",
            "Content-Type": "application/json",
        },
        content=b"a" * 70000,
    )

    assert response.status_code == 413
