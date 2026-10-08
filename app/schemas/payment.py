"""Request and response schemas for payment endpoints."""

import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import (
    AfterValidator,
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    UrlConstraints,
    field_validator,
)

from app.exceptions import UnsafeWebhookDestinationError
from app.models.enums import Currency, PaymentStatus
from app.webhook_security import validate_configured_webhook_url


def validate_metadata_size(value: dict[str, Any]) -> dict[str, Any]:
    """Reject metadata whose compact JSON representation exceeds four KiB."""
    # Count encoded bytes so multibyte text is charged at its actual wire size.
    serialized = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    if len(serialized.encode("utf-8")) > 4096:
        # Reject oversized data instead of silently truncating client metadata.
        raise ValueError("metadata must not exceed 4096 serialized bytes")
    return value


PaymentMetadata = Annotated[dict[str, Any], AfterValidator(validate_metadata_size)]
# Keep reusable request types so other endpoints can share the same payload constraints.
WebhookUrl = Annotated[AnyHttpUrl, UrlConstraints(max_length=2048)]


class PaymentCreate(BaseModel):
    """Validated client input for creating a payment."""

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(min_length=1, max_length=500)
    metadata: PaymentMetadata = Field(default_factory=dict)
    webhook_url: WebhookUrl

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        # Pydantic validates URL syntax and length before this destination policy check.
        try:
            validate_configured_webhook_url(str(value))
        except (UnsafeWebhookDestinationError, ValueError) as exception:
            # Convert the shared security error into a Pydantic request validation failure.
            raise ValueError(str(exception)) from exception
        return value


class PaymentCreated(BaseModel):
    """Compact response returned after accepting a payment."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    """Public representation of a stored payment."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: AnyHttpUrl
    created_at: datetime
    processed_at: datetime | None
