"""String enums used by payment and outbox records."""

from enum import StrEnum


class PaymentStatus(StrEnum):
    """Processing state of a payment."""

    # New records remain processable until the gateway returns a final outcome.
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Currency(StrEnum):
    """Supported payment currencies."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class OutboxStatus(StrEnum):
    """Publishing state of an outbox event."""

    # Published rows remain available for operational history.
    PENDING = "pending"
    PUBLISHED = "published"
