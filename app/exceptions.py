"""Domain exceptions raised by payment operations."""


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is reused with different payment data."""

    # The API maps this domain conflict to HTTP 409.


class PaymentNotFoundError(Exception):
    """Raised when a requested payment does not exist."""

    # The API maps this lookup failure to HTTP 404.


class WebhookDeliveryError(Exception):
    """Raised when a payment webhook cannot be delivered successfully."""

    # The consumer retries this operational failure before routing it to the DLQ.


class UnsafeWebhookDestinationError(WebhookDeliveryError):
    """Raised when a webhook URL targets a disallowed host or IP address."""

    # The consumer treats a policy violation as permanent and sends it directly to the DLQ.
