"""Validate webhook destinations and exact host-and-port exceptions."""

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit

from app.config import get_settings
from app.exceptions import UnsafeWebhookDestinationError, WebhookDeliveryError
from app.webhook_allowlist import parse_allowed_host_port

# NAT64 addresses carry an IPv4 address in their last 32 bits.
NAT64_PREFIX = ipaddress.ip_network("64:ff9b::/96")


def is_public_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Return True only for globally routable addresses, including NAT64-wrapped IPv4."""
    # Unwrap the embedded IPv4 address so the IPv4 rules apply to it.
    if isinstance(ip, ipaddress.IPv6Address) and ip in NAT64_PREFIX:
        ip = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
    return ip.is_global


def webhook_destination_is_allowed(url: str, allowed_host_ports: list[str]) -> bool:
    """Match a webhook URL against an exact allowlisted hostname and effective port."""
    parsed = urlsplit(url)
    # Only HTTP(S) URLs can use an allowlist exception.
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
        return False

    try:
        requested_port = parsed.port or (443 if parsed.scheme == "https" else 80)
        requested_host_port = (parsed.hostname.lower(), requested_port)
        return any(
            parse_allowed_host_port(entry) == requested_host_port for entry in allowed_host_ports
        )
    except ValueError:
        # Malformed ports cannot match an exception.
        return False


def validate_webhook_url(url: str, allowed_host_ports: list[str]) -> bool:
    """Reject non-public literal IPs; return True only for an exact allowlist match."""
    # Explicit exceptions require an exact host and effective port match.
    if webhook_destination_is_allowed(url, allowed_host_ports):
        return True

    parsed = urlsplit(url)
    hostname = parsed.hostname
    if hostname is None:
        # Reject malformed destinations before hostname or address policy checks.
        raise UnsafeWebhookDestinationError("webhook_url has no hostname")

    if hostname.lower() == "localhost" or hostname.lower().endswith(".localhost"):
        # Local names are blocked before any DNS lookup.
        raise UnsafeWebhookDestinationError("webhook_url must not point to a local address")

    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        # Hostnames are resolved asynchronously immediately before delivery.
        return False

    if not is_public_ip(ip):
        raise UnsafeWebhookDestinationError("webhook_url must not point to a non-public IP address")
    return False


def validate_configured_webhook_url(url: str) -> None:
    """Validate a webhook URL using the application's configured exact allowlist."""
    # Read the allowlist from cached settings on each call.
    validate_webhook_url(url, get_settings().webhook_allowed_hosts)


async def validate_webhook_destination(url: str, allowed_host_ports: list[str]) -> None:
    """Validate the destination before delivery, allowing explicitly allowlisted URLs."""
    # An exact allowlist match skips the DNS checks below.
    if validate_webhook_url(url, allowed_host_ports):
        return

    parsed = urlsplit(url)
    hostname = parsed.hostname
    if hostname is None:
        # A parsed URL without a host cannot be resolved safely.
        raise UnsafeWebhookDestinationError("webhook_url has no hostname")

    try:
        addresses = await asyncio.get_running_loop().getaddrinfo(
            hostname,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
    except OSError as exception:
        raise WebhookDeliveryError(f"webhook host resolution failed: {exception}") from exception

    if not addresses:
        raise WebhookDeliveryError("webhook host resolved to no addresses")

    for address in addresses:
        ip = ipaddress.ip_address(address[4][0])
        # Reject the whole hostname if any DNS answer is non-public.
        if not is_public_ip(ip):
            raise UnsafeWebhookDestinationError("webhook resolves to a non-public IP")
