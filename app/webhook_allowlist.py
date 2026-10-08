"""Parse exact host-and-port webhook allowlist entries."""

from urllib.parse import urlsplit


def parse_allowed_host_port(value: str) -> tuple[str, int]:
    """Parse one literal host:port entry and reject patterns or network ranges."""
    if value != value.strip() or any(character.isspace() for character in value):
        raise ValueError("webhook allowlist entries must be exact host:port values")
    if "*" in value or "/" in value:
        raise ValueError("webhook allowlist entries must be exact host:port values")

    parsed = urlsplit(f"//{value}")
    if (
        parsed.hostname is None
        or parsed.port is None
        or parsed.port < 1
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("webhook allowlist entries must be exact host:port values")

    return parsed.hostname.lower(), parsed.port
