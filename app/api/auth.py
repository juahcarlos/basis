"""API-key authentication for protected routes."""

import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

from app.api.deps import SettingsDep

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    api_key: Annotated[str | None, Depends(api_key_header)],
    settings: SettingsDep,
) -> None:
    """Reject requests without the configured API key."""
    # Resolve the secret once, then compare without data-dependent timing.
    configured_key = settings.api_key.get_secret_value()
    if api_key is None or not secrets.compare_digest(api_key.encode(), configured_key.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
