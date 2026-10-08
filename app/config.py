"""Application settings loaded from environment variables."""

from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.webhook_allowlist import parse_allowed_host_port


class Settings(BaseSettings):
    """Validated application configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: SecretStr
    rabbitmq_url: SecretStr
    api_key: SecretStr
    webhook_signing_secret: SecretStr
    webhook_allowed_hosts: list[str] = Field(default_factory=list)
    webhook_timeout_seconds: float = 10
    gateway_timeout_seconds: float = 30
    max_attempts: int = 3
    retry_base_delay_seconds: float = 2
    outbox_poll_interval_seconds: float = 1
    outbox_batch_size: int = 100
    outbox_retention_seconds: float = 604800
    log_level: str = "INFO"

    @field_validator("webhook_allowed_hosts")
    @classmethod
    def validate_webhook_allowed_hosts(cls, values: list[str]) -> list[str]:
        # Reject patterns at startup so only exact host:port exceptions are usable.
        for value in values:
            # Fail during settings validation rather than at webhook delivery time.
            parse_allowed_host_port(value)
        return values


@lru_cache
def get_settings() -> Settings:
    """Return the cached settings instance."""
    # Cache one validated settings object for the lifetime of this process.
    return Settings()
