from functools import lru_cache
from typing import Annotated

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://paystream:paystream@localhost:5432/paystream"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # No default key on purpose: the API refuses to start without one.
    api_keys: Annotated[list[SecretStr], NoDecode] = Field(default_factory=list)

    webhook_signing_secret: SecretStr | None = None
    webhook_allowed_hosts: Annotated[list[str], NoDecode] = Field(default_factory=list)
    webhook_connect_timeout: float = 3.0
    webhook_read_timeout: float = 5.0

    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = Field(default=0.9, ge=0.0, le=1.0)

    retry_max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_ms: int = Field(default=2000, ge=1)
    consumer_prefetch: int = Field(default=10, ge=1)
    delivery_limit: int = Field(default=10, ge=1)

    outbox_batch_size: int = Field(default=100, ge=1)
    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_max_backoff: float = Field(default=60.0, gt=0)
    outbox_retention_days: int = Field(default=7, ge=1)

    @field_validator("api_keys", "webhook_allowed_hosts", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @property
    def retry_delays_ms(self) -> list[int]:
        """Delay before attempt n+1, for n = 1 .. max_attempts-1."""
        return [self.retry_base_delay_ms * 2**i for i in range(self.retry_max_attempts - 1)]


@lru_cache
def get_settings() -> Settings:
    return Settings()
