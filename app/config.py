from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: str = Field(min_length=16)
    outbox_poll_seconds: float = Field(default=0.5, gt=0)
    webhook_timeout_seconds: float = Field(default=10, gt=0)
