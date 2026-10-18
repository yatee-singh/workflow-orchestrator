from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "orderflow"
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://orderflow:orderflow@localhost:5432/orderflow"
    redis_url: str = "redis://localhost:6379/0"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    rabbitmq_management_url: str = "http://guest:guest@localhost:15672/api"

    payment_fail_rate: float = 0.2
    max_retries: int = 3


@lru_cache
def get_settings() -> Settings:
    return Settings()
