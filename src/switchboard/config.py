"""Settings, loaded once from the environment."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://sync:sync@localhost:5433/switchboard"

    # base64 of 32 random bytes; see .env.example for how to generate one
    credentials_key: str = ""

    port: int = 8000
    public_url: str = "http://localhost:8000"

    stripe_api_key: str = ""
    stripe_webhook_secret: str = ""

    hubspot_client_id: str = ""
    hubspot_client_secret: str = ""
    hubspot_webhook_secret: str = ""

    anthropic_api_key: str = ""
    mapping_model: str = "claude-sonnet-5-5"


@lru_cache
def get_settings() -> Settings:
    return Settings()
