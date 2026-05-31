from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "OMN1L1NK_", "env_file": ".env"}

    db_url: str = "postgresql+asyncpg://omn1l1nk_user:changeme_omn1l1nk@localhost:5435/shared_db"
    db_pool_min: int = Field(default=2, ge=1, le=50)
    db_pool_max: int = Field(default=10, ge=1, le=100)
    db_connect_timeout: int = Field(default=30, ge=1)
    db_statement_timeout: int = Field(default=30, ge=0)

    augur_url: str = "http://localhost:8001"
    augur_enabled: bool = True
    threatpulse_url: str = "http://threatpulse:8081"
    threatpulse_enabled: bool = False

    poll_interval: int = Field(default=3, ge=1, le=60)
    batch_size: int = Field(default=50, ge=1, le=500)
    max_retries: int = Field(default=5, ge=0, le=50)

    ingest_api_keys: list[str] = []
    log_level: str = "INFO"

    dead_letter_path: str = "/tmp/omn1l1nk_dead_letter.jsonl"
    rules_cache_ttl: int = Field(default=30, ge=5, le=300)


settings = Settings()
