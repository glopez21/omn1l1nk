from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "OMN1L1NK_", "env_file": ".env"}

    db_url: str = "postgresql+asyncpg://omn1l1nk_user:changeme_omn1l1nk@localhost:5435/shared_db"
    augur_url: str = "http://localhost:8001"
    augur_enabled: bool = True
    threatpulse_url: str = "http://threatpulse:8081"
    threatpulse_enabled: bool = False
    poll_interval: int = 3
    batch_size: int = 50
    max_retries: int = 5
    ingest_api_keys: list[str] = []


settings = Settings()
