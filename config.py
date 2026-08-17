import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PSEUDOGRAM_BASE_URL: str = os.getenv("PSEUDOGRAM_BASE_URL", os.getenv("MOCK_API_BASE_URL", "https://pseudogram-api.onrender.com")).strip()
    PSEUDOGRAM_API_KEY: str = os.getenv("PSEUDOGRAM_API_KEY", os.getenv("API_KEY", "")).strip()
    DB_PATH: str = os.getenv("DB_PATH", "linkplease.db").strip()
    VERIFY_WEBHOOK_SIGNATURE: bool = os.getenv("VERIFY_WEBHOOK_SIGNATURE", "true").lower() == "true"
    
    @property
    def API_KEY(self) -> str:
        return self.PSEUDOGRAM_API_KEY.strip()

    @property
    def MOCK_API_BASE_URL(self) -> str:
        return self.PSEUDOGRAM_BASE_URL.strip()

    # Rate limiting for outbound POST /v1/dm/send: 10 requests per rolling 60s
    RATE_LIMIT_MAX_REQUESTS: int = 10
    RATE_LIMIT_WINDOW_SECONDS: float = 60.0
    MIN_SEND_INTERVAL_SECONDS: float = 6.05  # Ensures we never breach 10 req / 60s
    
    MAX_RETRIES: int = 5
    WORKER_POLL_INTERVAL: float = 1.0  # background loop check interval in seconds
    RECONCILIATION_INTERVAL: float = 2.0  # check pending DMs every 2 seconds
    PROCESSING_TIMEOUT_SECONDS: float = 30.0  # timeout to recover stuck 'processing' jobs

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
