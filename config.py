
from pydantic_settings import BaseSettings, SettingsConfigDict
class Settings(BaseSettings):
    FRONTEND_URL: str = "http://localhost:5173"
    KOTAK_CONSUMER_KEY: str = ""
    KOTAK_MOBILE_NUMBER: str = ""
    KOTAK_UCC: str = ""
    KOTAK_MPIN: str = ""
    KOTAK_ENVIRONMENT: str = "prod"

    # Reliability only; trading strategy thresholds/filters are unchanged.
    KINGBRO_KEEPALIVE_ENABLED: bool = True
    KINGBRO_KEEPALIVE_INTERVAL_SECONDS: int = 480
    KINGBRO_FEED_STALE_SECONDS: int = 90
    KINGBRO_FEED_RESTART_COOLDOWN_SECONDS: int = 60
    KINGBRO_OPTION_CONFIRM_TIMEOUT_SECONDS: int = 30
    KINGBRO_KEEPALIVE_URL: str = ""
    RENDER_EXTERNAL_URL: str = ""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
settings = Settings()
