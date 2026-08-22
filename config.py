
from pydantic_settings import BaseSettings, SettingsConfigDict
class Settings(BaseSettings):
    FRONTEND_URL: str = "http://localhost:5173"
    KOTAK_CONSUMER_KEY: str = ""
    KOTAK_MOBILE_NUMBER: str = ""
    KOTAK_UCC: str = ""
    KOTAK_MPIN: str = ""
    KOTAK_ENVIRONMENT: str = "prod"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
settings = Settings()
