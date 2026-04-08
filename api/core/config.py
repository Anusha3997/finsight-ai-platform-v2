from pydantic_settings import BaseSettings, SettingsConfigDict
 
 
class Settings(BaseSettings):
    # ── Database ──────────────────────────────────────────
    DATABASE_URL: str = (
        "postgresql://finsight:finsight_pass@localhost:5432/finsight_db"
    )
 
    # ── App ───────────────────────────────────────────────
    ENVIRONMENT: str = "development"
    APP_TITLE:   str = "Finsight API"
    APP_VERSION: str = "0.1.0"
 
    # ── CORS (for the UI later) ───────────────────────────
    # Comma-separated list of allowed origins
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:5173"]
 
    model_config = SettingsConfigDict(
        env_file=".env",          # load from .env if present
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",           # ignore unknown env vars — don't crash
    )
 
 
# Single instance — import this everywhere
settings = Settings()