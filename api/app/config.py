"""API configuration loaded from environment variables via pydantic-settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """API settings from environment variables."""

    database_url: str = "postgresql://postgres:password@localhost:5432/insidertrack"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    port: int = 8000  # Railway sets PORT env var
    api_env: str = "development"
    cors_origins: str = "http://localhost:3000,https://insight-cvm.vercel.app"
    log_level: str = "INFO"

    @property
    def cors_origin_list(self) -> list[str]:
        """Parse comma-separated CORS origins into a list."""
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


_settings: Settings | None = None


def get_settings() -> Settings:
    """Get API settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
