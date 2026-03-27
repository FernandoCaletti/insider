"""Pipeline configuration loaded from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Pipeline settings from environment variables."""

    database_url: str
    cvm_base_url: str
    log_level: str
    pipeline_batch_size: int

    @classmethod
    def from_env(cls) -> "Settings":
        database_url = os.environ.get("DATABASE_URL", "")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")

        return cls(
            database_url=database_url,
            cvm_base_url=os.environ.get(
                "CVM_BASE_URL", "https://dados.cvm.gov.br"
            ),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            pipeline_batch_size=int(
                os.environ.get("PIPELINE_BATCH_SIZE", "100")
            ),
        )


def get_settings() -> Settings:
    """Get pipeline settings singleton."""
    return Settings.from_env()
