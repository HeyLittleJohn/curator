"""Configuration management using Pydantic Settings.

All settings can be configured via environment variables or CLI arguments.
"""

from datetime import date
from enum import Enum
from functools import lru_cache
from typing import Optional

from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Schema name for every theta table, partition, and the theta Alembic version table.
THETA_SCHEMA: str = "theta"
ALEMBIC_VERSION_TABLE: str = "alembic_version_theta"

# First session of the Options Standard backfill (daily SPXW expirations).
BACKFILL_START: date = date(2022, 5, 16)

# Fixed window the initial revision partitions. Later months use add_partitions_ahead.
INITIAL_PARTITION_START: date = date(2022, 5, 1)
INITIAL_PARTITION_UNTIL: date = date(2028, 1, 1)

# Calendar DTE 0-5 is 1-minute; anything else on that contract-session is 15-minute.
INTERVAL_1M_SECONDS: int = 60
INTERVAL_15M_SECONDS: int = 900
ONE_MINUTE_MAX_CALENDAR_DTE: int = 5


class BackfillMode(str, Enum):
    """Backfill mode selection."""

    FULL = "full"
    INCREMENTAL = "incremental"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="THETADATA_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ThetaData API settings
    api_base_url: str = Field(
        default="http://localhost:25503/v3",
        description="ThetaData API base URL",
    )
    api_timeout: int = Field(
        default=30,
        description="API request timeout in seconds",
    )
    api_max_retries: int = Field(
        default=3,
        description="Maximum number of API request retries",
    )
    api_rate_limit: int = Field(
        default=4,
        description="Per-process HTTP limit. The account-wide cap is 4 across every worker.",
    )

    # PostgreSQL settings
    database_url: Optional[PostgresDsn] = Field(
        default=None,
        description="PostgreSQL URL for the markets database. Theta tables use THETA_SCHEMA.",
    )
    db_pool_min_size: int = Field(
        default=5,
        description="Minimum database connection pool size per worker",
    )
    db_pool_max_size: int = Field(
        default=20,
        description="Maximum database connection pool size per worker",
    )

    # Worker settings
    worker_count: Optional[int] = Field(
        default=None,
        description="Number of worker processes (defaults to CPU count)",
    )
    batch_size: int = Field(
        default=1000,
        description="Number of records to insert per batch",
    )

    # Backfill settings
    backfill_mode: BackfillMode = Field(
        default=BackfillMode.FULL,
        description="Backfill mode: 'full' or 'incremental'",
    )

    @field_validator("database_url", mode="before")
    @classmethod
    def validate_database_url(cls, v: Optional[str]) -> Optional[str]:
        """Ensure database URL uses asyncpg driver."""
        if v is None:
            return v
        if v.startswith("postgresql://"):
            return v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v

    @property
    def effective_worker_count(self) -> int:
        """Get effective worker count, defaulting to CPU count."""
        import os

        if self.worker_count is not None:
            return self.worker_count
        return os.cpu_count() or 4

    @property
    def asyncpg_dsn(self) -> Optional[str]:
        """Get database URL formatted for asyncpg (without +asyncpg)."""
        if self.database_url is None:
            return None
        url = str(self.database_url)
        return url.replace("postgresql+asyncpg://", "postgresql://")


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
