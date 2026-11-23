"""Configuration management using Pydantic Settings.

All settings can be configured via environment variables or CLI arguments.
"""

from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, PostgresDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ThetaData Options Standard allows 4 concurrent requests for the whole account.
OPTIONS_STANDARD_CONCURRENCY = 4


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

    # ThetaData API settings. Stick to one host: the terminal rejects a mix of
    # 127.0.0.1 and localhost with 476 WRONG_IP.
    api_base_url: str = Field(
        default="http://127.0.0.1:25503/v3",
        description="ThetaData API base URL",
    )
    api_timeout: int = Field(
        default=30,
        description="Socket connect and read timeout in seconds. Streamed bodies are not under a total deadline.",
    )
    api_max_retries: int = Field(
        default=3,
        description="Retries after the first attempt, for transient errors only",
    )
    api_backoff_base: float = Field(
        default=0.25,
        description="Initial exponential backoff ceiling in seconds",
    )
    api_backoff_cap: float = Field(
        default=8.0,
        description="Maximum exponential backoff ceiling in seconds",
    )
    api_rate_limit: int = Field(
        default=OPTIONS_STANDARD_CONCURRENCY,
        description="Account-wide in-flight request cap, shared by every worker. Configurable down, never above 4.",
    )
    lock_path: Path = Field(
        default=Path("/tmp/curator-theta.lock"),
        description="Per-machine lock. A second theta backfill or nightly refuses to start while it is held.",
    )

    # PostgreSQL settings
    database_url: Optional[PostgresDsn] = Field(
        default=None,
        description="PostgreSQL connection string (e.g., postgresql+asyncpg://user:pass@host:5432/db)",
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

    @field_validator("api_rate_limit")
    @classmethod
    def cap_account_concurrency(cls, value: int) -> int:
        """Keep the request cap inside the Options Standard account limit.

        Args:
            value: Configured in-flight request cap.

        Returns:
            The same value when it is between 1 and 4.

        Raises:
            ValueError: The cap is below 1 or above 4.
        """
        if value < 1:
            raise ValueError("api_rate_limit must be at least 1")
        if value > OPTIONS_STANDARD_CONCURRENCY:
            raise ValueError(
                f"api_rate_limit cannot exceed {OPTIONS_STANDARD_CONCURRENCY}, the Options Standard account cap"
            )
        return value

    @field_validator("api_max_retries")
    @classmethod
    def retries_are_bounded(cls, value: int) -> int:
        """Reject a negative retry count.

        Args:
            value: Retries after the first attempt.

        Returns:
            The same value when it is zero or greater.

        Raises:
            ValueError: The count is negative.
        """
        if value < 0:
            raise ValueError("api_max_retries must be >= 0")
        return value

    @model_validator(mode="after")
    def backoff_cap_covers_base(self) -> "Settings":
        """Require the backoff ceiling to sit at or above the base delay.

        Returns:
            This settings object.

        Raises:
            ValueError: ``api_backoff_cap`` is below ``api_backoff_base``.
        """
        if self.api_backoff_base < 0 or self.api_backoff_cap < 0:
            raise ValueError("backoff timings must be >= 0")
        if self.api_backoff_cap < self.api_backoff_base:
            raise ValueError("api_backoff_cap must be >= api_backoff_base")
        return self

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
