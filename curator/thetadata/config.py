"""Configuration management using Pydantic Settings.

All settings can be configured via environment variables or CLI arguments.
"""

from datetime import date
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, PostgresDsn, field_validator, model_validator
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
    api_connect_timeout: float = Field(
        default=10.0,
        description=(
            "Seconds to open a TCP connection to Theta Terminal. "
            "Env: THETADATA_API_CONNECT_TIMEOUT. Independent of the read timeout."
        ),
    )
    api_read_timeout: float = Field(
        default=120.0,
        description=(
            "Seconds to wait for the next bytes from Theta Terminal. "
            "Full-chain history can sit silent for longer than one contract. "
            "A body that keeps arriving is not under a total deadline. "
            "Env: THETADATA_API_READ_TIMEOUT."
        ),
    )
    api_timeout: float | None = Field(
        default=None,
        description=(
            "Legacy read timeout in seconds. Env: THETADATA_API_TIMEOUT. "
            "When set, and api_read_timeout is left at its default, this is the read timeout."
        ),
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

    @field_validator("api_connect_timeout", "api_read_timeout")
    @classmethod
    def timeout_is_positive(cls, value: float) -> float:
        """Reject a zero or negative socket timeout.

        Args:
            value: Connect or read timeout in seconds.

        Returns:
            The same value when it is greater than zero.

        Raises:
            ValueError: The timeout is not positive.
        """
        if value <= 0:
            raise ValueError("timeout must be > 0")
        return value

    @field_validator("api_timeout")
    @classmethod
    def legacy_timeout_is_positive(cls, value: float | None) -> float | None:
        """Reject a zero or negative legacy read timeout.

        Args:
            value: ``api_timeout`` when the caller set it, otherwise ``None``.

        Returns:
            The same value.

        Raises:
            ValueError: The timeout is set and is not positive.
        """
        if value is not None and value <= 0:
            raise ValueError("api_timeout must be > 0")
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

    @model_validator(mode="after")
    def apply_legacy_read_timeout(self) -> "Settings":
        """Treat ``api_timeout`` as the read timeout when the new setting is omitted.

        ``THETADATA_API_READ_TIMEOUT`` wins when both are set. After this runs,
        ``api_timeout`` equals the effective read timeout so older readers of
        that field still see it.

        Returns:
            This settings object. ``api_timeout`` matches the effective read timeout.
        """
        legacy = self.api_timeout
        legacy_set = "api_timeout" in self.model_fields_set and legacy is not None
        read_set = "api_read_timeout" in self.model_fields_set
        if legacy_set and not read_set and legacy is not None:
            self.api_read_timeout = float(legacy)
        self.api_timeout = self.api_read_timeout
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
