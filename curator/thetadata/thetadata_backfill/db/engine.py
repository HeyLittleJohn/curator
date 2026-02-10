"""Async database engine and session management."""

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from thetadata_backfill.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    """Create async SQLAlchemy engine from settings.
    
    Args:
        settings: Application settings with database URL.
        
    Returns:
        Configured async engine.
        
    Raises:
        ValueError: If database URL is not configured.
    """
    if settings.database_url is None:
        raise ValueError(
            "Database URL not configured. Set THETADATA_DATABASE_URL environment variable "
            "or pass --database-url to CLI."
        )
    
    return create_async_engine(
        str(settings.database_url),
        pool_size=settings.db_pool_min_size,
        max_overflow=settings.db_pool_max_size - settings.db_pool_min_size,
        pool_pre_ping=True,
        echo=False,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create async session factory.
    
    Args:
        engine: Async SQLAlchemy engine.
        
    Returns:
        Session factory for creating database sessions.
    """
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


class DatabaseManager:
    """Manages database connections and sessions."""
    
    def __init__(self, settings: Settings):
        """Initialize database manager.
        
        Args:
            settings: Application settings.
        """
        self.settings = settings
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker[AsyncSession]] = None
    
    async def initialize(self) -> None:
        """Initialize database connections."""
        self._engine = create_engine(self.settings)
        self._session_factory = create_session_factory(self._engine)
    
    async def close(self) -> None:
        """Close database connections."""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
    
    @property
    def engine(self) -> AsyncEngine:
        """Get the database engine."""
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._engine
    
    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Get the session factory."""
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._session_factory
    
    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Create a database session context.
        
        Yields:
            Database session that auto-commits on success.
        """
        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
