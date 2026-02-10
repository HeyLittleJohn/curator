"""Generic async repository for bulk database operations."""

from datetime import date
from typing import Any, Generic, Sequence, Type, TypeVar

from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from thetadata_backfill.models.base import Base

ModelT = TypeVar("ModelT", bound=Base)


class Repository(Generic[ModelT]):
    """Generic repository for bulk database operations."""
    
    def __init__(self, model: Type[ModelT], session: AsyncSession):
        """Initialize repository.
        
        Args:
            model: SQLAlchemy model class.
            session: Async database session.
        """
        self.model = model
        self.session = session
    
    async def bulk_insert(
        self,
        records: list[dict[str, Any]],
        batch_size: int = 1000,
    ) -> int:
        """Bulk insert records.
        
        Args:
            records: List of record dictionaries.
            batch_size: Number of records per batch.
            
        Returns:
            Number of records inserted.
        """
        if not records:
            return 0
        
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            stmt = insert(self.model).values(batch)
            await self.session.execute(stmt)
            total += len(batch)
        
        return total
    
    async def bulk_upsert(
        self,
        records: list[dict[str, Any]],
        conflict_columns: list[str],
        update_columns: list[str],
        batch_size: int = 1000,
    ) -> int:
        """Bulk upsert records (insert or update on conflict).
        
        Args:
            records: List of record dictionaries.
            conflict_columns: Columns that define uniqueness.
            update_columns: Columns to update on conflict.
            batch_size: Number of records per batch.
            
        Returns:
            Number of records processed.
        """
        if not records:
            return 0
        
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            stmt = insert(self.model).values(batch)
            
            if update_columns:
                update_dict = {col: stmt.excluded[col] for col in update_columns}
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_columns,
                    set_=update_dict,
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
            
            await self.session.execute(stmt)
            total += len(batch)
        
        return total
    
    async def get_existing_dates(
        self,
        symbol: str,
        date_column: str = "trade_date",
    ) -> set[date]:
        """Get set of dates that already exist for a symbol.
        
        Args:
            symbol: Symbol to check.
            date_column: Name of the date column.
            
        Returns:
            Set of dates that have data.
        """
        date_col = getattr(self.model, date_column)
        symbol_col = getattr(self.model, "symbol")
        
        stmt = (
            select(date_col)
            .where(symbol_col == symbol)
            .distinct()
        )
        result = await self.session.execute(stmt)
        return {row[0] for row in result.fetchall()}
    
    async def get_existing_timestamps(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timestamp_column: str = "timestamp",
    ) -> set:
        """Get set of timestamps that already exist for a symbol and date range.
        
        Args:
            symbol: Symbol to check.
            start_date: Start date.
            end_date: End date.
            timestamp_column: Name of the timestamp column.
            
        Returns:
            Set of timestamps that have data.
        """
        ts_col = getattr(self.model, timestamp_column)
        symbol_col = getattr(self.model, "symbol")
        
        stmt = (
            select(ts_col)
            .where(
                and_(
                    symbol_col == symbol,
                    ts_col >= start_date,
                    ts_col <= end_date,
                )
            )
            .distinct()
        )
        result = await self.session.execute(stmt)
        return {row[0] for row in result.fetchall()}
    
    async def count(self, symbol: str | None = None) -> int:
        """Count records, optionally filtered by symbol.
        
        Args:
            symbol: Optional symbol filter.
            
        Returns:
            Number of records.
        """
        from sqlalchemy import func
        
        stmt = select(func.count()).select_from(self.model)
        if symbol is not None:
            symbol_col = getattr(self.model, "symbol")
            stmt = stmt.where(symbol_col == symbol)
        
        result = await self.session.execute(stmt)
        return result.scalar() or 0
