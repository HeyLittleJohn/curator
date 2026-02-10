"""Abstract base class for all ThetaData downloaders."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, AsyncIterator, Generic, Optional, Type, TypeVar

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from thetadata_backfill.client import ThetaDataClient
from thetadata_backfill.config import BackfillMode, Settings
from thetadata_backfill.db.repository import Repository
from thetadata_backfill.models.base import Base

SchemaT = TypeVar("SchemaT", bound=BaseModel)
ModelT = TypeVar("ModelT", bound=Base)


@dataclass
class BackfillResult:
    """Result of a backfill operation."""
    
    symbol: str
    data_type: str
    start_date: date
    end_date: date
    records_downloaded: int = 0
    records_inserted: int = 0
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    
    @property
    def success(self) -> bool:
        """Check if backfill was successful."""
        return len(self.errors) == 0


@dataclass
class DownloadTask:
    """Task describing what data to download."""
    
    symbol: str
    data_type: str  # e.g., "ohlc", "trade", "quote", "eod", "greeks"
    start_date: date
    end_date: date
    extra: dict[str, Any] = field(default_factory=dict)  # For options: expiration, strike, right


class BaseDownloader(ABC, Generic[SchemaT, ModelT]):
    """Abstract base class for all data downloaders.
    
    Each downloader handles a specific asset class (equity, option, index)
    and knows how to:
    - Build API endpoints
    - Parse API responses into Pydantic schemas
    - Convert schemas to database records
    - Handle incremental vs full backfill
    """
    
    # Override in subclasses
    data_type: str = ""
    schema_class: Type[SchemaT]
    model_class: Type[ModelT]
    
    def __init__(
        self,
        client: ThetaDataClient,
        settings: Settings,
    ):
        """Initialize downloader.
        
        Args:
            client: ThetaData API client.
            settings: Application settings.
        """
        self.client = client
        self.settings = settings
    
    @abstractmethod
    def get_list_endpoint(self) -> str:
        """Get endpoint for listing available symbols.
        
        Returns:
            API endpoint path.
        """
        ...
    
    @abstractmethod
    def get_dates_endpoint(self, request_type: str = "quote") -> str:
        """Get endpoint for listing available dates.
        
        Args:
            request_type: Type of data request.
            
        Returns:
            API endpoint path.
        """
        ...
    
    @abstractmethod
    def get_history_endpoint(self, request_type: str) -> str:
        """Get endpoint for historical data.
        
        Args:
            request_type: Type of data request (ohlc, trade, quote, eod, etc).
            
        Returns:
            API endpoint path.
        """
        ...
    
    @abstractmethod
    def build_request_params(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Build request parameters for API call.
        
        Args:
            symbol: Symbol to fetch.
            target_date: Date to fetch.
            request_type: Type of data request.
            **kwargs: Additional parameters.
            
        Returns:
            Dictionary of query parameters.
        """
        ...
    
    @abstractmethod
    def parse_response(
        self,
        data: dict[str, Any],
        symbol: str,
    ) -> list[SchemaT]:
        """Parse API response into Pydantic schemas.
        
        Args:
            data: Raw API response.
            symbol: Symbol being fetched (for context).
            
        Returns:
            List of parsed schema objects.
        """
        ...
    
    @abstractmethod
    def schema_to_record(self, schema: SchemaT, symbol: str) -> dict[str, Any]:
        """Convert Pydantic schema to database record.
        
        Args:
            schema: Parsed schema object.
            symbol: Symbol being processed.
            
        Returns:
            Dictionary suitable for database insertion.
        """
        ...
    
    @abstractmethod
    def get_conflict_columns(self) -> list[str]:
        """Get columns that define uniqueness for upsert.
        
        Returns:
            List of column names.
        """
        ...
    
    @abstractmethod
    def get_update_columns(self) -> list[str]:
        """Get columns to update on conflict.
        
        Returns:
            List of column names.
        """
        ...
    
    async def get_available_symbols(self) -> list[str]:
        """Get list of available symbols.
        
        Returns:
            List of symbol strings.
        """
        data = await self.client.get(self.get_list_endpoint())
        return [item["symbol"] for item in data["response"]]
    
    async def get_available_dates(
        self,
        symbol: str,
        request_type: str = "quote",
    ) -> list[date]:
        """Get list of available dates for a symbol.
        
        Args:
            symbol: Symbol to check.
            request_type: Type of data request.
            
        Returns:
            List of available dates.
        """
        data = await self.client.get(
            self.get_dates_endpoint(request_type),
            params={"symbol": symbol},
        )
        return [
            date.fromisoformat(item["date"])
            for item in data["response"]
        ]
    
    async def download_date(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        **kwargs: Any,
    ) -> list[SchemaT]:
        """Download data for a single date.
        
        Args:
            symbol: Symbol to fetch.
            target_date: Date to fetch.
            request_type: Type of data request.
            **kwargs: Additional parameters.
            
        Returns:
            List of parsed schema objects.
        """
        params = self.build_request_params(symbol, target_date, request_type, **kwargs)
        endpoint = self.get_history_endpoint(request_type)
        
        data = await self.client.get(endpoint, params=params)
        return self.parse_response(data, symbol)
    
    async def download_date_range(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        request_type: str,
        **kwargs: Any,
    ) -> AsyncIterator[list[SchemaT]]:
        """Download data for a date range.
        
        Yields batches of data for each day.
        
        Args:
            symbol: Symbol to fetch.
            start_date: Start date (inclusive).
            end_date: End date (inclusive).
            request_type: Type of data request.
            **kwargs: Additional parameters.
            
        Yields:
            List of parsed schema objects for each date.
        """
        # Get available dates in range
        all_dates = await self.get_available_dates(symbol, request_type)
        dates_in_range = [
            d for d in all_dates
            if start_date <= d <= end_date
        ]
        
        for target_date in dates_in_range:
            records = await self.download_date(
                symbol, target_date, request_type, **kwargs
            )
            if records:
                yield records
    
    async def backfill(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        request_type: str,
        session: AsyncSession,
        mode: Optional[BackfillMode] = None,
        **kwargs: Any,
    ) -> BackfillResult:
        """Run backfill for a symbol.
        
        Args:
            symbol: Symbol to backfill.
            start_date: Start date (inclusive).
            end_date: End date (inclusive).
            request_type: Type of data request.
            session: Database session.
            mode: Backfill mode (full or incremental).
            **kwargs: Additional parameters.
            
        Returns:
            Backfill result with statistics.
        """
        import time
        
        if mode is None:
            mode = self.settings.backfill_mode
        
        start_time = time.time()
        result = BackfillResult(
            symbol=symbol,
            data_type=request_type,
            start_date=start_date,
            end_date=end_date,
        )
        
        try:
            repo = Repository(self.model_class, session)
            
            # Get dates to process
            all_dates = await self.get_available_dates(symbol, request_type)
            dates_in_range = [d for d in all_dates if start_date <= d <= end_date]
            
            # For incremental mode, filter out existing dates
            if mode == BackfillMode.INCREMENTAL:
                existing_dates = await repo.get_existing_dates(symbol)
                dates_in_range = [d for d in dates_in_range if d not in existing_dates]
            
            # Download and insert data
            all_records: list[dict[str, Any]] = []
            
            for target_date in dates_in_range:
                try:
                    schemas = await self.download_date(
                        symbol, target_date, request_type, **kwargs
                    )
                    result.records_downloaded += len(schemas)
                    
                    for schema in schemas:
                        record = self.schema_to_record(schema, symbol)
                        all_records.append(record)
                    
                    # Batch insert when we have enough records
                    if len(all_records) >= self.settings.batch_size:
                        inserted = await repo.bulk_upsert(
                            all_records,
                            self.get_conflict_columns(),
                            self.get_update_columns(),
                            batch_size=self.settings.batch_size,
                        )
                        result.records_inserted += inserted
                        all_records = []
                        
                except Exception as e:
                    result.errors.append(f"Error on {target_date}: {str(e)}")
            
            # Insert remaining records
            if all_records:
                inserted = await repo.bulk_upsert(
                    all_records,
                    self.get_conflict_columns(),
                    self.get_update_columns(),
                    batch_size=self.settings.batch_size,
                )
                result.records_inserted += inserted
                
        except Exception as e:
            result.errors.append(f"Backfill error: {str(e)}")
        
        result.duration_seconds = time.time() - start_time
        return result
