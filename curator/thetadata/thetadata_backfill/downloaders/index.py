"""Index data downloader."""

from datetime import date
from typing import Any, Optional, Type

from thetadata_backfill.client import ThetaDataClient
from thetadata_backfill.config import Settings
from thetadata_backfill.downloaders.base import BaseDownloader
from thetadata_backfill.models.index import IndexOHLC, IndexPrice, IndexEOD
from thetadata_backfill.schemas.index import (
    IndexOHLCResponse,
    IndexPriceResponse,
    IndexEODResponse,
)


class IndexOHLCDownloader(BaseDownloader[IndexOHLCResponse, IndexOHLC]):
    """Downloader for index OHLC data."""
    
    data_type = "ohlc"
    schema_class = IndexOHLCResponse
    model_class = IndexOHLC
    
    def get_list_endpoint(self) -> str:
        return "index/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "price") -> str:
        return "index/list/dates"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return f"index/history/{request_type}"
    
    def build_request_params(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        interval: str = "1m",
        **kwargs: Any,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "date": target_date.strftime("%Y%m%d"),
            "interval": interval,
        }
    
    def parse_response(
        self,
        data: dict[str, Any],
        symbol: str,
    ) -> list[IndexOHLCResponse]:
        return [
            IndexOHLCResponse(**{**item, "symbol": item.get("symbol", symbol)})
            for item in data["response"]
        ]
    
    def schema_to_record(self, schema: IndexOHLCResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol if schema.symbol is None else schema.symbol,
            "timestamp": schema.timestamp,
            "open": schema.open,
            "high": schema.high,
            "low": schema.low,
            "close": schema.close,
            "volume": schema.volume,
            "count": schema.count,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "timestamp"]
    
    def get_update_columns(self) -> list[str]:
        return ["open", "high", "low", "close", "volume", "count"]


class IndexPriceDownloader(BaseDownloader[IndexPriceResponse, IndexPrice]):
    """Downloader for index price tick data."""
    
    data_type = "price"
    schema_class = IndexPriceResponse
    model_class = IndexPrice
    
    def get_list_endpoint(self) -> str:
        return "index/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "price") -> str:
        return "index/list/dates"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return "index/history/price"
    
    def build_request_params(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "date": target_date.strftime("%Y%m%d"),
        }
    
    def parse_response(
        self,
        data: dict[str, Any],
        symbol: str,
    ) -> list[IndexPriceResponse]:
        return [
            IndexPriceResponse(**{**item, "symbol": item.get("symbol", symbol)})
            for item in data["response"]
        ]
    
    def schema_to_record(self, schema: IndexPriceResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol if schema.symbol is None else schema.symbol,
            "timestamp": schema.timestamp,
            "price": schema.price,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "timestamp"]
    
    def get_update_columns(self) -> list[str]:
        return ["price"]


class IndexEODDownloader(BaseDownloader[IndexEODResponse, IndexEOD]):
    """Downloader for index end-of-day data."""
    
    data_type = "eod"
    schema_class = IndexEODResponse
    model_class = IndexEOD
    
    def get_list_endpoint(self) -> str:
        return "index/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "eod") -> str:
        return "index/list/dates"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return "index/history/eod"
    
    def build_request_params(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "start_date": target_date.strftime("%Y%m%d"),
            "end_date": target_date.strftime("%Y%m%d"),
        }
    
    def parse_response(
        self,
        data: dict[str, Any],
        symbol: str,
    ) -> list[IndexEODResponse]:
        return [
            IndexEODResponse(**{**item, "symbol": item.get("symbol", symbol)})
            for item in data["response"]
        ]
    
    def schema_to_record(self, schema: IndexEODResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol if schema.symbol is None else schema.symbol,
            "trade_date": schema.created.date(),
            "report_created": schema.created,
            "last_trade": schema.last_trade,
            "open": schema.open,
            "high": schema.high,
            "low": schema.low,
            "close": schema.close,
            "volume": schema.volume,
            "count": schema.count,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "trade_date"]
    
    def get_update_columns(self) -> list[str]:
        return ["open", "high", "low", "close", "volume", "count"]
