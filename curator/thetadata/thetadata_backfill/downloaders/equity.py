"""Equity data downloader."""

from datetime import date
from typing import Any

from thetadata_backfill.downloaders.base import BaseDownloader
from thetadata_backfill.models.equity import EquityEOD, EquityOHLC, EquityQuote, EquityTrade
from thetadata_backfill.schemas.equity import (
    EquityEODResponse,
    EquityOHLCResponse,
    EquityQuoteResponse,
    EquityTradeResponse,
)


class EquityOHLCDownloader(BaseDownloader[EquityOHLCResponse, EquityOHLC]):
    """Downloader for equity OHLC data."""
    
    data_type = "ohlc"
    schema_class = EquityOHLCResponse
    model_class = EquityOHLC
    
    def get_list_endpoint(self) -> str:
        return "stock/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "ohlc") -> str:
        return f"stock/list/dates/{request_type}"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return f"stock/history/{request_type}"
    
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
    ) -> list[EquityOHLCResponse]:
        return [
            EquityOHLCResponse(**{**item, "symbol": item.get("symbol", symbol)})
            for item in data["response"]
        ]
    
    def schema_to_record(self, schema: EquityOHLCResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": schema.symbol,
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


class EquityTradeDownloader(BaseDownloader[EquityTradeResponse, EquityTrade]):
    """Downloader for equity trade data."""
    
    data_type = "trade"
    schema_class = EquityTradeResponse
    model_class = EquityTrade
    
    def get_list_endpoint(self) -> str:
        return "stock/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "trade") -> str:
        return f"stock/list/dates/{request_type}"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return f"stock/history/{request_type}"
    
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
    ) -> list[EquityTradeResponse]:
        return [
            EquityTradeResponse(**{**item, "symbol": item.get("symbol", symbol)})
            for item in data["response"]
        ]
    
    def schema_to_record(self, schema: EquityTradeResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol if schema.symbol is None else schema.symbol,
            "timestamp": schema.timestamp,
            "sequence": schema.sequence,
            "size": schema.size,
            "price": schema.price,
            "condition": schema.condition,
            "exchange": schema.exchange,
            "ext_condition1": schema.ext_condition1,
            "ext_condition2": schema.ext_condition2,
            "ext_condition3": schema.ext_condition3,
            "ext_condition4": schema.ext_condition4,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "sequence"]
    
    def get_update_columns(self) -> list[str]:
        return ["timestamp", "size", "price", "condition"]


class EquityQuoteDownloader(BaseDownloader[EquityQuoteResponse, EquityQuote]):
    """Downloader for equity quote (NBBO) data."""
    
    data_type = "quote"
    schema_class = EquityQuoteResponse
    model_class = EquityQuote
    
    def get_list_endpoint(self) -> str:
        return "stock/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "quote") -> str:
        return f"stock/list/dates/{request_type}"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return f"stock/history/{request_type}"
    
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
    ) -> list[EquityQuoteResponse]:
        return [
            EquityQuoteResponse(**{**item, "symbol": item.get("symbol", symbol)})
            for item in data["response"]
        ]
    
    def schema_to_record(self, schema: EquityQuoteResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol if schema.symbol is None else schema.symbol,
            "timestamp": schema.timestamp,
            "bid": schema.bid,
            "bid_size": schema.bid_size,
            "bid_exchange": schema.bid_exchange,
            "bid_condition": schema.bid_condition,
            "ask": schema.ask,
            "ask_size": schema.ask_size,
            "ask_exchange": schema.ask_exchange,
            "ask_condition": schema.ask_condition,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "timestamp"]
    
    def get_update_columns(self) -> list[str]:
        return ["bid", "bid_size", "ask", "ask_size"]


class EquityEODDownloader(BaseDownloader[EquityEODResponse, EquityEOD]):
    """Downloader for equity end-of-day data."""
    
    data_type = "eod"
    schema_class = EquityEODResponse
    model_class = EquityEOD
    
    def get_list_endpoint(self) -> str:
        return "stock/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "eod") -> str:
        return f"stock/list/dates/{request_type}"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return "stock/history/eod"
    
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
    ) -> list[EquityEODResponse]:
        return [
            EquityEODResponse(**{**item, "symbol": item.get("symbol", symbol)})
            for item in data["response"]
        ]
    
    def schema_to_record(self, schema: EquityEODResponse, symbol: str) -> dict[str, Any]:
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
            "bid": schema.bid,
            "bid_size": schema.bid_size,
            "ask": schema.ask,
            "ask_size": schema.ask_size,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "trade_date"]
    
    def get_update_columns(self) -> list[str]:
        return ["open", "high", "low", "close", "volume", "count"]
