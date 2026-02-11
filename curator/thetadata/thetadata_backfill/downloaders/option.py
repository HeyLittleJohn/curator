"""Options data downloader with full Greeks support."""

from datetime import date
from decimal import Decimal
from typing import Any, Optional

from thetadata_backfill.downloaders.base import BaseDownloader
from thetadata_backfill.models.option import (
    OptionGreeks,
    OptionOHLC,
    OptionOpenInterest,
    OptionTradeGreeks,
)
from thetadata_backfill.schemas.option import (
    OptionGreeksResponse,
    OptionOHLCResponse,
    OptionOpenInterestResponse,
    OptionRight,
    OptionTradeGreeksResponse,
)


class OptionOHLCDownloader(BaseDownloader[OptionOHLCResponse, OptionOHLC]):
    """Downloader for option OHLC data."""
    
    data_type = "ohlc"
    schema_class = OptionOHLCResponse
    model_class = OptionOHLC
    
    def get_list_endpoint(self) -> str:
        return "option/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "ohlc") -> str:
        return f"option/list/dates/{request_type}"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return f"option/history/{request_type}"
    
    def build_request_params(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        expiration: Optional[date] = None,
        strike: Optional[float] = None,
        right: Optional[str] = None,
        interval: str = "1m",
        **kwargs: Any,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "date": target_date.strftime("%Y%m%d"),
            "interval": interval,
        }
        if expiration is not None:
            params["expiration"] = expiration.strftime("%Y%m%d")
        else:
            params["expiration"] = "*"  # All expirations
        if strike is not None:
            params["strike"] = str(strike)
        if right is not None:
            params["right"] = right.lower()
        return params
    
    def parse_response(
        self,
        data: dict[str, Any],
        symbol: str,
    ) -> list[OptionOHLCResponse]:
        results = []
        for item in data["response"]:
            # Handle nested contract structure
            if "contract" in item and "data" in item:
                contract = item["contract"]
                for record in item["data"]:
                    results.append(OptionOHLCResponse(
                        symbol=contract["symbol"],
                        expiration=date.fromisoformat(contract["expiration"]),
                        strike=Decimal(str(contract["strike"])),
                        right=OptionRight.from_str(contract["right"]),
                        **record,
                    ))
            else:
                # Flat structure
                results.append(OptionOHLCResponse(**item))
        return results
    
    def schema_to_record(self, schema: OptionOHLCResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": schema.symbol,
            "expiration": schema.expiration,
            "strike": schema.strike,
            "right": schema.right.value,
            "timestamp": schema.timestamp,
            "open": schema.open,
            "high": schema.high,
            "low": schema.low,
            "close": schema.close,
            "volume": schema.volume,
            "count": schema.count,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "expiration", "strike", "right", "timestamp"]
    
    def get_update_columns(self) -> list[str]:
        return ["open", "high", "low", "close", "volume", "count"]


class OptionTradeGreeksDownloader(BaseDownloader[OptionTradeGreeksResponse, OptionTradeGreeks]):
    """Downloader for option trades with full Greeks."""
    
    data_type = "trade_greeks"
    schema_class = OptionTradeGreeksResponse
    model_class = OptionTradeGreeks
    
    def get_list_endpoint(self) -> str:
        return "option/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "trade") -> str:
        return f"option/list/dates/{request_type}"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return "option/history/trade_greeks/all"
    
    def build_request_params(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        expiration: Optional[date] = None,
        strike: Optional[float] = None,
        right: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "date": target_date.strftime("%Y%m%d"),
        }
        if expiration is not None:
            params["expiration"] = expiration.strftime("%Y%m%d")
        else:
            params["expiration"] = "*"
        if strike is not None:
            params["strike"] = str(strike)
        if right is not None:
            params["right"] = right.lower()
        return params
    
    def parse_response(
        self,
        data: dict[str, Any],
        symbol: str,
    ) -> list[OptionTradeGreeksResponse]:
        results = []
        for item in data["response"]:
            if "contract" in item and "data" in item:
                contract = item["contract"]
                for record in item["data"]:
                    results.append(OptionTradeGreeksResponse(
                        symbol=contract["symbol"],
                        expiration=date.fromisoformat(contract["expiration"]),
                        strike=Decimal(str(contract["strike"])),
                        right=OptionRight.from_str(contract["right"]),
                        **record,
                    ))
            else:
                results.append(OptionTradeGreeksResponse(**item))
        return results
    
    def schema_to_record(self, schema: OptionTradeGreeksResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": schema.symbol,
            "expiration": schema.expiration,
            "strike": schema.strike,
            "right": schema.right.value,
            "timestamp": schema.timestamp,
            "sequence": schema.sequence,
            "size": schema.size,
            "price": schema.price,
            "condition": schema.condition,
            "exchange": schema.exchange,
            "underlying_timestamp": schema.underlying_timestamp,
            "underlying_price": schema.underlying_price,
            "implied_vol": schema.implied_vol,
            "iv_error": schema.iv_error,
            # First-order Greeks
            "delta": schema.delta,
            "gamma": schema.gamma,
            "theta": schema.theta,
            "vega": schema.vega,
            "rho": schema.rho,
            "epsilon": schema.epsilon,
            "lambda_val": schema.lambda_,
            # Second-order Greeks
            "vanna": schema.vanna,
            "charm": schema.charm,
            "vomma": schema.vomma,
            "veta": schema.veta,
            "color": schema.color,
            "zomma": schema.zomma,
            "speed": schema.speed,
            "ultima": schema.ultima,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "expiration", "strike", "right", "sequence"]
    
    def get_update_columns(self) -> list[str]:
        return [
            "timestamp", "size", "price", "condition", "underlying_price",
            "implied_vol", "delta", "gamma", "theta", "vega", "rho",
            "vanna", "charm", "vomma", "veta",
        ]


class OptionGreeksDownloader(BaseDownloader[OptionGreeksResponse, OptionGreeks]):
    """Downloader for option Greeks (quote-based)."""
    
    data_type = "greeks"
    schema_class = OptionGreeksResponse
    model_class = OptionGreeks
    
    def get_list_endpoint(self) -> str:
        return "option/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "quote") -> str:
        return f"option/list/dates/{request_type}"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return "option/history/greeks/all"
    
    def build_request_params(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        expiration: Optional[date] = None,
        strike: Optional[float] = None,
        right: Optional[str] = None,
        interval: str = "5m",
        **kwargs: Any,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "date": target_date.strftime("%Y%m%d"),
            "interval": interval,
        }
        if expiration is not None:
            params["expiration"] = expiration.strftime("%Y%m%d")
        else:
            params["expiration"] = "*"
        if strike is not None:
            params["strike"] = str(strike)
        if right is not None:
            params["right"] = right.lower()
        return params
    
    def parse_response(
        self,
        data: dict[str, Any],
        symbol: str,
    ) -> list[OptionGreeksResponse]:
        results = []
        for item in data["response"]:
            if "contract" in item and "data" in item:
                contract = item["contract"]
                for record in item["data"]:
                    results.append(OptionGreeksResponse(
                        symbol=contract["symbol"],
                        expiration=date.fromisoformat(contract["expiration"]),
                        strike=Decimal(str(contract["strike"])),
                        right=OptionRight.from_str(contract["right"]),
                        **record,
                    ))
            else:
                results.append(OptionGreeksResponse(**item))
        return results
    
    def schema_to_record(self, schema: OptionGreeksResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": schema.symbol,
            "expiration": schema.expiration,
            "strike": schema.strike,
            "right": schema.right.value,
            "timestamp": schema.timestamp,
            "underlying_timestamp": schema.underlying_timestamp,
            "bid": schema.bid,
            "ask": schema.ask,
            "underlying_price": schema.underlying_price,
            "implied_vol": schema.implied_vol,
            "iv_error": schema.iv_error,
            # First-order Greeks
            "delta": schema.delta,
            "gamma": schema.gamma,
            "theta": schema.theta,
            "vega": schema.vega,
            "rho": schema.rho,
            "epsilon": schema.epsilon,
            "lambda_val": schema.lambda_,
            # Second-order Greeks
            "vanna": schema.vanna,
            "charm": schema.charm,
            "vomma": schema.vomma,
            "veta": schema.veta,
            "color": schema.color,
            "zomma": schema.zomma,
            "speed": schema.speed,
            "ultima": schema.ultima,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "expiration", "strike", "right", "timestamp"]
    
    def get_update_columns(self) -> list[str]:
        return [
            "bid", "ask", "underlying_price", "implied_vol",
            "delta", "gamma", "theta", "vega", "rho",
            "vanna", "charm", "vomma", "veta",
        ]


class OptionOpenInterestDownloader(BaseDownloader[OptionOpenInterestResponse, OptionOpenInterest]):
    """Downloader for option open interest data."""
    
    data_type = "open_interest"
    schema_class = OptionOpenInterestResponse
    model_class = OptionOpenInterest
    
    def get_list_endpoint(self) -> str:
        return "option/list/symbols"
    
    def get_dates_endpoint(self, request_type: str = "open_interest") -> str:
        return "option/list/dates/open_interest"
    
    def get_history_endpoint(self, request_type: str) -> str:
        return "option/history/open_interest"
    
    def build_request_params(
        self,
        symbol: str,
        target_date: date,
        request_type: str,
        expiration: Optional[date] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "date": target_date.strftime("%Y%m%d"),
        }
        if expiration is not None:
            params["expiration"] = expiration.strftime("%Y%m%d")
        else:
            params["expiration"] = "*"
        return params
    
    def parse_response(
        self,
        data: dict[str, Any],
        symbol: str,
    ) -> list[OptionOpenInterestResponse]:
        results = []
        for item in data["response"]:
            if "contract" in item and "data" in item:
                contract = item["contract"]
                for record in item["data"]:
                    results.append(OptionOpenInterestResponse(
                        symbol=contract["symbol"],
                        expiration=date.fromisoformat(contract["expiration"]),
                        strike=Decimal(str(contract["strike"])),
                        right=OptionRight.from_str(contract["right"]),
                        **record,
                    ))
            else:
                results.append(OptionOpenInterestResponse(**item))
        return results
    
    def schema_to_record(self, schema: OptionOpenInterestResponse, symbol: str) -> dict[str, Any]:
        return {
            "symbol": schema.symbol,
            "expiration": schema.expiration,
            "strike": schema.strike,
            "right": schema.right.value,
            "trade_date": schema.date,
            "open_interest": schema.open_interest,
        }
    
    def get_conflict_columns(self) -> list[str]:
        return ["symbol", "expiration", "strike", "right", "trade_date"]
    
    def get_update_columns(self) -> list[str]:
        return ["open_interest"]
