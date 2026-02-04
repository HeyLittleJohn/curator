"""Async HTTP client for ThetaData API with rate limiting and retries."""

import asyncio
import logging
from datetime import date
from typing import Any, AsyncIterator, Optional, TypeVar

import aiohttp
from pydantic import BaseModel

from thetadata_backfill.config import Settings

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class ThetaDataClientError(Exception):
    """Base exception for ThetaData client errors."""
    pass


class ThetaDataRateLimitError(ThetaDataClientError):
    """Raised when rate limited by the API."""
    pass


class ThetaDataClient:
    """Async HTTP client for ThetaData API.
    
    Features:
    - Connection pooling via aiohttp
    - Rate limiting with semaphore
    - Automatic retries with exponential backoff
    - NDJSON streaming for large responses
    """
    
    def __init__(self, settings: Settings):
        """Initialize client.
        
        Args:
            settings: Application settings.
        """
        self.settings = settings
        self.base_url = settings.api_base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(settings.api_rate_limit)
    
    async def __aenter__(self) -> "ThetaDataClient":
        """Enter async context."""
        await self.start()
        return self
    
    async def __aexit__(self, *args: Any) -> None:
        """Exit async context."""
        await self.close()
    
    async def start(self) -> None:
        """Start the client session."""
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.settings.api_timeout)
            connector = aiohttp.TCPConnector(limit=self.settings.api_rate_limit * 2)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
            )
    
    async def close(self) -> None:
        """Close the client session."""
        if self._session is not None:
            await self._session.close()
            self._session = None
    
    @property
    def session(self) -> aiohttp.ClientSession:
        """Get the session, raising if not started."""
        if self._session is None:
            raise RuntimeError("Client not started. Call start() or use async context manager.")
        return self._session
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        retries: Optional[int] = None,
    ) -> dict[str, Any]:
        """Make an HTTP request with rate limiting and retries.
        
        Args:
            method: HTTP method.
            endpoint: API endpoint (will be appended to base_url).
            params: Query parameters.
            retries: Number of retries (defaults to settings).
            
        Returns:
            Parsed JSON response.
        """
        if retries is None:
            retries = self.settings.api_max_retries
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Add format parameter to get JSON
        if params is None:
            params = {}
        params.setdefault("format", "json")
        
        last_error: Optional[Exception] = None
        
        for attempt in range(retries + 1):
            async with self._semaphore:
                try:
                    async with self.session.request(method, url, params=params) as response:
                        if response.status == 429:
                            # Rate limited - wait and retry
                            retry_after = int(response.headers.get("Retry-After", 1))
                            logger.warning(f"Rate limited, waiting {retry_after}s")
                            await asyncio.sleep(retry_after)
                            continue
                        
                        response.raise_for_status()
                        data = await response.json()
                        
                        # ThetaData wraps responses in a "response" key
                        if isinstance(data, dict) and "response" in data:
                            return {"response": data["response"]}
                        return {"response": data}
                        
                except aiohttp.ClientResponseError as e:
                    last_error = e
                    if e.status >= 500:
                        # Server error - retry with backoff
                        wait_time = 2 ** attempt
                        logger.warning(f"Server error {e.status}, retrying in {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue
                    raise ThetaDataClientError(f"API error: {e}")
                except asyncio.TimeoutError:
                    last_error = asyncio.TimeoutError()
                    wait_time = 2 ** attempt
                    logger.warning(f"Request timeout, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
        
        raise ThetaDataClientError(f"Max retries exceeded: {last_error}")
    
    async def get(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Make a GET request.
        
        Args:
            endpoint: API endpoint.
            params: Query parameters.
            
        Returns:
            Parsed JSON response.
        """
        return await self._request("GET", endpoint, params)
    
    async def stream_ndjson(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream NDJSON response line by line.
        
        Args:
            endpoint: API endpoint.
            params: Query parameters.
            
        Yields:
            Parsed JSON objects from each line.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        if params is None:
            params = {}
        params["format"] = "ndjson"
        
        async with self._semaphore:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                
                async for line in response.content:
                    line = line.decode("utf-8").strip()
                    if line:
                        import json
                        yield json.loads(line)
    
    # Convenience methods for common endpoints
    
    async def get_stock_symbols(self) -> list[str]:
        """Get list of all stock symbols."""
        data = await self.get("stock/list/symbols")
        return [item["symbol"] for item in data["response"]]
    
    async def get_stock_dates(
        self,
        symbol: str,
        request_type: str = "quote",
    ) -> list[date]:
        """Get available dates for a stock symbol.
        
        Args:
            symbol: Stock symbol.
            request_type: Type of data (quote, trade, ohlc, eod).
            
        Returns:
            List of available dates.
        """
        data = await self.get(
            f"stock/list/dates/{request_type}",
            params={"symbol": symbol},
        )
        return [
            date.fromisoformat(item["date"])
            for item in data["response"]
        ]
    
    async def get_option_expirations(self, symbol: str) -> list[date]:
        """Get available expirations for an option symbol."""
        data = await self.get(
            "option/list/expirations",
            params={"symbol": symbol},
        )
        return [
            date.fromisoformat(item["expiration"])
            for item in data["response"]
        ]
    
    async def get_option_strikes(
        self,
        symbol: str,
        expiration: date,
    ) -> list[float]:
        """Get available strikes for an option.
        
        Args:
            symbol: Underlying symbol.
            expiration: Expiration date.
            
        Returns:
            List of strike prices.
        """
        data = await self.get(
            "option/list/strikes",
            params={
                "symbol": symbol,
                "expiration": expiration.strftime("%Y%m%d"),
            },
        )
        return [float(item["strike"]) for item in data["response"]]
    
    async def get_index_symbols(self) -> list[str]:
        """Get list of all index symbols."""
        data = await self.get("index/list/symbols")
        return [item["symbol"] for item in data["response"]]
    
    async def get_index_dates(self, symbol: str) -> list[date]:
        """Get available dates for an index symbol."""
        data = await self.get(
            "index/list/dates",
            params={"symbol": symbol},
        )
        return [
            date.fromisoformat(item["date"])
            for item in data["response"]
        ]
