"""Async ThetaData v3 client for the Options Standard plan.

History and list calls stream CSV or NDJSON. One spawn-context semaphore,
shared with every aiomultiprocess worker, caps in-flight requests. The client
retries rate limits, disconnects, and server errors with bounded jittered
backoff, and it releases the slot while it waits.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from datetime import date
from decimal import Decimal
from typing import Any, Literal, TypeVar

import aiohttp
from pydantic import BaseModel, ValidationError

from curator.thetadata.client.endpoints import require_standard_endpoint, validate_chain_request
from curator.thetadata.client.errors import (
    ThetaDataClientError,
    ThetaDataNoDataError,
    ThetaDataRateLimitError,
    ThetaDataRequestError,
    ThetaDataSubscriptionError,
    ThetaDataTerminalUnreachableError,
    sanitize_text,
)
from curator.thetadata.client.failure import tag_failure
from curator.thetadata.client.retry import backoff_delay
from curator.thetadata.client.rows import (
    ContractRow,
    EodRow,
    ExpirationRow,
    GreeksRow,
    OhlcRow,
    OpenInterestRow,
    QuoteRow,
    StrikeRow,
)
from curator.thetadata.client.stream import iter_response_records, iter_text_lines
from curator.thetadata.config import Settings
from curator.thetadata.limits import account_request_slot

logger = logging.getLogger(__name__)

ResponseFormat = Literal["csv", "ndjson"]
RowT = TypeVar("RowT", bound=BaseModel)

_HISTORY_PREFIX = "option/history/"
_TRANSIENT_STATUSES = frozenset({429, 474, 500, 502, 503, 504, 571})
_STATUS_NAMES = {
    404: "404 NO_IMPL",
    429: "429 OS_LIMIT",
    470: "470 GENERAL",
    471: "471 PERMISSION",
    472: "472 NO_DATA",
    473: "473 INVALID_PARAMS",
    474: "474 DISCONNECTED",
    475: "475 TERMINAL_PARSE",
    476: "476 WRONG_IP",
    477: "477 NO_PAGE_FOUND",
    478: "478 INVALID_SESSION_ID",
    571: "571 SERVER_STARTING",
    572: "572 UNCAUGHT_ERROR",
}


class ThetaDataClient:
    """HTTP client for Theta Terminal v3 at a configurable base URL.

    The default base URL is ``http://127.0.0.1:25503/v3``. Use one host for a
    terminal process. Mixing ``127.0.0.1`` and ``localhost`` makes the terminal
    return 476 WRONG_IP.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        uniform: Callable[[float, float], float] | None = None,
    ) -> None:
        """Store settings. The session starts in ``start`` or ``async with``.

        Args:
            settings: Application settings. ``api_rate_limit`` is the account cap.
                ``api_connect_timeout`` and ``api_read_timeout`` are the socket timeouts.
            sleep: Awaitable delay used between retries. Tests pass a recorder.
                Production waits with ``asyncio.sleep``.
            uniform: Jitter source for ``backoff_delay``. Tests pass a stub.
                Production uses ``random.uniform``.
        """
        self.settings = settings
        self.base_url = settings.api_base_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None
        self._sleep = sleep or asyncio.sleep
        self._uniform = uniform

    async def __aenter__(self) -> ThetaDataClient:
        """Open the HTTP session.

        Returns:
            This client.
        """
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Close the HTTP session."""
        await self.close()

    async def start(self) -> None:
        """Open the aiohttp session.

        The connector limit matches the account cap for this process. The
        cross-process cap is the shared semaphore, not the connector. Connect
        and read use separate timeouts, and a streamed body has no total deadline.
        """
        if self._session is not None:
            return
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=self.settings.api_connect_timeout,
            sock_connect=self.settings.api_connect_timeout,
            sock_read=self.settings.api_read_timeout,
        )
        connector = aiohttp.TCPConnector(limit=self.settings.api_rate_limit)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            trust_env=False,
        )

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session is None:
            return
        await self._session.close()
        self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        """Return the open session.

        Returns:
            The aiohttp session.

        Raises:
            RuntimeError: ``start`` has not been called.
        """
        if self._session is None:
            raise RuntimeError("Client not started. Call start() or use async context manager.")
        return self._session

    async def get(
        self,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """GET a small JSON list response.

        History routes are refused here so a chain cannot be buffered whole.
        Use the ``iter_*`` methods for quote, OHLC, open interest, EOD, and greeks.

        Args:
            endpoint: Path under the v3 base URL.
            params: Query parameters. ``format=json`` is set by this method.

        Returns:
            ``{"response": ...}`` matching the terminal's JSON envelope.

        Raises:
            ThetaDataRequestError: ``endpoint`` is a history chain.
            ThetaDataSubscriptionError: ``endpoint`` is outside Options Standard.
        """
        path = require_standard_endpoint(endpoint)
        if path.startswith(_HISTORY_PREFIX):
            raise ThetaDataRequestError(
                f"{path} returns a chain. Stream it with the matching iter_* method instead of get()."
            )
        query = _query(params, "json")
        validate_chain_request(path, query)
        body = await self._read_full(path, query)
        return _json_envelope(body)

    async def list_expirations(
        self,
        symbol: str,
        *,
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[ExpirationRow]:
        """Stream expirations for one underlying.

        Args:
            symbol: Underlying symbol, such as ``SPXW``.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One expiration row.
        """
        async for row in self._iter_models(
            "option/list/expirations",
            {"symbol": symbol},
            ExpirationRow,
            response_format,
        ):
            yield row

    async def list_strikes(
        self,
        symbol: str,
        expiration: date | str,
        *,
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[StrikeRow]:
        """Stream strikes for one underlying and expiration.

        Args:
            symbol: Underlying symbol.
            expiration: Expiration date.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One strike row.
        """
        async for row in self._iter_models(
            "option/list/strikes",
            {"symbol": symbol, "expiration": _format_expiration(expiration)},
            StrikeRow,
            response_format,
        ):
            yield row

    async def list_contracts(
        self,
        session_date: date,
        symbol: str | None = None,
        *,
        request_type: Literal["quote", "trade"] = "quote",
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[ContractRow]:
        """Stream contracts quoted or traded on one session.

        Args:
            session_date: Session to list.
            symbol: Optional underlying filter.
            request_type: ``quote`` or ``trade``.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One contract row.

        Raises:
            ThetaDataRequestError: ``request_type`` is not ``quote`` or ``trade``.
        """
        if request_type not in {"quote", "trade"}:
            raise ThetaDataRequestError("contracts request_type must be 'quote' or 'trade'.")
        params: dict[str, str] = {"date": _format_day(session_date)}
        if symbol is not None:
            params["symbol"] = symbol
        async for row in self._iter_models(
            f"option/list/contracts/{request_type}",
            params,
            ContractRow,
            response_format,
        ):
            yield row

    async def iter_quotes(
        self,
        symbol: str,
        expiration: date | str,
        *,
        session_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        strike: Decimal | float | str | None = None,
        right: str | None = None,
        interval: str = "1m",
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[QuoteRow]:
        """Stream NBBO quotes. The default interval is 1 minute.

        Args:
            symbol: Underlying symbol.
            expiration: Expiration, or ``*`` for every expiration on one day.
            session_date: Single session. Overrides a start/end range.
            start_date: Range start, inclusive.
            end_date: Range end, inclusive.
            strike: One strike, or omitted for every strike.
            right: ``call``, ``put``, or ``both``.
            interval: Bar size. ``expiration=*`` requires ``1m`` or greater.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One quote row.
        """
        async for row in self._iter_models(
            "option/history/quote",
            _history_params(
                symbol,
                expiration,
                session_date=session_date,
                start_date=start_date,
                end_date=end_date,
                strike=strike,
                right=right,
                interval=interval,
            ),
            QuoteRow,
            response_format,
        ):
            yield row

    async def iter_ohlc(
        self,
        symbol: str,
        expiration: date | str,
        *,
        session_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        strike: Decimal | float | str | None = None,
        right: str | None = None,
        interval: str = "1m",
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[OhlcRow]:
        """Stream OHLC bars. The default interval is 1 minute.

        Args:
            symbol: Underlying symbol.
            expiration: Expiration, or ``*`` for every expiration on one day.
            session_date: Single session. Overrides a start/end range.
            start_date: Range start, inclusive.
            end_date: Range end, inclusive.
            strike: One strike, or omitted for every strike.
            right: ``call``, ``put``, or ``both``.
            interval: Bar size. ``expiration=*`` requires ``1m`` or greater.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One OHLC row.
        """
        async for row in self._iter_models(
            "option/history/ohlc",
            _history_params(
                symbol,
                expiration,
                session_date=session_date,
                start_date=start_date,
                end_date=end_date,
                strike=strike,
                right=right,
                interval=interval,
            ),
            OhlcRow,
            response_format,
        ):
            yield row

    async def iter_open_interest(
        self,
        symbol: str,
        expiration: date | str,
        *,
        session_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        strike: Decimal | float | str | None = None,
        right: str | None = None,
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[OpenInterestRow]:
        """Stream open interest.

        Args:
            symbol: Underlying symbol.
            expiration: Expiration, or ``*`` for every expiration on one day.
            session_date: Single session.
            start_date: Range start, inclusive.
            end_date: Range end, inclusive.
            strike: One strike, or omitted for every strike.
            right: ``call``, ``put``, or ``both``.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One open-interest row.
        """
        async for row in self._iter_models(
            "option/history/open_interest",
            _history_params(
                symbol,
                expiration,
                session_date=session_date,
                start_date=start_date,
                end_date=end_date,
                strike=strike,
                right=right,
                interval=None,
            ),
            OpenInterestRow,
            response_format,
        ):
            yield row

    async def iter_eod(
        self,
        symbol: str,
        expiration: date | str,
        *,
        start_date: date,
        end_date: date,
        strike: Decimal | float | str | None = None,
        right: str | None = None,
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[EodRow]:
        """Stream end-of-day reports between two sessions, inclusive.

        Args:
            symbol: Underlying symbol.
            expiration: Expiration, or ``*`` for every expiration on one day.
            start_date: Range start, inclusive.
            end_date: Range end, inclusive.
            strike: One strike, or omitted for every strike.
            right: ``call``, ``put``, or ``both``.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One end-of-day row.
        """
        async for row in self._iter_models(
            "option/history/eod",
            _history_params(
                symbol,
                expiration,
                session_date=None,
                start_date=start_date,
                end_date=end_date,
                strike=strike,
                right=right,
                interval=None,
            ),
            EodRow,
            response_format,
        ):
            yield row

    async def iter_greeks(
        self,
        symbol: str,
        expiration: date | str,
        *,
        session_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        strike: Decimal | float | str | None = None,
        right: str | None = None,
        interval: str = "1m",
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[GreeksRow]:
        """Stream first-order greeks and IV, including ``underlying_price``.

        There is no argument for second-order, third-order, ``greeks/all``, or
        ``trade_greeks``. Those routes raise ``ThetaDataSubscriptionError``.

        Args:
            symbol: Underlying symbol.
            expiration: Expiration, or ``*`` for every expiration on one day.
            session_date: Single session. Overrides a start/end range.
            start_date: Range start, inclusive.
            end_date: Range end, inclusive.
            strike: One strike, or omitted for every strike.
            right: ``call``, ``put``, or ``both``.
            interval: Bar size. ``expiration=*`` requires ``1m`` or greater.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One first-order greek row.
        """
        async for row in self._iter_models(
            "option/history/greeks/first_order",
            _history_params(
                symbol,
                expiration,
                session_date=session_date,
                start_date=start_date,
                end_date=end_date,
                strike=strike,
                right=right,
                interval=interval,
            ),
            GreeksRow,
            response_format,
        ):
            yield row

    async def iter_records(
        self,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        *,
        response_format: ResponseFormat = "csv",
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream one Standard endpoint as row dictionaries.

        Pro-only paths raise before a connection is opened.

        Args:
            endpoint: Path under the v3 base URL.
            params: Query parameters.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One dictionary per data row.

        Raises:
            ThetaDataSubscriptionError: The endpoint is outside Options Standard.
            ThetaDataNoDataError: The terminal reported no rows.
            ThetaDataRateLimitError: The account stayed rate limited after retries.
            ThetaDataTerminalUnreachableError: The terminal could not be reached.
            ThetaDataRequestError: The parameters or a row were rejected.
        """
        path = require_standard_endpoint(endpoint)
        query = _query(params, response_format)
        validate_chain_request(path, query)
        yielded = False
        attempts = self.settings.api_max_retries + 1
        for attempt in range(attempts):
            retry_after: float | None = None
            try:
                async with account_request_slot(self.settings.api_rate_limit):
                    async with self.session.get(self._url(path), params=query) as response:
                        if response.status != 200:
                            error = await _error_from_response(response, query, path)
                            give_up = yielded or not _is_transient(response.status) or attempt == attempts - 1
                            if give_up:
                                raise error
                            retry_after = _retry_after(response)
                            logger.warning("ThetaData %s returned HTTP %s; retrying", path, response.status)
                        else:
                            async for record in _records(response, response_format):
                                yielded = True
                                yield record
                            return
            except ThetaDataClientError:
                raise
            except (TimeoutError, aiohttp.ClientError) as exc:
                if yielded or attempt == attempts - 1:
                    raise _transport_failure(path, exc) from None
                _warn_retry(path, exc)
            await self._pause(attempt, retry_after)
        raise ThetaDataTerminalUnreachableError(_terminal_message(path))

    async def _iter_models(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        model: type[RowT],
        response_format: ResponseFormat,
    ) -> AsyncIterator[RowT]:
        """Stream ``endpoint`` and validate each row as ``model``.

        Args:
            endpoint: Standard path.
            params: Query parameters.
            model: Pydantic row type.
            response_format: ``csv`` or ``ndjson``.

        Yields:
            One validated row.

        Raises:
            ThetaDataRequestError: A row does not match ``model``.
        """
        async for record in self.iter_records(endpoint, params, response_format=response_format):
            try:
                yield model.model_validate(record)
            except ValidationError as exc:
                raise ThetaDataRequestError(f"Could not parse a {model.__name__} row from {endpoint}.") from exc

    async def _read_full(self, endpoint: str, params: Mapping[str, str]) -> str:
        """Read a small non-history body, retrying transient failures.

        Args:
            endpoint: Allow-listed list path.
            params: Query parameters, including ``format``.

        Returns:
            The response text.

        Raises:
            ThetaDataClientError: The terminal returned a terminal error, or retries ran out.
        """
        attempts = self.settings.api_max_retries + 1
        for attempt in range(attempts):
            retry_after: float | None = None
            try:
                async with account_request_slot(self.settings.api_rate_limit):
                    async with self.session.get(self._url(endpoint), params=params) as response:
                        if response.status != 200:
                            error = await _error_from_response(response, params, endpoint)
                            if not _is_transient(response.status) or attempt == attempts - 1:
                                raise error
                            retry_after = _retry_after(response)
                            logger.warning("ThetaData %s returned HTTP %s; retrying", endpoint, response.status)
                        else:
                            raw = await response.content.read()
                            return raw.decode("utf-8")
            except ThetaDataClientError:
                raise
            except (TimeoutError, aiohttp.ClientError) as exc:
                if attempt == attempts - 1:
                    raise _transport_failure(endpoint, exc) from None
                _warn_retry(endpoint, exc)
            await self._pause(attempt, retry_after)
        raise ThetaDataTerminalUnreachableError(_terminal_message(endpoint))

    async def _pause(self, attempt: int, retry_after: float | None) -> None:
        """Sleep outside the request slot.

        Args:
            attempt: Zero-based retry index.
            retry_after: Terminal ``Retry-After`` seconds, when the header is numeric.
        """
        if self._uniform is None:
            delay = backoff_delay(attempt, self.settings.api_backoff_base, self.settings.api_backoff_cap)
        else:
            delay = backoff_delay(
                attempt,
                self.settings.api_backoff_base,
                self.settings.api_backoff_cap,
                uniform=self._uniform,
            )
        if retry_after is not None:
            delay = min(self.settings.api_backoff_cap, max(delay, retry_after))
        await self._sleep(delay)

    def _url(self, endpoint: str) -> str:
        """Join the base URL and a normalized path.

        Args:
            endpoint: Normalized relative path.

        Returns:
            Absolute URL.
        """
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    async def get_option_expirations(self, symbol: str) -> list[date]:
        """Collect expiration dates for one underlying.

        Args:
            symbol: Underlying symbol.

        Returns:
            Expiration dates. The list is small; history chains are not collected here.
        """
        return [row.expiration async for row in self.list_expirations(symbol)]

    async def get_option_strikes(self, symbol: str, expiration: date) -> list[float]:
        """Collect strikes for one expiration.

        Args:
            symbol: Underlying symbol.
            expiration: Expiration date.

        Returns:
            Strike prices as floats.
        """
        return [float(row.strike) async for row in self.list_strikes(symbol, expiration)]

    async def get_stock_symbols(self) -> list[str]:
        """Refuse stock listings. This subscription is Options Standard only.

        Returns:
            Never.

        Raises:
            ThetaDataSubscriptionError: Always.
        """
        raise ThetaDataSubscriptionError(
            "stock/list/symbols is not included in the ThetaData Options Standard subscription."
        )

    async def get_stock_dates(self, symbol: str, request_type: str = "quote") -> list[date]:
        """Refuse stock date listings. This subscription is Options Standard only.

        Args:
            symbol: Unused stock symbol.
            request_type: Unused request type.

        Returns:
            Never.

        Raises:
            ThetaDataSubscriptionError: Always.
        """
        del symbol, request_type
        raise ThetaDataSubscriptionError(
            "stock date listings are not included in the ThetaData Options Standard subscription."
        )

    async def get_index_symbols(self) -> list[str]:
        """Refuse index listings. There is no indices subscription.

        Returns:
            Never.

        Raises:
            ThetaDataSubscriptionError: Always.
        """
        raise ThetaDataSubscriptionError(
            "index listings are not included in the ThetaData Options Standard subscription."
        )

    async def get_index_dates(self, symbol: str) -> list[date]:
        """Refuse index date listings. There is no indices subscription.

        Args:
            symbol: Unused index symbol.

        Returns:
            Never.

        Raises:
            ThetaDataSubscriptionError: Always.
        """
        del symbol
        raise ThetaDataSubscriptionError("index dates are not included in the ThetaData Options Standard subscription.")


def _query(params: Mapping[str, Any] | None, response_format: str) -> dict[str, str]:
    """Copy query parameters and set the response format.

    Args:
        params: Caller parameters.
        response_format: ``csv``, ``ndjson``, or ``json``.

    Returns:
        String query parameters.
    """
    query = {str(key): _stringify(value) for key, value in dict(params or {}).items()}
    query["format"] = response_format
    return query


def _stringify(value: Any) -> str:
    """Render one query value.

    Args:
        value: A date, decimal, or other scalar.

    Returns:
        The query text.
    """
    if isinstance(value, date):
        return _format_day(value)
    return str(value)


def _format_day(value: date) -> str:
    """Format a session date as ``YYYYMMDD``.

    Args:
        value: Calendar date.

    Returns:
        Eight-digit text.
    """
    return value.strftime("%Y%m%d")


def _format_expiration(expiration: date | str) -> str:
    """Format an expiration query value.

    Args:
        expiration: A date or ``*``.

    Returns:
        ``*`` or ``YYYYMMDD``.
    """
    if isinstance(expiration, str):
        return expiration
    return _format_day(expiration)


def _history_params(
    symbol: str,
    expiration: date | str,
    *,
    session_date: date | None,
    start_date: date | None,
    end_date: date | None,
    strike: Decimal | float | str | None,
    right: str | None,
    interval: str | None,
) -> dict[str, str]:
    """Build a history query.

    Args:
        symbol: Underlying symbol.
        expiration: Expiration or ``*``.
        session_date: Single session.
        start_date: Range start.
        end_date: Range end.
        strike: Optional strike.
        right: Optional right.
        interval: Optional interval. ``None`` omits the parameter.

    Returns:
        Query parameters without ``format``.
    """
    params: dict[str, str] = {
        "symbol": symbol,
        "expiration": _format_expiration(expiration),
    }
    if session_date is not None:
        params["date"] = _format_day(session_date)
    if start_date is not None:
        params["start_date"] = _format_day(start_date)
    if end_date is not None:
        params["end_date"] = _format_day(end_date)
    if strike is not None:
        params["strike"] = format(Decimal(str(strike)), "f")
    if right is not None:
        params["right"] = right.lower()
    if interval is not None:
        params["interval"] = interval
    return params


def _is_transient(status: int) -> bool:
    """Return whether an HTTP status is worth a bounded retry.

    Args:
        status: HTTP status code.

    Returns:
        True for rate limits, disconnects, and temporary server failures.
    """
    return status in _TRANSIENT_STATUSES


def _retry_after(response: aiohttp.ClientResponse) -> float | None:
    """Parse a numeric ``Retry-After`` header.

    Args:
        response: Terminal response.

    Returns:
        Seconds to wait, or ``None`` when the header is absent or not a number.
    """
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _status_label(status: int) -> str:
    """Return the terminal's name for a status code.

    Args:
        status: HTTP status code.

    Returns:
        A label such as ``472 NO_DATA``.
    """
    return _STATUS_NAMES.get(status, str(status))


def _detail_suffix(detail: str) -> str:
    """Return a short sanitized suffix for an error body.

    Args:
        detail: Already sanitized body text.

    Returns:
        Empty string, or a space plus the detail.
    """
    if not detail:
        return ""
    return f" {detail}"


def _warn_retry(endpoint: str, exc: BaseException) -> None:
    """Log one transient transport failure before the backoff sleep.

    Args:
        endpoint: Normalized path.
        exc: Timeout or client error that will be retried.
    """
    if isinstance(exc, TimeoutError):
        logger.warning("ThetaData %s timed out; retrying", endpoint)
        return
    logger.warning("ThetaData %s could not be reached; retrying", endpoint)


def _transport_failure(endpoint: str, exc: BaseException) -> ThetaDataClientError:
    """Build the final error for a timeout or a connection failure.

    Args:
        endpoint: Normalized path that was requested.
        exc: Timeout or aiohttp client error. Retries are already exhausted,
            or the body had already started.

    Returns:
        A client error tagged with ``timeout`` or the exception class name.
    """
    if isinstance(exc, TimeoutError):
        kind = "connect" if isinstance(exc, aiohttp.ConnectionTimeoutError) else "read"
        error = ThetaDataTerminalUnreachableError(f"ThetaData {kind} timeout while requesting {endpoint}.")
        return tag_failure(error, reason="timeout", endpoint=endpoint)
    error = ThetaDataTerminalUnreachableError(_terminal_message(endpoint))
    return tag_failure(error, reason=type(exc).__name__, endpoint=endpoint)


async def _error_from_response(
    response: aiohttp.ClientResponse,
    params: Mapping[str, str],
    endpoint: str,
) -> ThetaDataClientError:
    """Map a non-200 terminal response to a client error.

    Only the first 2 KiB of the body are read.

    Args:
        response: Failed response.
        params: Query that was sent, used to name the symbol and session.
        endpoint: Normalized path, stored on the error for the job log.

    Returns:
        The typed error, tagged ``http_<status>``. Transient statuses are still
        returned so the caller can decide to retry.
    """
    raw = await response.content.read(2048)
    detail = sanitize_text(raw.decode("utf-8", errors="replace"))
    status = response.status
    label = _status_label(status)
    symbol = params.get("symbol", "the requested symbol")
    when = params.get("date") or params.get("start_date") or params.get("expiration") or "the requested session"
    if status == 429:
        error: ThetaDataClientError = ThetaDataRateLimitError(
            f"ThetaData rate limit ({label}). Too many requests are in flight for this account.{_detail_suffix(detail)}"
        )
    elif status == 471:
        error = ThetaDataSubscriptionError(
            f"ThetaData subscription does not permit this request ({label}). Options Standard only."
            f"{_detail_suffix(detail)}"
        )
    elif status == 472:
        error = ThetaDataNoDataError(f"No ThetaData rows for {symbol} on {when} ({label}).{_detail_suffix(detail)}")
    elif status in {474, 571}:
        error = ThetaDataTerminalUnreachableError(
            f"Theta Terminal is not reachable ({label}). Start Theta Terminal v3 and retry.{_detail_suffix(detail)}"
        )
    elif status == 476:
        error = ThetaDataRequestError(
            f"Theta Terminal rejected the host ({label}). Use one of 127.0.0.1 or localhost for the whole run."
        )
    elif status == 473:
        error = ThetaDataRequestError(f"ThetaData rejected the request parameters ({label}).{_detail_suffix(detail)}")
    else:
        error = ThetaDataRequestError(f"ThetaData request failed ({label}).{_detail_suffix(detail)}")
    return tag_failure(error, reason=f"http_{status}", endpoint=endpoint)


def _terminal_message(endpoint: str) -> str:
    """Describe a terminal that cannot be reached.

    Args:
        endpoint: Normalized path that was requested.

    Returns:
        A message with the path and no credentials.
    """
    return (
        f"Theta Terminal is not running or not reachable while requesting {endpoint}. "
        "Start Theta Terminal v3 and retry."
    )


async def _response_chunks(response: aiohttp.ClientResponse) -> AsyncIterator[bytes]:
    """Yield body chunks without assembling them.

    Args:
        response: Open successful response.

    Yields:
        The next chunk.
    """
    async for chunk in response.content.iter_chunked(65536):
        yield bytes(chunk)


async def _records(
    response: aiohttp.ClientResponse,
    response_format: ResponseFormat,
) -> AsyncIterator[dict[str, Any]]:
    """Parse a successful body into row dictionaries.

    Args:
        response: Open 200 response.
        response_format: ``csv`` or ``ndjson``.

    Yields:
        One row dictionary.

    Raises:
        ThetaDataRequestError: A line cannot be parsed, or the format is not streamed.
    """
    try:
        lines = iter_text_lines(_response_chunks(response))
        async for record in iter_response_records(lines, response_format):
            yield dict(record)
    except (ValueError, UnicodeError, csv.Error) as exc:
        raise ThetaDataRequestError("ThetaData row could not be parsed.") from exc


def _json_envelope(body: str) -> dict[str, Any]:
    """Wrap a JSON list body as ``{"response": ...}``.

    Args:
        body: Response text.

    Returns:
        The envelope downloaders already expect.

    Raises:
        ThetaDataRequestError: The body is not JSON.
    """
    try:
        data = json.loads(body) if body.strip() else []
    except ValueError as exc:
        raise ThetaDataRequestError("ThetaData JSON response could not be parsed.") from exc
    if isinstance(data, dict) and "response" in data:
        return {"response": data["response"]}
    return {"response": data}
