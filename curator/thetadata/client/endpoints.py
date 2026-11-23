"""Options Standard endpoint allow-list and chain-request checks.

Pro paths are refused in-process. They never open a socket.
"""

from __future__ import annotations

from collections.abc import Mapping

from curator.thetadata.client.errors import ThetaDataRequestError, ThetaDataSubscriptionError

#: Endpoints included in ThetaData Options Standard for this backfill.
STANDARD_ENDPOINTS: frozenset[str] = frozenset(
    {
        "option/list/expirations",
        "option/list/strikes",
        "option/list/contracts/quote",
        "option/list/contracts/trade",
        "option/history/quote",
        "option/history/ohlc",
        "option/history/open_interest",
        "option/history/eod",
        "option/history/greeks/first_order",
    }
)

#: History routes whose ``interval`` must be at least 1 minute when expiration is ``*``.
_INTERVAL_ENDPOINTS: frozenset[str] = frozenset(
    {
        "option/history/quote",
        "option/history/ohlc",
        "option/history/greeks/first_order",
    }
)

_PRO_ONLY_PREFIXES: tuple[str, ...] = (
    "option/history/greeks/all",
    "option/history/greeks/second_order",
    "option/history/greeks/third_order",
    "option/history/trade_greeks",
    "option/snapshot/greeks/all",
    "option/snapshot/greeks/second_order",
    "option/snapshot/greeks/third_order",
    "option/at_time/trade_greeks",
)

_SUB_MINUTE_INTERVALS: frozenset[str] = frozenset({"tick", "10ms", "100ms", "500ms", "1s", "5s", "10s", "15s", "30s"})
_AT_LEAST_ONE_MINUTE: frozenset[str] = frozenset({"1m", "5m", "10m", "15m", "30m", "1h"})
_PRO_ONLY_MESSAGE = (
    "is not included in the ThetaData Options Standard subscription. "
    "Second-order greeks, third-order greeks, greeks/all, and trade_greeks require Pro."
)


def normalize_endpoint(endpoint: str) -> str:
    """Return the v3 path without a leading slash, query, or ``v3/`` prefix.

    Args:
        endpoint: Caller-supplied path.

    Returns:
        A normalized relative path such as ``option/history/quote``.
    """
    path = endpoint.split("?", 1)[0].strip().lstrip("/")
    if path.startswith("v3/"):
        path = path[3:]
    return path.rstrip("/")


def _is_pro_only(path: str) -> bool:
    """Return whether ``path`` is a Pro-only greeks or trade-greeks route.

    Args:
        path: Normalized endpoint path.

    Returns:
        True when the path is outside Options Standard because it is Pro-only.
    """
    return any(path == prefix or path.startswith(prefix + "/") for prefix in _PRO_ONLY_PREFIXES)


def require_standard_endpoint(endpoint: str) -> str:
    """Accept one Options Standard path.

    Args:
        endpoint: Caller-supplied path.

    Returns:
        The normalized path.

    Raises:
        ThetaDataSubscriptionError: The path is Pro-only or not on the Standard list.
    """
    path = normalize_endpoint(endpoint)
    if _is_pro_only(path):
        raise ThetaDataSubscriptionError(f"{path} {_PRO_ONLY_MESSAGE}")
    if path not in STANDARD_ENDPOINTS:
        raise ThetaDataSubscriptionError(f"{path} is not included in the ThetaData Options Standard subscription.")
    return path


def _normalize_day(value: str) -> str:
    """Return ``YYYYMMDD`` for a date query value.

    Args:
        value: ``YYYY-MM-DD`` or ``YYYYMMDD``.

    Returns:
        Eight digits. Unrecognized values are returned unchanged so the
        comparison still sees them as different strings.
    """
    text = value.strip()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text.replace("-", "")
    return text


def _is_single_day(params: Mapping[str, str]) -> bool:
    """Return whether the query names exactly one session.

    Args:
        params: Query parameters.

    Returns:
        True when ``date`` is set, or ``start_date`` and ``end_date`` are the same day.
    """
    if params.get("date"):
        return True
    start = params.get("start_date")
    end = params.get("end_date")
    return bool(start and end and _normalize_day(start) == _normalize_day(end))


def validate_chain_request(endpoint: str, params: Mapping[str, str]) -> None:
    """Reject ``expiration=*`` requests the terminal will not serve.

    ``expiration=*`` is valid for one session only. On quote, OHLC, and
    first-order greeks the interval must be ``1m`` or greater. Sub-minute
    intervals are single-contract, single-day requests.

    Args:
        endpoint: Normalized or raw path.
        params: Query parameters about to be sent.

    Raises:
        ThetaDataRequestError: The expiration wildcard is combined with a
            multi-day range or a sub-minute interval.
    """
    path = normalize_endpoint(endpoint)
    if not path.startswith("option/history/"):
        return
    expiration = params.get("expiration", "")
    if expiration != "*":
        interval = params.get("interval")
        if interval is not None and interval not in _SUB_MINUTE_INTERVALS | _AT_LEAST_ONE_MINUTE:
            raise ThetaDataRequestError(f"Unknown ThetaData interval {interval!r}.")
        return
    if not _is_single_day(params):
        raise ThetaDataRequestError(
            "expiration=* is only valid for a single day. Pass date, or start_date equal to end_date."
        )
    interval = params.get("interval")
    if path not in _INTERVAL_ENDPOINTS:
        return
    if interval is None:
        raise ThetaDataRequestError("expiration=* on an interval endpoint requires interval of 1m or greater.")
    if interval in _SUB_MINUTE_INTERVALS:
        raise ThetaDataRequestError(
            f"expiration=* is only valid for a single day at intervals of 1m or greater. {interval} is below 1m."
        )
    if interval not in _AT_LEAST_ONE_MINUTE:
        raise ThetaDataRequestError(f"Unknown ThetaData interval {interval!r}.")
