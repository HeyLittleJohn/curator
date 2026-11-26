"""Reason codes for a failed ThetaData request.

``theta.progress`` has status and a row count, and no column for an error
message. Callers log the reason with the job identity and prefix the scrubbed
outcome text with the same code so a day summary can count them.
"""

from __future__ import annotations

from typing import TypeVar

from curator.thetadata.exceptions import sanitize_text
from curator.thetadata.secrets import scrub_secrets

FAILURE_REASON = "failure_reason"
FAILURE_ENDPOINT = "failure_endpoint"

_DATASET_ENDPOINTS: dict[str, str] = {
    "quotes": "option/history/quote",
    "ohlc": "option/history/ohlc",
    "greeks_first_order": "option/history/greeks/first_order",
    "open_interest": "option/history/open_interest",
    "eod": "option/history/eod",
}

_ExcT = TypeVar("_ExcT", bound=BaseException)


def tag_failure(exc: _ExcT, *, reason: str, endpoint: str) -> _ExcT:
    """Attach a reason code and the v3 path to an exception.

    Args:
        exc: Failure about to be raised or logged.
        reason: ``timeout``, ``http_<status>``, or an exception class name.
        endpoint: Normalized v3 path that failed.

    Returns:
        The same exception.
    """
    setattr(exc, FAILURE_REASON, reason)
    setattr(exc, FAILURE_ENDPOINT, endpoint)
    return exc


def classify_failure(exc: BaseException) -> tuple[str, str]:
    """Return a stable reason code and a short sanitized message.

    Args:
        exc: Exception raised while requesting or storing one job.

    Returns:
        ``(reason, message)``. ``reason`` is ``timeout`` for a read or connect
        timeout, ``http_<status>`` for a terminal HTTP status, or the exception
        class name. ``message`` is one line with secrets removed.
    """
    tagged = getattr(exc, FAILURE_REASON, None)
    if isinstance(tagged, str) and tagged.strip():
        reason = tagged.strip()
    elif isinstance(exc, TimeoutError):
        reason = "timeout"
    else:
        reason = type(exc).__name__
    message = sanitize_text(scrub_secrets(str(exc))) or reason
    return reason, message


def failure_endpoint(exc: BaseException, dataset: str) -> str:
    """Return the v3 path that failed, or the dataset's history path.

    Args:
        exc: Exception from the attempt. A tagged client error carries the path.
        dataset: Plan dataset name, used when the exception has no path.

    Returns:
        A path such as ``option/history/ohlc``.
    """
    tagged = getattr(exc, FAILURE_ENDPOINT, None)
    if isinstance(tagged, str) and tagged.strip():
        return tagged.strip()
    return _DATASET_ENDPOINTS.get(dataset, dataset)


def outcome_error(reason: str, message: str) -> str:
    """Join a reason code and a message for the progress outcome.

    Args:
        reason: Reason code. It must not contain a colon.
        message: Short sanitized message.

    Returns:
        ``reason: message``, which the day summary splits on the first colon.
    """
    return f"{reason}: {message}"
