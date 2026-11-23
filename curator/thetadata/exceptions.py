"""ThetaData errors.

This module stays free of the client and the worker pool so either side can
import it while the other is still loading. Every message goes through
``sanitize_text`` so a response body cannot carry a database URL or password.
"""

from __future__ import annotations

import re

_SECRET_TEXT = re.compile(
    r"(?i)("
    r"postgres(?:ql)?(?:\+\w+)?://\S+"
    r"|://[^/\s:]+:[^@\s/]+@"
    r"|(?:password|passwd|secret|api[_-]?key|dsn)\s*[=:]\s*\S+"
    r")"
)


def sanitize_text(text: str) -> str:
    """Remove credentials and database URLs from a message.

    Args:
        text: Raw error text.

    Returns:
        A single-line message with secrets replaced by ``[redacted]``.
    """
    cleaned = _SECRET_TEXT.sub("[redacted]", text).replace("\r", " ").replace("\n", " ").strip()
    if len(cleaned) > 300:
        return cleaned[:300] + "..."
    return cleaned


class ThetaDataClientError(Exception):
    """Base error for the ThetaData client.

    Subclass messages are safe to log.
    """

    def __init__(self, message: str) -> None:
        """Store a sanitized message.

        Args:
            message: Explanation of what failed.
        """
        super().__init__(sanitize_text(message))


class ThetaDataRateLimitError(ThetaDataClientError):
    """The account is rate limited or the terminal returned HTTP 429."""


class ThetaDataSubscriptionError(ThetaDataClientError, PermissionError):
    """The Options Standard subscription does not allow this request."""


class ThetaDataNoDataError(ThetaDataClientError):
    """The terminal has no rows for the requested contract and date."""


class ThetaDataTerminalUnreachableError(ThetaDataClientError):
    """Theta Terminal is not running, restarting, or not accepting connections."""


class ThetaDataRequestError(ThetaDataClientError):
    """The request was rejected or a response row could not be parsed."""


class ThetaDataMachineLockError(ThetaDataClientError):
    """Another theta backfill or nightly already holds the machine lock."""
