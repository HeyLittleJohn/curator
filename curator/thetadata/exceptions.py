"""ThetaData errors.

This module stays free of the client and the worker pool so either side can
import it while the other is still loading. Every message goes through
``sanitize_text`` so a response body cannot carry a database URL or password.
Tracebacks written to the log are redacted the same way and keep their lines.
"""

from __future__ import annotations

import logging
import re
import traceback
from datetime import date
from pathlib import Path

_LOG_ATTACHED = False

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
    cleaned = _redact(text).replace("\n", " ").strip()
    if len(cleaned) > 300:
        return cleaned[:300] + "..."
    return cleaned


def sanitize_traceback(text: str) -> str:
    """Remove credentials and database URLs from a traceback.

    Args:
        text: Output of ``traceback.format_exception``. May contain a DSN.

    Returns:
        The traceback with secrets replaced. Line breaks stay so the log is
        readable. Text longer than 8000 characters is truncated.
    """
    cleaned = _redact(text).strip()
    if len(cleaned) > 8000:
        return cleaned[:8000] + "..."
    return cleaned


def log_sanitized_exception(exc: BaseException) -> str:
    """Write a secret-free traceback to the curator log file.

    The file is ``~/curator/.logs/curator_YYYY-MM-DD.log``, the same directory
    ``logger_setup`` uses. The traceback is redacted before it is logged.
    ``logger.exception`` is not used, because that handler would attach the
    original stack and could write a DSN that appeared in the message.

    Args:
        exc: The exception that was caught.

    Returns:
        The sanitized traceback that was written.
    """
    text = sanitize_traceback("".join(traceback.format_exception(exc)))
    logger = logging.getLogger("curator.thetadata")
    _ensure_log_file(logger)
    logger.error("%s", text)
    return text


def _redact(text: str) -> str:
    """Replace secrets and normalize newlines.

    Args:
        text: Raw text that may contain a URL or a password assignment.

    Returns:
        Text with those secrets replaced by ``[redacted]``.
    """
    from curator.thetadata.secrets import scrub_secrets

    scrubbed = scrub_secrets(text)
    return _SECRET_TEXT.sub("[redacted]", scrubbed).replace("\r\n", "\n").replace("\r", "\n")


def _ensure_log_file(logger: logging.Logger) -> None:
    """Attach one file handler for sanitized theta tracebacks.

    Args:
        logger: The ``curator.thetadata`` logger.

    Returns:
        None.
    """
    global _LOG_ATTACHED
    if _LOG_ATTACHED:
        return
    path = Path.home() / "curator" / ".logs" / f"curator_{date.today():%Y-%m-%d}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    _LOG_ATTACHED = True


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
