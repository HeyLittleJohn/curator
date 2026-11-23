"""Remove database URLs from errors.

Migration failures can echo a driver URL. Callers print ``scrub_secrets``
output and do not chain the original exception.
"""

import re

_DB_URL = re.compile(r"\b(?:postgresql|postgres)(?:\+[A-Za-z0-9_]+)?://\S+")
_PASSWORD = re.compile(r"(?i)\b(password|passwd|pwd)\s*[=:]\s*\S+")


def scrub_secrets(message: str) -> str:
    """Remove database URLs and password assignments from text.

    Args:
        message: An error or log line.

    Returns:
        The same text with connection strings and password assignments replaced.
    """
    scrubbed = _DB_URL.sub("<database-url>", message)
    return _PASSWORD.sub(r"\1=<redacted>", scrubbed)
