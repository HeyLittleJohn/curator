"""Parse ThetaData v3 timestamps.

v3 sends zone-less Eastern wall times. They are America/New_York, including DST,
and are stored as timestamptz. TD-5 should call ``parse_thetadata_timestamp``
before inserting.
"""

import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")

_OFFSET_AT_END = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})$", re.IGNORECASE)


def parse_thetadata_timestamp(value: str) -> datetime:
    """Parse a zone-less ThetaData v3 timestamp as America/New_York.

    Args:
        value: A timestamp such as ``2024-11-04T09:30:00.000`` with no offset.
            A single space may separate the date and the time.

    Returns:
        A timezone-aware datetime in America/New_York. During the fall-back
        hour the earlier (daylight) instant is used.

    Raises:
        ValueError: If ``value`` is empty, has a zone, omits the time, or falls
            in a DST gap.
    """
    text = value.strip()
    if not text or _OFFSET_AT_END.search(text):
        raise ValueError("ThetaData v3 timestamps are zone-less America/New_York times")
    if "T" not in text and " " not in text:
        raise ValueError("ThetaData v3 timestamps include a time")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("ThetaData v3 timestamp is not a valid date-time") from exc
    if parsed.tzinfo is not None:
        raise ValueError("ThetaData v3 timestamps are zone-less America/New_York times")
    aware = parsed.replace(tzinfo=EASTERN)
    back = aware.astimezone(timezone.utc).astimezone(EASTERN).replace(tzinfo=None)
    if back != parsed:
        raise ValueError("timestamp does not exist in America/New_York")
    return aware
