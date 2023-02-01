from datetime import datetime


def timestamp_to_datetime(timestamp: int, msec_units: bool = True) -> datetime:
    return datetime.fromtimestamp(timestamp / 1000) if msec_units else datetime.fromtimestamp(timestamp)
