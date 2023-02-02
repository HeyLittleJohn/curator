from datetime import datetime

import numpy as np


def timestamp_to_datetime(timestamp: int, msec_units: bool = True) -> datetime:
    return datetime.fromtimestamp(timestamp / 1000) if msec_units else datetime.fromtimestamp(timestamp)


def first_weekday_of_month(year_month_array: np.ndarray):
    if year_month_array.dtype != np.datetime64:
        year_month_array = year_month_array.astype(np.datetime64)
    return np.busday_offset(year_month_array, 0, roll="modifiedpreceding", weekmask=[1, 1, 1, 1, 1, 0, 0])
    # NOTE: may need to add info for market holidays
