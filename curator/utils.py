import functools
import inspect
import json
import logging
import os
from datetime import datetime
from json import JSONDecodeError
from typing import TextIO

import numpy as np
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.asyncio import AsyncSession

from curator.proj_constants import POOL_DEFAULT_KWARGS, async_session_maker, e_cal, log, o_cal

_async_session_maker = async_session_maker  # NOTE: This is monkeypatched by a test fixture!


def timestamp_to_datetime(timestamp: int, msec_units: bool = True, nano_sec: bool = False) -> datetime:
    if nano_sec:
        dem = 1000000000
    elif msec_units:
        dem = 1000
    else:
        dem = 1
    return datetime.fromtimestamp(timestamp / dem)


def string_to_datetime(date_string: str, date_format: str = "%Y-%m-%d") -> datetime:
    return datetime.strptime(date_string, date_format)


def string_to_date(date_string: str, date_format: str = "%Y-%m-%d") -> datetime.date:
    return datetime.strptime(date_string, date_format).date()


def months_ago(months=24, end_date: datetime = None) -> datetime:
    return (
        datetime.now() - relativedelta(months=months)
        if not end_date
        else end_date - relativedelta(months=months)
    )


def first_weekday_of_month(year_month_array: np.ndarray) -> np.ndarray:
    if year_month_array.dtype != np.datetime64:
        year_month_array = year_month_array.astype(np.datetime64)
    return np.busday_offset(year_month_array, 0, roll="modifiedpreceding", weekmask=[1, 1, 1, 1, 1, 0, 0])
    # NOTE: may need to add info for market holidays


def trading_days_in_range(start_date: str, end_date: str, cal_type="e_cal", count: bool = True) -> int:
    if cal_type == "e_cal":
        cal = e_cal
    elif cal_type == "o_cal":
        cal = o_cal
    else:
        raise ValueError(f"cal_type must be 'e_cal' or 'o_cal', not {cal_type}")
    return cal[start_date:end_date].shape[0] if count else cal[start_date:end_date]


def timestamp_now(msec_units: bool = True):
    """returns a timestamp in milliseconds"""
    return int(datetime.now().timestamp() * 1000) if msec_units else int(datetime.now().timestamp())


def chunk_iter_generator(data: list, size=250000):
    for i in range(0, len(data), size):
        yield data[i : i + size]


def clean_o_ticker(o_ticker: str) -> str:
    return o_ticker.split(":")[1] if ":" in o_ticker else o_ticker


def extract_underlying_from_o_ticker(o_ticker: str) -> str:
    o_ticker = clean_o_ticker(o_ticker)
    underlying = ""
    for char in o_ticker:
        if char.isdigit():
            break
        underlying += char
    return underlying


def Session(func):
    """
    Decorator that adds a SQLAlchemy AsyncSession to the function passed if the function is not
    already being passed an AsyncSession object.
    If no AsyncSession object is being passes this decorator will handle all session commit and
    rollback operations. Commit if no errors, rollback if there is an error raised.
    Example:
    @Session
    async def example(session: AsyncSession, other_data: str):
        ...
    a = await example(other_data="stuff")
    b = await example(async_session_maker(), "stuff")
    NOTE: The FIRST or SECOND argument is "session". "session" in ANY OTHER ARGUMENT SPOT will break!
    ONLY pass an AsyncSession object or NOTHING to the "session" argument!
    """

    async def _session_work(session: AsyncSession, args, kwargs):
        if "session" in kwargs:
            kwargs["session"] = session
        elif "session" in list(inspect.signature(func).parameters.keys()):
            sig_args = list(inspect.signature(func).parameters.keys())
            if sig_args[0] == "session":
                args = (session, *args)
            elif sig_args[0] in {"cls", "self"} and sig_args[1] == "session":
                args = (args[0], session, *args[1:])
            else:
                raise RuntimeError("session is not the first or second argument in the function")
        else:
            raise RuntimeError("session not an args")
        func_return = await func(*args, **kwargs)
        return func_return

    @functools.wraps(func)
    async def wrapper_events(*args, **kwargs):
        wrap_log = logging.getLogger(__name__)
        func_mod_and_name = f"{func.__module__}.{func.__name__}"
        wrap_log.info(f"Starting {func_mod_and_name}")
        session_passed = False
        for arg in list(args) + list(kwargs.values()):
            if issubclass(type(arg), AsyncSession):
                session_passed = True
                break
        try:
            if session_passed is True:
                func_return = await func(*args, **kwargs)
            else:
                session: AsyncSession = _async_session_maker()
                try:
                    func_return = await _session_work(session, args, kwargs)
                except:  # noqa: E722
                    await session.rollback()
                    raise
                else:
                    await session.commit()
                finally:
                    await session.close()

            wrap_log.info(f"Finished {func_mod_and_name}")
            return func_return
        except Exception as e:
            wrap_log.exception(e)
            raise

    return wrapper_events


def write_api_data_to_file(data: list[dict] | dict, file_path: str, file_name: str = None, append=False):
    """Write api data to a json file"""
    os.makedirs(file_path, exist_ok=True)
    if not append:
        with open(file_path + file_name, "w") as f:
            json.dump(data, f)
        log.info(f"Data written to {file_path + file_name}")
    else:
        files = os.listdir(file_path)
        file_name = files[0] if len(files) > 0 else str(timestamp_now()) + ".json"
        with open(file_path + file_name, "+a") as f:
            if not f.tell() > 0:
                f.write("[")
            json.dump(data, f)
            f.write(",\n")


def close_json_file(file: TextIO):
    """removes the dangling comma and adds a closing bracket to correctly format json files
    Specifically will be used with Quotes downloads"""
    file.seek(0, 2)
    s = file.tell()
    file.seek(s - 2)
    file.truncate()
    file.write("]")
    file.seek(0)


# NOTE: call prep_json_file here with flag
def read_data_from_file(file_path: str, close_file=False) -> list[dict]:
    """Read api data from a json file"""
    try:
        if close_file:
            with open(file_path, "+a") as f:
                if close_file:
                    close_json_file(f)
                    data = json.load(f)
        else:
            with open(file_path, "r") as f:
                data = json.load(f)
    except JSONDecodeError as e:
        raise e
    return data


# def get_ticker_from_oticker(o_ticker: str) -> str:
#     for i, char in enumerate(o_ticker):
#         if not char.isalpha():
#             return o_ticker[:i]

#     return o_ticker


def pool_kwarg_config(kwargs: dict) -> dict:
    """This function updates the kwargs for an aiomultiprocess.Pool from the defaults."""
    pool_kwargs = POOL_DEFAULT_KWARGS.copy()
    pool_kwargs.update(kwargs)
    if pool_kwargs["queuecount"] > pool_kwargs["processes"]:
        pool_kwargs["queuecount"] = pool_kwargs["processes"]
    return pool_kwargs
