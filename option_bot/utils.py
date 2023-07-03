import functools
import inspect
import json
import os
from datetime import datetime

import numpy as np
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.asyncio import AsyncSession

from option_bot.proj_constants import async_session_maker, log, POOL_DEFAULT_KWARGS


_async_session_maker = async_session_maker  # NOTE: This is monkeypatched by a test fixture!


def timestamp_to_datetime(timestamp: int, msec_units: bool = True) -> datetime:
    return datetime.fromtimestamp(timestamp / 1000) if msec_units else datetime.fromtimestamp(timestamp)


def two_years_ago():
    return datetime.now() - relativedelta(months=24)


def first_weekday_of_month(year_month_array: np.ndarray) -> np.ndarray:
    if year_month_array.dtype != np.datetime64:
        year_month_array = year_month_array.astype(np.datetime64)
    return np.busday_offset(year_month_array, 0, roll="modifiedpreceding", weekmask=[1, 1, 1, 1, 1, 0, 0])
    # NOTE: may need to add info for market holidays


def timestamp_now(msec_units: bool = True):
    """returns a timestamp in milliseconds"""
    return int(datetime.now().timestamp() * 1000) if msec_units else int(datetime.now().timestamp())


def chunk_iter_generator(data: list, size=250000):
    for i in range(0, len(data), size):
        yield data[i : i + size]


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
        func_mod_and_name = f"{func.__module__}.{func.__name__}"
        log.info(f"Starting {func_mod_and_name}")
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

            log.info(f"Finished {func_mod_and_name}")
            return func_return
        except Exception as e:
            log.exception(e)
            raise

    return wrapper_events


def write_api_data_to_file(data: list[dict], file_path: str, file_name: str):
    """Write api data to a json file"""
    os.makedirs(file_path, exist_ok=True)
    with open(file_path + file_name, "w") as f:
        json.dump(data, f)
    log.info(f"Data written to {file_path + file_name}")


def read_data_from_file(file_path: str) -> list[dict]:
    """Read api data from a json file"""
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def pool_kwarg_config(kwargs: dict) -> dict:
    """This function updates the kwargs for an aiomultiprocess.Pool from the defaults."""
    pool_kwargs = POOL_DEFAULT_KWARGS.copy()
    pool_kwargs.update(kwargs)
    return pool_kwargs
