"""ThetaData worker pool.

Each worker process runs one asyncio event loop, using the same aiomultiprocess
pool helper as the Polygon pipeline. ThetaData allows four concurrent HTTP
requests for the whole account, so every request takes a slot from one
semaphore shared by the parent and every worker.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from multiprocessing.synchronize import Semaphore as MpSemaphore
from typing import TYPE_CHECKING, Any, Optional

from aiomultiprocess import Pool
from aiomultiprocess.core import get_context

from curator.thetadata.config import BackfillMode, Settings
from curator.utils import pool_kwarg_config

if TYPE_CHECKING:
    from curator.thetadata.downloaders import BackfillResult

logger = logging.getLogger(__name__)

ACCOUNT_CONCURRENCY = 4

_request_slots: MpSemaphore | None = None


@dataclass
class WorkerTask:
    """One backfill unit handed to a worker process."""

    task_id: str
    asset_type: str
    data_type: str
    symbol: str
    start_date: date
    end_date: date
    mode: BackfillMode
    extra: dict[str, Any]


@dataclass
class WorkerResult:
    """Outcome of a single worker task."""

    task_id: str
    result: Optional[BackfillResult]
    error: Optional[str]


def install_request_slots(slots: MpSemaphore) -> None:
    """Install the shared request semaphore in this process.

    Args:
        slots: Semaphore created in the parent and passed to each worker.
    """
    global _request_slots
    _request_slots = slots


def request_slots() -> MpSemaphore:
    """Return the process-wide ThetaData request semaphore.

    Returns:
        A semaphore whose value is ``ACCOUNT_CONCURRENCY``.
    """
    global _request_slots
    if _request_slots is None:
        _request_slots = get_context().Semaphore(ACCOUNT_CONCURRENCY)
    return _request_slots


@asynccontextmanager
async def account_request_slot() -> AsyncGenerator[None, None]:
    """Hold one account-wide ThetaData HTTP slot.

    Acquire runs in a thread so a full gate does not block the event loop.
    Before a pool is configured this is a no-op and the client's own
    semaphore is the only limit.

    Yields:
        None while the slot is held.
    """
    slots = _request_slots
    if slots is None:
        yield
        return
    await asyncio.to_thread(slots.acquire)
    try:
        yield
    finally:
        slots.release()


def theta_pool_kwargs(worker_count: int | None = None) -> dict[str, Any]:
    """Build ``aiomultiprocess.Pool`` kwargs for ThetaData workers.

    Reuses curator's pool defaults, including one uvloop event loop per
    worker. Child concurrency is 1 so each process runs a single task on
    that loop. HTTP fan-out stays at four via ``request_slots``, which lets
    CPU-heavy parsing use more processes than in-flight requests.

    Args:
        worker_count: Worker processes. ``None`` keeps curator's default.

    Returns:
        Keyword arguments for ``aiomultiprocess.Pool``.
    """
    slots = request_slots()
    overrides: dict[str, Any] = {
        "childconcurrency": 1,
        "initializer": install_request_slots,
        "initargs": (slots,),
    }
    if worker_count is not None and worker_count > 0:
        overrides["processes"] = worker_count
        overrides["queuecount"] = worker_count
    return pool_kwarg_config(overrides)


async def process_task(task: WorkerTask, settings: Settings) -> WorkerResult:
    """Download and upsert one symbol/data-type range inside a worker.

    Args:
        task: Work item for this call.
        settings: Application settings, pickled in from the parent.

    Returns:
        The inserted-row stats, or an error string when the task fails.
    """
    from curator.thetadata.client import ThetaDataClient
    from curator.thetadata.db import DatabaseManager
    from curator.thetadata.downloaders import (
        EquityEODDownloader,
        EquityOHLCDownloader,
        EquityQuoteDownloader,
        EquityTradeDownloader,
        IndexEODDownloader,
        IndexOHLCDownloader,
        IndexPriceDownloader,
        OptionGreeksDownloader,
        OptionOHLCDownloader,
        OptionOpenInterestDownloader,
        OptionTradeGreeksDownloader,
    )

    downloader_map = {
        ("equity", "ohlc"): EquityOHLCDownloader,
        ("equity", "trade"): EquityTradeDownloader,
        ("equity", "quote"): EquityQuoteDownloader,
        ("equity", "eod"): EquityEODDownloader,
        ("option", "ohlc"): OptionOHLCDownloader,
        ("option", "trade_greeks"): OptionTradeGreeksDownloader,
        ("option", "greeks"): OptionGreeksDownloader,
        ("option", "open_interest"): OptionOpenInterestDownloader,
        ("index", "ohlc"): IndexOHLCDownloader,
        ("index", "price"): IndexPriceDownloader,
        ("index", "eod"): IndexEODDownloader,
    }
    downloader_class = downloader_map.get((task.asset_type, task.data_type))
    if downloader_class is None:
        return WorkerResult(
            task_id=task.task_id,
            result=None,
            error=f"Unknown asset/data type: {task.asset_type}/{task.data_type}",
        )

    try:
        db_manager = DatabaseManager(settings)
        await db_manager.initialize()
        async with ThetaDataClient(settings) as client:
            downloader = downloader_class(client, settings)
            async with db_manager.session() as session:
                result = await downloader.backfill(
                    symbol=task.symbol,
                    start_date=task.start_date,
                    end_date=task.end_date,
                    request_type=task.data_type,
                    session=session,
                    mode=task.mode,
                    **task.extra,
                )
        await db_manager.close()
        return WorkerResult(task_id=task.task_id, result=result, error=None)
    except Exception as exc:
        logger.exception("Error processing task %s", task.task_id)
        return WorkerResult(task_id=task.task_id, result=None, error=str(exc))


async def run_tasks(tasks: list[WorkerTask], settings: Settings) -> list[WorkerResult]:
    """Run backfill tasks on the shared aiomultiprocess pool.

    Args:
        tasks: Work items. Each runs on one worker event loop.
        settings: Application settings passed into every task.

    Returns:
        One result per task, in input order. Empty when ``tasks`` is empty.
    """
    if not tasks:
        return []
    payloads = [(task, settings) for task in tasks]
    async with Pool(**theta_pool_kwargs(settings.worker_count)) as pool:
        return list(await pool.starmap(process_task, payloads))
