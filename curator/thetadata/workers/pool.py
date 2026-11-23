"""ThetaData worker pool.

Each worker process runs one asyncio event loop, using the same aiomultiprocess
pool helper as the Polygon pipeline. Child concurrency is 1. In-flight HTTP
requests share one spawn-context semaphore of at most 4, installed in every
worker through the pool initializer. The parent also holds a machine lock so a
second backfill cannot start its own semaphore.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any, Optional

from aiomultiprocess import Pool

from curator.thetadata.client.errors import sanitize_text
from curator.thetadata.config import BackfillMode, Settings
from curator.thetadata.limits import (
    ACCOUNT_CONCURRENCY as ACCOUNT_CONCURRENCY,
)
from curator.thetadata.limits import (
    backfill_lock,
    install_request_slots,
    request_slots,
)
from curator.utils import pool_kwarg_config

if TYPE_CHECKING:
    from curator.thetadata.downloaders import BackfillResult

logger = logging.getLogger(__name__)

_PRO_ONLY_DATA_TYPES = frozenset({"trade_greeks", "greeks_all", "second_order", "third_order"})


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


def theta_pool_kwargs(worker_count: int | None = None, *, concurrency: int | None = None) -> dict[str, Any]:
    """Build ``aiomultiprocess.Pool`` kwargs for ThetaData workers.

    Reuses curator's pool defaults, including one uvloop event loop per
    worker. Child concurrency is 1 so each process runs a single task on
    that loop. The shared semaphore is created here, in the parent's spawn
    context, and handed to each worker. Pass ``concurrency`` to lower the
    cap. The first call in the process fixes the semaphore size.

    Args:
        worker_count: Worker processes. ``None`` keeps curator's default.
        concurrency: Account-wide cap. ``None`` uses 4. Values above 4 raise.

    Returns:
        Keyword arguments for ``aiomultiprocess.Pool``.
    """
    slots = request_slots(concurrency)
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
    if task.data_type in _PRO_ONLY_DATA_TYPES:
        return WorkerResult(
            task_id=task.task_id,
            result=None,
            error=(
                "Options Standard does not include trade_greeks, greeks/all, or second- and third-order greeks. "
                "Use first-order greeks."
            ),
        )

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
    )

    downloader_map = {
        ("equity", "ohlc"): EquityOHLCDownloader,
        ("equity", "trade"): EquityTradeDownloader,
        ("equity", "quote"): EquityQuoteDownloader,
        ("equity", "eod"): EquityEODDownloader,
        ("option", "ohlc"): OptionOHLCDownloader,
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
        message = sanitize_text(f"{type(exc).__name__}: {exc}")
        logger.error("Error processing task %s: %s", task.task_id, message)
        return WorkerResult(task_id=task.task_id, result=None, error=message)


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
    async with backfill_lock(settings.lock_path):
        pool_kwargs = theta_pool_kwargs(settings.worker_count, concurrency=settings.api_rate_limit)
        async with Pool(**pool_kwargs) as pool:
            return list(await pool.starmap(process_task, payloads))
