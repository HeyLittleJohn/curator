"""Worker pool using aiomultiprocess with uvloop for maximum parallelism."""

import asyncio
import logging
import multiprocessing as mp
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

try:
    import uvloop
    HAS_UVLOOP = True
except ImportError:
    HAS_UVLOOP = False

from thetadata_backfill.client import ThetaDataClient
from thetadata_backfill.config import BackfillMode, Settings
from thetadata_backfill.db import DatabaseManager
from thetadata_backfill.downloaders import BackfillResult

logger = logging.getLogger(__name__)


@dataclass
class WorkerTask:
    """Task to be processed by a worker."""
    
    task_id: str
    asset_type: str  # "equity", "option", "index"
    data_type: str   # "ohlc", "trade", "quote", "eod", "greeks", etc.
    symbol: str
    start_date: date
    end_date: date
    mode: BackfillMode
    extra: dict[str, Any]


@dataclass
class WorkerResult:
    """Result from a worker task."""
    
    task_id: str
    result: Optional[BackfillResult]
    error: Optional[str]


def install_uvloop() -> None:
    """Install uvloop as the event loop policy if available."""
    if HAS_UVLOOP:
        uvloop.install()
        logger.info("Using uvloop event loop")
    else:
        logger.warning("uvloop not available, using default asyncio event loop")


async def process_task(
    task: WorkerTask,
    settings: Settings,
) -> WorkerResult:
    """Process a single backfill task.
    
    This runs in a worker process with its own event loop and connections.
    
    Args:
        task: Task to process.
        settings: Application settings.
        
    Returns:
        Result of the task.
    """
    from thetadata_backfill.downloaders import (
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
    
    # Map of (asset_type, data_type) -> downloader class
    DOWNLOADER_MAP = {
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
    
    downloader_class = DOWNLOADER_MAP.get((task.asset_type, task.data_type))
    if downloader_class is None:
        return WorkerResult(
            task_id=task.task_id,
            result=None,
            error=f"Unknown asset/data type: {task.asset_type}/{task.data_type}",
        )
    
    try:
        # Initialize connections for this task
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
        
        return WorkerResult(
            task_id=task.task_id,
            result=result,
            error=None,
        )
        
    except Exception as e:
        logger.exception(f"Error processing task {task.task_id}")
        return WorkerResult(
            task_id=task.task_id,
            result=None,
            error=str(e),
        )


def worker_entry(
    task_queue: mp.Queue,
    result_queue: mp.Queue,
    settings_dict: dict[str, Any],
) -> None:
    """Worker process entry point.
    
    This is the function that runs in each worker process.
    It sets up uvloop and processes tasks from the queue.
    
    Args:
        task_queue: Queue to receive tasks from.
        result_queue: Queue to send results to.
        settings_dict: Settings as a dictionary (for pickling).
    """
    # Install uvloop before creating event loop
    install_uvloop()
    
    # Recreate settings from dict
    settings = Settings(**settings_dict)
    
    async def run_worker():
        while True:
            try:
                task = task_queue.get(timeout=1)
            except Exception:
                continue
            
            if task is None:
                # Shutdown signal
                break
            
            result = await process_task(task, settings)
            result_queue.put(result)
    
    asyncio.run(run_worker())


class WorkerPool:
    """Pool of worker processes for parallel backfill.
    
    Each worker has its own:
    - uvloop event loop
    - HTTP client with connection pool
    - Database connection pool
    """
    
    def __init__(self, settings: Settings):
        """Initialize worker pool.
        
        Args:
            settings: Application settings.
        """
        self.settings = settings
        self.worker_count = settings.effective_worker_count
        self._task_queue: Optional[mp.Queue] = None
        self._result_queue: Optional[mp.Queue] = None
        self._workers: list[mp.Process] = []
        self._task_counter = 0
    
    def start(self) -> None:
        """Start the worker pool."""
        self._task_queue = mp.Queue()
        self._result_queue = mp.Queue()
        
        # Serialize settings for workers
        settings_dict = {
            "api_base_url": self.settings.api_base_url,
            "api_timeout": self.settings.api_timeout,
            "api_max_retries": self.settings.api_max_retries,
            "api_rate_limit": self.settings.api_rate_limit,
            "database_url": str(self.settings.database_url) if self.settings.database_url else None,
            "db_pool_min_size": self.settings.db_pool_min_size,
            "db_pool_max_size": self.settings.db_pool_max_size,
            "batch_size": self.settings.batch_size,
            "backfill_mode": self.settings.backfill_mode.value,
        }
        
        for i in range(self.worker_count):
            worker = mp.Process(
                target=worker_entry,
                args=(self._task_queue, self._result_queue, settings_dict),
                name=f"thetadata-worker-{i}",
            )
            worker.start()
            self._workers.append(worker)
        
        logger.info(f"Started {self.worker_count} worker processes")
    
    def stop(self) -> None:
        """Stop the worker pool."""
        if self._task_queue is None:
            return
        
        # Send shutdown signals
        for _ in self._workers:
            self._task_queue.put(None)
        
        # Wait for workers to finish
        for worker in self._workers:
            worker.join(timeout=10)
            if worker.is_alive():
                worker.terminate()
        
        self._workers = []
        self._task_queue = None
        self._result_queue = None
        
        logger.info("Worker pool stopped")
    
    def submit(self, task: WorkerTask) -> None:
        """Submit a task to the worker pool.
        
        Args:
            task: Task to process.
        """
        if self._task_queue is None:
            raise RuntimeError("Worker pool not started")
        
        self._task_queue.put(task)
    
    def create_task(
        self,
        asset_type: str,
        data_type: str,
        symbol: str,
        start_date: date,
        end_date: date,
        mode: Optional[BackfillMode] = None,
        **extra: Any,
    ) -> WorkerTask:
        """Create and submit a task.
        
        Args:
            asset_type: Asset type (equity, option, index).
            data_type: Data type (ohlc, trade, quote, eod, etc).
            symbol: Symbol to backfill.
            start_date: Start date.
            end_date: End date.
            mode: Backfill mode.
            **extra: Additional parameters.
            
        Returns:
            The created task.
        """
        self._task_counter += 1
        task = WorkerTask(
            task_id=f"task-{self._task_counter}",
            asset_type=asset_type,
            data_type=data_type,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            mode=mode or self.settings.backfill_mode,
            extra=extra,
        )
        self.submit(task)
        return task
    
    def get_result(self, timeout: Optional[float] = None) -> Optional[WorkerResult]:
        """Get a result from the worker pool.
        
        Args:
            timeout: Timeout in seconds.
            
        Returns:
            Result if available, None if timeout.
        """
        if self._result_queue is None:
            raise RuntimeError("Worker pool not started")
        
        try:
            return self._result_queue.get(timeout=timeout)
        except Exception:
            return None
    
    def __enter__(self) -> "WorkerPool":
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.stop()
