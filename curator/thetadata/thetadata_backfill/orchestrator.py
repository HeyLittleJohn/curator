"""Backfill orchestrator for coordinating parallel downloads."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from thetadata_backfill.client import ThetaDataClient
from thetadata_backfill.config import BackfillMode, Settings
from thetadata_backfill.downloaders import BackfillResult
from thetadata_backfill.workers import WorkerPool, WorkerResult

logger = logging.getLogger(__name__)
console = Console()


@dataclass
class BackfillJob:
    """Definition of a backfill job."""
    
    asset_type: str  # equity, option, index
    data_types: list[str]  # ohlc, trade, quote, eod, greeks, etc.
    symbols: Optional[list[str]] = None  # None = all symbols
    start_date: Optional[date] = None  # None = earliest available
    end_date: Optional[date] = None  # None = today
    mode: BackfillMode = BackfillMode.FULL


@dataclass
class BackfillStats:
    """Statistics for a backfill run."""
    
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    total_records: int = 0
    total_duration: float = 0.0
    errors: list[str] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_tasks == 0:
            return 0.0
        return (self.total_tasks - self.failed_tasks) / self.total_tasks * 100


class Orchestrator:
    """Orchestrates parallel backfill operations.
    
    The orchestrator:
    - Discovers symbols and date ranges
    - Generates tasks for the worker pool
    - Monitors progress and collects results
    - Reports statistics
    """
    
    def __init__(self, settings: Settings):
        """Initialize orchestrator.
        
        Args:
            settings: Application settings.
        """
        self.settings = settings
        self._stats = BackfillStats()
    
    async def discover_symbols(
        self,
        asset_type: str,
    ) -> list[str]:
        """Discover available symbols for an asset type.
        
        Args:
            asset_type: Type of asset (equity, option, index).
            
        Returns:
            List of symbols.
        """
        async with ThetaDataClient(self.settings) as client:
            if asset_type == "equity":
                return await client.get_stock_symbols()
            elif asset_type == "option":
                # Options use underlying symbols
                return await client.get_stock_symbols()
            elif asset_type == "index":
                return await client.get_index_symbols()
            else:
                raise ValueError(f"Unknown asset type: {asset_type}")
    
    async def discover_date_range(
        self,
        asset_type: str,
        symbol: str,
        data_type: str = "quote",
    ) -> tuple[date, date]:
        """Discover available date range for a symbol.
        
        Args:
            asset_type: Type of asset.
            symbol: Symbol to check.
            data_type: Type of data.
            
        Returns:
            Tuple of (earliest_date, latest_date).
        """
        async with ThetaDataClient(self.settings) as client:
            if asset_type == "equity":
                dates = await client.get_stock_dates(symbol, data_type)
            elif asset_type == "index":
                dates = await client.get_index_dates(symbol)
            else:
                dates = await client.get_stock_dates(symbol, data_type)
            
            if not dates:
                today = date.today()
                return (today, today)
            
            return (min(dates), max(dates))
    
    async def run_backfill(
        self,
        job: BackfillJob,
    ) -> BackfillStats:
        """Run a backfill job.
        
        Args:
            job: Backfill job definition.
            
        Returns:
            Statistics for the backfill run.
        """
        import time
        
        start_time = time.time()
        self._stats = BackfillStats()
        
        console.print(f"\n[bold blue]Starting {job.asset_type} backfill[/bold blue]")
        console.print(f"Mode: {job.mode.value}")
        console.print(f"Data types: {', '.join(job.data_types)}")
        
        # Discover symbols if not specified
        symbols = job.symbols
        if symbols is None:
            console.print("Discovering symbols...")
            symbols = await self.discover_symbols(job.asset_type)
            console.print(f"Found {len(symbols)} symbols")
        
        # Determine date range
        if job.start_date is None or job.end_date is None:
            console.print("Determining date range...")
            if symbols:
                earliest, latest = await self.discover_date_range(
                    job.asset_type, symbols[0]
                )
                start_date = job.start_date or earliest
                end_date = job.end_date or latest
            else:
                start_date = job.start_date or date.today()
                end_date = job.end_date or date.today()
        else:
            start_date = job.start_date
            end_date = job.end_date
        
        console.print(f"Date range: {start_date} to {end_date}")
        
        # Calculate total tasks
        total_tasks = len(symbols) * len(job.data_types)
        self._stats.total_tasks = total_tasks
        
        # Create progress display
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task(
                f"[cyan]Backfilling {job.asset_type}...",
                total=total_tasks,
            )
            
            # Start worker pool and submit tasks
            with WorkerPool(self.settings) as pool:
                # Submit all tasks
                pending_tasks = 0
                for symbol in symbols:
                    for data_type in job.data_types:
                        pool.create_task(
                            asset_type=job.asset_type,
                            data_type=data_type,
                            symbol=symbol,
                            start_date=start_date,
                            end_date=end_date,
                            mode=job.mode,
                        )
                        pending_tasks += 1
                
                # Collect results
                while pending_tasks > 0:
                    result = pool.get_result(timeout=1.0)
                    if result is not None:
                        pending_tasks -= 1
                        self._process_result(result)
                        progress.advance(task_id)
        
        self._stats.total_duration = time.time() - start_time
        
        # Print summary
        self._print_summary()
        
        return self._stats
    
    def _process_result(self, result: WorkerResult) -> None:
        """Process a worker result.
        
        Args:
            result: Result from worker.
        """
        self._stats.completed_tasks += 1
        
        if result.error:
            self._stats.failed_tasks += 1
            self._stats.errors.append(f"{result.task_id}: {result.error}")
            logger.error(f"Task {result.task_id} failed: {result.error}")
        elif result.result:
            self._stats.total_records += result.result.records_inserted
            if result.result.errors:
                for error in result.result.errors:
                    self._stats.errors.append(f"{result.result.symbol}: {error}")
    
    def _print_summary(self) -> None:
        """Print backfill summary."""
        console.print("\n[bold green]Backfill Complete![/bold green]")
        
        table = Table(title="Backfill Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Tasks", str(self._stats.total_tasks))
        table.add_row("Completed", str(self._stats.completed_tasks))
        table.add_row("Failed", str(self._stats.failed_tasks))
        table.add_row("Success Rate", f"{self._stats.success_rate:.1f}%")
        table.add_row("Records Inserted", f"{self._stats.total_records:,}")
        table.add_row("Duration", f"{self._stats.total_duration:.1f}s")
        
        console.print(table)
        
        if self._stats.errors:
            console.print(f"\n[yellow]Errors ({len(self._stats.errors)}):[/yellow]")
            for error in self._stats.errors[:10]:  # Show first 10 errors
                console.print(f"  • {error}")
            if len(self._stats.errors) > 10:
                console.print(f"  ... and {len(self._stats.errors) - 10} more")
