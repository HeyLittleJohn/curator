"""Spawn-safe helpers for ThetaData client tests.

aiomultiprocess uses the spawn start method, so pool workers import this module
instead of a test function closed over the test module.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Protocol

from curator.thetadata.client import ThetaDataClient
from curator.thetadata.config import Settings
from curator.thetadata.limits import backfill_lock


class _Flag(Protocol):
    """The slice of a multiprocessing Event this helper uses."""

    def set(self) -> None:
        """Mark the flag set."""

    def wait(self, timeout: float | None = None) -> bool:
        """Block until the flag is set.

        Args:
            timeout: Seconds to wait. ``None`` waits without a deadline.

        Returns:
            True when the flag is set.
        """


async def fetch_expiration_pid(base_url: str) -> int:
    """Request one expiration list from a fake terminal.

    Args:
        base_url: Terminal base URL, including the ``/v3`` prefix.

    Returns:
        The process id that handled the request.
    """
    settings = Settings(
        _env_file=None,
        api_base_url=base_url,
        api_max_retries=0,
        api_backoff_base=0.0,
        api_backoff_cap=0.0,
        api_timeout=30,
    )
    async with ThetaDataClient(settings) as client:
        rows = [row async for row in client.list_expirations("SPXW")]
    if len(rows) != 1 or rows[0].symbol != "SPXW":
        raise RuntimeError("fake terminal returned an unexpected expiration list")
    return os.getpid()


def hold_backfill_lock(path: str, ready: _Flag, release: _Flag) -> None:
    """Hold the machine lock until ``release`` is set.

    Args:
        path: Lock file path.
        ready: Set once the lock is held.
        release: Waited on before the lock is released.
    """

    async def _hold() -> None:
        async with backfill_lock(Path(path)):
            ready.set()
            await asyncio.to_thread(release.wait, 30)

    asyncio.run(_hold())
