"""Account-wide ThetaData request cap and the per-machine backfill lock.

Theta Terminal allows four concurrent HTTP requests for the whole Options
Standard account. aiomultiprocess starts workers with the spawn context, so a
semaphore created in that same context is passed through the pool initializer
and installed in every child. Acquire and release hit one kernel semaphore,
which is what keeps the cap shared across processes.

A separate exclusive lock file stops a second backfill or nightly on this
machine from creating its own semaphore and doubling the cap. Workers do not
take that lock; the parent holds it for the whole run.
"""

from __future__ import annotations

import asyncio
import errno
import fcntl
import os
import stat
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from multiprocessing.synchronize import Semaphore as MpSemaphore
from pathlib import Path

from aiomultiprocess.core import get_context

from curator.thetadata.config import OPTIONS_STANDARD_CONCURRENCY
from curator.thetadata.exceptions import ThetaDataMachineLockError

ACCOUNT_CONCURRENCY = OPTIONS_STANDARD_CONCURRENCY

_request_slots: MpSemaphore | None = None
_lock_depth = 0
_lock_file: int | None = None
_lock_path: Path | None = None


def clamp_concurrency(limit: int) -> int:
    """Return a request cap that stays inside the Options Standard account limit.

    Args:
        limit: Requested number of in-flight HTTP requests.

    Returns:
        ``limit`` when it is between 1 and 4 inclusive.

    Raises:
        ValueError: ``limit`` is below 1 or above 4.
    """
    if limit < 1:
        raise ValueError("ThetaData concurrency must be at least 1")
    if limit > ACCOUNT_CONCURRENCY:
        raise ValueError(
            f"ThetaData concurrency {limit} exceeds the Options Standard account cap of {ACCOUNT_CONCURRENCY}"
        )
    return limit


def reset_request_slots() -> None:
    """Forget this process's handle to the shared request semaphore.

    Tests call this between cases. It does not change a semaphore other
    processes are already using.
    """
    global _request_slots
    _request_slots = None


def request_slots(limit: int | None = None) -> MpSemaphore:
    """Return the process-wide ThetaData request semaphore.

    The first call creates a spawn-context semaphore. Later calls in this
    process reuse it so a pool and a client cannot each invent a cap of 4.

    Args:
        limit: Cap to use when this process has not created the semaphore yet.
            ``None`` uses 4. Ignored once the semaphore exists, including in a
            worker where the parent installed the semaphore.

    Returns:
        The shared semaphore.
    """
    global _request_slots
    if _request_slots is None:
        chosen = clamp_concurrency(ACCOUNT_CONCURRENCY if limit is None else limit)
        _request_slots = get_context().Semaphore(chosen)
    return _request_slots


def install_request_slots(slots: MpSemaphore) -> None:
    """Install the parent's request semaphore inside a worker process.

    Args:
        slots: Semaphore created in the parent and passed through pool initargs.
    """
    global _request_slots
    _request_slots = slots


def reset_machine_lock() -> None:
    """Drop a machine lock held by this process.

    Tests call this so a failed case cannot leave the lock held.
    """
    global _lock_depth, _lock_file, _lock_path
    if _lock_file is not None:
        fcntl.flock(_lock_file, fcntl.LOCK_UN)
        os.close(_lock_file)
    _lock_file = None
    _lock_path = None
    _lock_depth = 0


@asynccontextmanager
async def account_request_slot(limit: int | None = None) -> AsyncGenerator[None, None]:
    """Hold one account-wide ThetaData HTTP slot until the response is consumed.

    Acquire runs in a thread so a full gate does not block the event loop.
    The slot stays held across ``yield``, which covers the whole streamed body.

    Args:
        limit: Cap used if this process has not created the semaphore yet.

    Yields:
        None while the slot is held.
    """
    slots = request_slots(limit)
    await asyncio.to_thread(slots.acquire)
    try:
        yield
    finally:
        slots.release()


def _unsafe_lock_path() -> ThetaDataMachineLockError:
    """Build the error raised when the lock path is not safe to open.

    Returns:
        An error whose message does not include the path, credentials, or a DSN.
    """
    return ThetaDataMachineLockError(
        "The theta backfill lock must be a regular file owned by this user. "
        "Refusing to open a symlink or another user's file."
    )


def _open_lock_file(path: Path) -> int:
    """Open the lock file without following a symlink.

    Args:
        path: Lock file path. The parent directory must already exist.

    Returns:
        A file descriptor for a regular file owned by this user.

    Raises:
        ThetaDataMachineLockError: The path is a symlink, not a regular file,
            or owned by another user.
    """
    flags = os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    try:
        file_handle = os.open(path, flags, 0o600)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EISDIR}:
            raise _unsafe_lock_path() from None
        raise
    try:
        info = os.fstat(file_handle)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise _unsafe_lock_path()
        os.fchmod(file_handle, 0o600)
    except OSError, ThetaDataMachineLockError:
        os.close(file_handle)
        raise
    return file_handle


def _lock_error() -> ThetaDataMachineLockError:
    """Build the error raised when the machine lock is already held.

    Returns:
        An error whose message does not include paths, credentials, or DSNs.
    """
    return ThetaDataMachineLockError(
        "Another curator theta backfill or nightly is already running on this machine. "
        "Refusing to start so the account-wide limit of "
        f"{ACCOUNT_CONCURRENCY} in-flight requests is not doubled."
    )


@contextmanager
def _machine_lock(path: Path) -> Generator[None, None, None]:
    """Hold the exclusive machine lock, reentering in this process only.

    Args:
        path: Lock file. The parent directory is created when missing.

    Yields:
        None while the lock is held.

    Raises:
        ThetaDataMachineLockError: Another process holds the lock, or this
            process already holds a lock for a different path.
    """
    global _lock_depth, _lock_file, _lock_path
    if _lock_depth > 0:
        if _lock_path != Path(path):
            raise _lock_error()
        _lock_depth += 1
        try:
            yield
        finally:
            _lock_depth -= 1
        return

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    file_handle = _open_lock_file(path)
    try:
        fcntl.flock(file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(file_handle)
        raise _lock_error() from exc
    os.ftruncate(file_handle, 0)
    os.write(file_handle, str(os.getpid()).encode("ascii"))
    _lock_file = file_handle
    _lock_path = Path(path)
    _lock_depth = 1
    try:
        yield
    finally:
        _lock_depth -= 1
        if _lock_depth == 0:
            fcntl.flock(file_handle, fcntl.LOCK_UN)
            os.close(file_handle)
            _lock_file = None
            _lock_path = None


@asynccontextmanager
async def backfill_lock(path: Path) -> AsyncGenerator[None, None]:
    """Hold the per-machine backfill lock for one theta run.

    The same process may enter again (the orchestrator locks, then the pool
    helper locks). A second process gets ``ThetaDataMachineLockError``
    immediately instead of blocking.

    Args:
        path: Lock file path from settings.

    Yields:
        None while this run is the only theta backfill on the machine.
    """
    with _machine_lock(path):
        yield
