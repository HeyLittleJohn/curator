"""Scratch-Postgres checks for the progress queue.

Opt in with ``THETADATA_TEST_DATABASE_URL`` and ``pytest -m postgres``.
CI deselects this marker. Rows are synthetic. The URL is never printed.
"""

import os
import threading
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from curator.thetadata.migrate import run_migration, to_sync_database_url
from curator.thetadata.planner import run_plan
from curator.thetadata.progress import DONE, ORDER_OLDEST, PENDING, PostgresProgressStore
from curator.thetadata.secrets import scrub_secrets

pytestmark = pytest.mark.postgres


def _require_database() -> str:
    """Return the scratch database URL or skip.

    Returns:
        The URL. It is not printed.

    Raises:
        pytest.skip.Exception: When the variable is unset.
    """
    url = os.environ.get("THETADATA_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("THETADATA_TEST_DATABASE_URL is not set")
    return url


def _engine() -> Engine:
    """Open an engine without logging the URL.

    Returns:
        An engine with parameters hidden.

    Raises:
        pytest.fail.Exception: If connecting fails. The message is scrubbed.
    """
    url = to_sync_database_url(_require_database())
    try:
        engine = create_engine(url, pool_pre_ping=True, echo=False, hide_parameters=True)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except Exception as exc:
        pytest.fail(scrub_secrets(str(exc)))


def _reset(engine: Engine) -> None:
    """Drop a previous theta schema on the scratch database.

    Args:
        engine: Open engine.

    Returns:
        None.
    """
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS theta CASCADE"))


def test_claim_skip_locked_and_finished_rerun() -> None:
    """Two connections claim different rows, and a finished plan writes nothing."""
    _require_database()
    engine = _engine()
    expiration = date(2022, 5, 20)
    try:
        _reset(engine)
        run_migration("upgrade", "head", os.environ["THETADATA_TEST_DATABASE_URL"])
        with engine.begin() as connection:
            first = run_plan(
                [expiration],
                date(2022, 5, 16),
                date(2022, 5, 17),
                datasets=["quotes"],
                queue=PostgresProgressStore(connection),
            )
        assert len(first.jobs) == 2
        claimed: list[tuple[str, date, date]] = []
        barrier = threading.Barrier(2)

        def take() -> None:
            """Claim one pending job and commit it.

            Returns:
                None.
            """
            with engine.connect() as connection:
                barrier.wait()
                jobs = PostgresProgressStore(connection).claim(1)
                connection.commit()
                if jobs:
                    claimed.append(jobs[0].key())

        threads = [threading.Thread(target=take) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert len(claimed) == 2
        assert len(set(claimed)) == 2

        with engine.begin() as connection:
            store = PostgresProgressStore(connection)
            for job in first.jobs:
                store.mark(job, DONE, 1)
        with engine.begin() as connection:
            second = run_plan(
                [expiration],
                date(2022, 5, 16),
                date(2022, 5, 17),
                datasets=["quotes"],
                queue=PostgresProgressStore(connection),
            )
        assert second.jobs == ()
        assert second.counts.writes == 0
        assert second.counts.skipped_done == 2
    finally:
        _reset(engine)
        engine.dispose()


def test_claim_order_is_newest_first_and_oldest_reverses_it() -> None:
    """The claim statement returns the newest session first."""
    _require_database()
    engine = _engine()
    near = date(2022, 5, 20)
    far = date(2022, 6, 17)
    start = date(2022, 5, 16)
    end = date(2022, 5, 18)
    try:
        _reset(engine)
        run_migration("upgrade", "head", os.environ["THETADATA_TEST_DATABASE_URL"])
        with engine.begin() as connection:
            store = PostgresProgressStore(connection)
            run_plan(
                [near, far],
                start,
                end,
                datasets=["quotes", "eod"],
                queue=store,
                materialize=False,
            )
            first = store.claim(2)
            assert [job.trade_date for job in first] == [end, end]
            rest = store.claim(20)
            claimed = first + rest
            keys = [(job.trade_date, job.expiration, job.dataset) for job in claimed]
            assert len(keys) == 12
            assert keys == sorted(keys, reverse=True)
            for job in claimed:
                store.mark(job, PENDING, 0)
            oldest = store.claim(20, order=ORDER_OLDEST)
            old_keys = [(job.trade_date, job.expiration, job.dataset) for job in oldest]
            assert old_keys == sorted(old_keys)
            assert old_keys == list(reversed(keys))
    finally:
        _reset(engine)
        engine.dispose()
