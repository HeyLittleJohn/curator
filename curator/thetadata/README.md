# ThetaData backfill

ThetaData lives in the curator package and is invoked as:

```bash
uv run curator theta --help
```

`curator theta plan` queues jobs. `curator theta backfill --start --end` and `curator theta load-day --date` download them. Quotes, OHLC, and first-order greeks use the job interval (60s for calendar DTE 0-5, 900s otherwise) inside a default window of 125 listed strikes on each side of ATM. ATM is the listed strike nearest the median put-call forward at the first bar of that response; the chain center is used when the open has no two-sided strike. `underlying_price` is not an input. `--strike-range all` keeps the full chain. Open interest and EOD always keep the full chain. Output is counts and `pg_total_relation_size` bytes, plus a projection from 2022-05-16 that scales those measurements by NYSE session count. Schema migrations are a separate Alembic chain from the repository-root history (head `34551fa734e8`):

```bash
uv run curator theta db upgrade
uv run curator theta db current
uv run curator theta db downgrade base
uv run curator theta db upgrade --sql
```

The URL comes from `--database-url` or `THETADATA_DATABASE_URL` and is not printed. Tables live in schema `theta` inside the `markets` database. `theta.alembic_version_theta` is the only version table this chain writes.

v3 timestamps are zone-less America/New_York wall times. `curator.thetadata.time.parse_thetadata_timestamp` applies DST and returns a timestamptz value. Strikes are `numeric(12, 3)`, not floats. `quotes` is partitioned by Eastern month on `ts`; `add_partitions_ahead` creates later months. `underlying_price` is stored for greeks and IV only and is not the SPX spot source.

The Python backfill uses curator's aiomultiprocess pool: one uvloop event loop per worker process, and child concurrency 1. In-flight HTTP requests share one `multiprocessing` semaphore created in the parent's spawn context and installed in every worker. The default cap is 4 and can be lowered, never raised. A lock file (`THETADATA_LOCK_PATH`, default `/tmp/curator-theta.lock`) stops a second backfill or nightly on the same machine from starting and doubling that cap. The client streams CSV or NDJSON for Options Standard endpoints only: option expirations, strikes, contracts, quote, OHLC, open interest, EOD, and first-order greeks with IV. `greeks/all`, second- and third-order greeks, and `trade_greeks` raise a subscription error before any request is sent. The Rust crate is parked in `_shelved_rust/` and is not part of the build or CI.

## Planner

```bash
uv run curator theta plan --start YYYY-MM-DD --end YYYY-MM-DD [--dataset quotes] [--max-dte 5]
```

The command prints counts only. Expirations come from `option/list/expirations?symbol=SPXW`. Sessions are NYSE trading days from `pandas_market_calendars`, including half days and excluding holidays. Nothing before 2022-05-16 is planned: an earlier start is clamped to that floor, and a range that ends before it is refused.

A job is one dataset, one session, and one listed expiration with calendar DTE of at least 0. Weekday rules are not used. Quotes, OHLC, and first-order greeks use 60-second bars when calendar DTE is 0 through 5 (the last 6 calendar days) and 900-second bars otherwise. The cutoff and both lengths are arguments on `run_plan`. Open interest and EOD are daily, store no interval, and cover the full strike chain. The default includes every listed expiration so far-dated positions stay in GEX. `--max-dte` is an optional cap.

The live expiration list has returned 2305 dates from 2012-06-01 through 2089-06-30. That last date is decades past listed SPXW series and is treated as a ThetaData placeholder. Expirations more than 10 years of 366 days after the plan end are counted as placeholders and dropped. That horizon is not a `max_dte` cap. Dates before the first session simply have negative DTE and produce no jobs.

`theta.progress` is the queue. Statuses are `pending`, `running`, `done`, `failed`, and `empty`. Resume skips `done` and `empty`. Planning a day whose jobs are all finished writes nothing and queues nothing. `failed` rows are requeued. `running` rows older than one hour are reset to `pending` on resume. `interval_s` is stored per job so a later cutoff change does not rewrite a finished row. Claiming is one statement: `UPDATE ... WHERE status = 'pending' ... FOR UPDATE SKIP LOCKED ... RETURNING`. Two worker processes cannot take the same job. The claimed record is a picklable `PlanJob` for the aiomultiprocess pool.

Without `THETADATA_DATABASE_URL` the queue stays in memory. With a URL, rows are written to `theta.progress` and the URL is not printed. Apply `curator theta db upgrade` before the first persisted plan.

## Nightly

```bash
uv run curator theta nightly [--lookback 10] [--wait]
```

The command plans today and any unfinished NYSE sessions in the lookback, then loads them. The default lookback is 10 sessions and never starts before 2022-05-16. Before 17:15 America/New_York it exits 0. `--wait` sleeps until then. Weekends and NYSE holidays exit 0. A finished window makes no requests and writes nothing. Job failures exit non-zero. The run holds the same lock file and uses the same worker pool and 4-request cap as `backfill`. Counts only. The systemd user timer and the enable steps are in `docs/theta-nightly.md`.
