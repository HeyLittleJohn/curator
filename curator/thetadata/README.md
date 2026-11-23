# ThetaData backfill

ThetaData lives in the curator package and is invoked as:

```bash
uv run curator theta --help
```

Backfill commands print `not implemented` until the client tickets fill them in. Schema migrations are a separate Alembic chain from the repository-root history (head `34551fa734e8`):

```bash
uv run curator theta db upgrade
uv run curator theta db current
uv run curator theta db downgrade base
uv run curator theta db upgrade --sql
```

The URL comes from `--database-url` or `THETADATA_DATABASE_URL` and is not printed. Tables live in schema `theta` inside the `markets` database. `theta.alembic_version_theta` is the only version table this chain writes.

v3 timestamps are zone-less America/New_York wall times. `curator.thetadata.time.parse_thetadata_timestamp` applies DST and returns a timestamptz value. Strikes are `numeric(12, 3)`, not floats. `quotes` is partitioned by Eastern month on `ts`; `add_partitions_ahead` creates later months. `underlying_price` is stored for greeks and IV only and is not the SPX spot source.

The Python backfill uses curator's aiomultiprocess pool: one uvloop event loop per worker process, and child concurrency 1. In-flight HTTP requests share one `multiprocessing` semaphore created in the parent's spawn context and installed in every worker. The default cap is 4 and can be lowered, never raised. A lock file (`THETADATA_LOCK_PATH`, default `/tmp/curator-theta.lock`) stops a second backfill or nightly on the same machine from starting and doubling that cap. The client streams CSV or NDJSON for Options Standard endpoints only: option expirations, strikes, contracts, quote, OHLC, open interest, EOD, and first-order greeks with IV. `greeks/all`, second- and third-order greeks, and `trade_greeks` raise a subscription error before any request is sent. The Rust crate is parked in `_shelved_rust/` and is not part of the build or CI.
