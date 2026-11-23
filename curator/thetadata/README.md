# ThetaData backfill

ThetaData lives in the curator package and is invoked as:

```bash
uv run curator theta --help
```

Commands are registered the same way as `silver`. Backfill, migrations, and status print `not implemented` until the schema and client tickets fill them in.

The Python backfill uses curator's aiomultiprocess pool: one uvloop event loop per worker process, and child concurrency 1. In-flight HTTP requests share one `multiprocessing` semaphore created in the parent's spawn context and installed in every worker. The default cap is 4 and can be lowered, never raised. A lock file (`THETADATA_LOCK_PATH`, default `/tmp/curator-theta.lock`) stops a second backfill or nightly on the same machine from starting and doubling that cap. The client streams CSV or NDJSON for Options Standard endpoints only: option expirations, strikes, contracts, quote, OHLC, open interest, EOD, and first-order greeks with IV. `greeks/all`, second- and third-order greeks, and `trade_greeks` raise a subscription error before any request is sent. The Rust crate is parked in `_shelved_rust/` and is not part of the build or CI.
