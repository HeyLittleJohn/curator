# ThetaData backfill

ThetaData lives in the curator package and is invoked as:

```bash
uv run curator theta --help
```

Commands are registered the same way as `silver`. Backfill, migrations, and status print `not implemented` until the schema and client tickets fill them in.

The Python backfill uses curator's aiomultiprocess pool: one asyncio event loop per worker process, and one account-wide limit of 4 in-flight HTTP requests. The Rust crate is parked in `_shelved_rust/` and is not part of the build or CI.
