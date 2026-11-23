# Shelved: thetadata-backfill-rs

This Rust crate is parked. The ThetaData backfill runs in Python on curator's aiomultiprocess pool (one asyncio event loop per worker, with one account-wide limit of 4 in-flight HTTP requests). The crate is not built or tested in CI. Do not delete it.
