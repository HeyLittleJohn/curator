# ThetaData Backfill - Rust Implementation

High-performance ThetaData historical data backfill system written in Rust with Python bindings via PyO3/maturin.

## Features

- **Maximum Performance**: Tokio async runtime with parallel task processing
- **Full Data Support**: Equities, options (with all Greeks), and indices
- **Python Bindings**: Use as a Python module in larger projects
- **Same Database**: Compatible with the Python version's PostgreSQL schema

## Building

### Prerequisites
- Rust 1.70+
- Python 3.8+
- PostgreSQL

### Build Native Binary
```bash
cd thetadata-backfill-rs
cargo build --release
```

### Build Python Module with Maturin
```bash
pip install maturin
cd thetadata-backfill-rs
maturin develop --release
```

## CLI Usage

```bash
# Full backfill of equities
thetadata-backfill -d "postgresql://user:pass@localhost/db" equities --symbols AAPL,MSFT

# Incremental backfill of options with Greeks
thetadata-backfill -d $DATABASE_URL -m incremental options --symbols SPY

# Backfill indices
thetadata-backfill -d $DATABASE_URL indices --symbols SPX,VIX
```

## Python Usage

```python
from thetadata_backfill_rs import BackfillConfig, run_equity_backfill

config = BackfillConfig(
    database_url="postgresql://user:pass@localhost/db",
    worker_count=8,
    mode="incremental",
)

result = run_equity_backfill(
    config,
    symbols=["AAPL", "MSFT"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    data_types=["ohlc", "eod"],
)

print(f"Inserted {result.total_records} records in {result.duration_seconds}s")
print(f"Success rate: {result.success_rate}%")
```

## Greeks Supported

All first-order and second-order Greeks:
- **First-order**: delta, gamma, theta, vega, rho, epsilon, lambda
- **Second-order**: vanna, charm, vomma, veta, color, zomma, speed, ultima

## Performance Comparison

The Rust implementation is designed to be faster than the Python version due to:
- Zero-cost abstractions
- Native async I/O with Tokio
- Efficient memory usage with no GC pressure
- Parallel task execution without GIL constraints
