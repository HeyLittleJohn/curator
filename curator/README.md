# ThetaData Historical Data Backfill System

High-performance Python application for backfilling historical financial data from ThetaData API v3 into PostgreSQL.

## Features

- **Parallel Processing**: Uses `aiomultiprocess` with `uvloop` for maximum throughput
- **Full Data Support**: Equities, options (with all Greeks), and indices
- **Flexible Modes**: Full backfill or incremental (skip existing data)
- **OOP Design**: Abstract base class with customizable downloaders per asset type

## Installation

```bash
pip install -e .
```

## Configuration

All settings can be configured via environment variables or CLI arguments:

| Variable | CLI Flag | Description |
|----------|----------|-------------|
| `THETADATA_DATABASE_URL` | `--database-url` | PostgreSQL connection string |
| `THETADATA_API_BASE_URL` | `--api-url` | ThetaData API URL (default: localhost:25503) |
| `THETADATA_WORKER_COUNT` | `--workers` | Number of worker processes |

## Quick Start

1. **Set up PostgreSQL** and run migrations:
   ```bash
   export THETADATA_DATABASE_URL="postgresql://user:pass@localhost:5432/thetadata"
   thetadata-backfill migrate
   ```

2. **Start ThetaData client** (per ThetaData documentation)

3. **Run backfill**:
   ```bash
   # Full backfill of AAPL equities
   thetadata-backfill equities --symbols AAPL --mode full

   # Incremental backfill of options with all Greeks
   thetadata-backfill options --symbols SPY --mode incremental

   # Backfill all data types
   thetadata-backfill all --start-date 2024-01-01
   ```

## Commands

- `thetadata-backfill equities` - Backfill equity data (OHLC, trades, quotes, EOD)
- `thetadata-backfill options` - Backfill options with all Greeks
- `thetadata-backfill indices` - Backfill index data
- `thetadata-backfill all` - Full backfill of all data types
- `thetadata-backfill status` - Show database record counts
- `thetadata-backfill migrate` - Run database migrations

## Greeks Supported

All first-order and second-order Greeks:
- First-order: delta, gamma, theta, vega, rho, epsilon, lambda
- Second-order: vanna, charm, vomma, veta, color, zomma, speed, ultima
