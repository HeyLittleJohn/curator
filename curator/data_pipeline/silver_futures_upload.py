"""Silver futures MBO data upload script.

This module provides functionality to decompress Databento zstd-compressed CSV files
and upload the MBO (Market By Order) data to PostgreSQL.

Example usage:
    uv run python -m curator.data_pipeline.silver_futures_upload --input-dir /path/to/data
"""

import asyncio
import csv
import io
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
import zstandard
from sqlalchemy.dialects.postgresql import insert

from curator.db_tools.schemas import SilverFuturesMBO
from curator.proj_constants import POSTGRES_BATCH_MAX, async_session_maker, log

app = typer.Typer()


def decompress_zst_file_streaming(file_path: Path) -> list[dict]:
    """Decompress a zstd-compressed CSV file using streaming and return parsed rows.

    Uses streaming decompression which is required for Databento zstd files.

    Args:
        file_path: Path to the .zst compressed file.

    Returns:
        List of dictionaries, one per CSV row.
    """
    dctx = zstandard.ZstdDecompressor()
    rows = []
    with open(file_path, "rb") as f:
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8")
            csv_reader = csv.DictReader(text_stream)
            for row in csv_reader:
                rows.append(row)
    return rows


def parse_csv_content(content: str) -> list[dict]:
    """Parse CSV content into a list of dictionaries.

    Args:
        content: CSV string content with header row.

    Returns:
        List of dictionaries, one per row, with keys from header.
    """
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def parse_iso8601(ts_str: str) -> datetime:
    """Parse ISO 8601 timestamp string to datetime.

    Args:
        ts_str: ISO 8601 formatted timestamp string (e.g., "2024-02-05T00:02:11.569482710Z").

    Returns:
        Python datetime object.
    """
    # Handle nanosecond precision by truncating to microseconds
    if "." in ts_str:
        base, frac = ts_str.rsplit(".", 1)
        frac = frac.rstrip("Z")
        # Truncate to 6 digits for microseconds
        frac = frac[:6].ljust(6, "0")
        ts_str = f"{base}.{frac}+00:00"
    else:
        ts_str = ts_str.rstrip("Z") + "+00:00"
    return datetime.fromisoformat(ts_str)


def parse_price_to_fixed_point(price_str: str) -> int | None:
    """Convert decimal price string to fixed-point integer (1e9 scale).

    Args:
        price_str: Price as decimal string (e.g., "22.635000000").

    Returns:
        Price as integer in fixed-point format, or None if empty.
    """
    if not price_str:
        return None
    # Convert decimal to fixed-point integer (multiply by 1e9)
    price_float = float(price_str)
    return int(price_float * 1_000_000_000)


def transform_row(row: dict) -> dict:
    """Transform a raw CSV row into a format suitable for database insertion.

    Args:
        row: Dictionary from CSV with string values.

    Returns:
        Dictionary with properly typed values for SilverFuturesMBO model.
    """
    return {
        "ts_recv": parse_iso8601(row["ts_recv"]),
        "ts_event": parse_iso8601(row["ts_event"]),
        "rtype": int(row["rtype"]),
        "publisher_id": int(row["publisher_id"]),
        "instrument_id": int(row["instrument_id"]),
        "action": row["action"],
        "side": row["side"] if row["side"] else None,
        "price": parse_price_to_fixed_point(row["price"]),
        "size": int(row["size"]),
        "channel_id": int(row["channel_id"]) if row["channel_id"] else None,
        "order_id": int(row["order_id"]),
        "flags": int(row["flags"]) if row["flags"] else None,
        "ts_in_delta": int(row["ts_in_delta"]) if row["ts_in_delta"] else None,
        "sequence": int(row["sequence"]) if row["sequence"] else None,
        "symbol": row["symbol"],
    }


async def upload_batch(data: list[dict]) -> int:
    """Upload a batch of MBO records to the database.

    Args:
        data: List of dictionaries containing MBO record data.

    Returns:
        Number of records inserted/updated.
    """
    if not data:
        return 0

    async with async_session_maker() as session:
        stmt = insert(SilverFuturesMBO).values(data)
        stmt = stmt.on_conflict_do_nothing(constraint="uq_silver_mbo_event")
        result = await session.execute(stmt)
        await session.commit()
        return result.rowcount if result.rowcount else len(data)


async def process_file(file_path: Path) -> tuple[str, int, int]:
    """Process a single zst file: decompress, parse, and upload to database.

    Args:
        file_path: Path to the .zst file to process.

    Returns:
        Tuple of (filename, total_rows, inserted_rows).
    """
    log.info(f"Processing file: {file_path.name}")

    try:
        # Decompress and parse CSV using streaming
        rows = decompress_zst_file_streaming(file_path)
        total_rows = len(rows)

        if total_rows == 0:
            log.warning(f"No rows found in {file_path.name}")
            return (file_path.name, 0, 0)

        # Transform and upload in batches
        inserted = 0
        batch_size = min(POSTGRES_BATCH_MAX, 10000)

        for i in range(0, total_rows, batch_size):
            batch = rows[i : i + batch_size]
            transformed = [transform_row(row) for row in batch]
            count = await upload_batch(transformed)
            inserted += count

            if (i + batch_size) % 50000 == 0:
                log.info(f"  Processed {i + batch_size}/{total_rows} rows from {file_path.name}")

        log.info(f"Completed {file_path.name}: {inserted}/{total_rows} rows inserted")
        return (file_path.name, total_rows, inserted)

    except Exception as e:
        log.error(f"Error processing {file_path.name}: {e}")
        raise


async def process_directory(
    input_dir: Path,
    limit: Optional[int] = None,
    skip_large: bool = False,
    max_size_mb: float = 100.0,
) -> dict:
    """Process all zst files in a directory.

    Args:
        input_dir: Path to directory containing .zst files.
        limit: Maximum number of files to process (None for all).
        skip_large: If True, skip files larger than max_size_mb.
        max_size_mb: Maximum file size in MB when skip_large is True.

    Returns:
        Dictionary with processing statistics.
    """
    zst_files = sorted(input_dir.glob("*.zst"))

    if skip_large:
        max_bytes = max_size_mb * 1024 * 1024
        zst_files = [f for f in zst_files if f.stat().st_size <= max_bytes]

    if limit:
        zst_files = zst_files[:limit]

    log.info(f"Found {len(zst_files)} zst files to process")

    results = {
        "files_processed": 0,
        "files_failed": 0,
        "total_rows": 0,
        "inserted_rows": 0,
        "failed_files": [],
    }

    for file_path in zst_files:
        try:
            filename, total, inserted = await process_file(file_path)
            results["files_processed"] += 1
            results["total_rows"] += total
            results["inserted_rows"] += inserted
        except Exception as e:
            results["files_failed"] += 1
            results["failed_files"].append((file_path.name, str(e)))

    return results


@app.command()
def upload(
    input_dir: str = typer.Option(
        ...,
        "--input-dir",
        "-i",
        help="Directory containing .zst compressed CSV files",
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        "-l",
        help="Maximum number of files to process",
    ),
    skip_large: bool = typer.Option(
        False,
        "--skip-large",
        help="Skip files larger than --max-size-mb",
    ),
    max_size_mb: float = typer.Option(
        100.0,
        "--max-size-mb",
        help="Maximum file size in MB when --skip-large is set",
    ),
) -> None:
    """Upload silver futures MBO data from zstd-compressed CSV files.

    Args:
        input_dir: Directory containing .zst files.
        limit: Optional limit on number of files to process.
        skip_large: Whether to skip large files.
        max_size_mb: Size threshold for --skip-large.
    """
    input_path = Path(input_dir)

    if not input_path.exists():
        typer.echo(f"Error: Directory {input_dir} does not exist")
        raise typer.Exit(1)

    if not input_path.is_dir():
        typer.echo(f"Error: {input_dir} is not a directory")
        raise typer.Exit(1)

    typer.echo(f"Processing files from: {input_path}")

    results = asyncio.run(
        process_directory(
            input_path,
            limit=limit,
            skip_large=skip_large,
            max_size_mb=max_size_mb,
        )
    )

    typer.echo("\n--- Upload Summary ---")
    typer.echo(f"Files processed: {results['files_processed']}")
    typer.echo(f"Files failed: {results['files_failed']}")
    typer.echo(f"Total rows: {results['total_rows']}")
    typer.echo(f"Inserted rows: {results['inserted_rows']}")

    if results["failed_files"]:
        typer.echo("\nFailed files:")
        for fname, error in results["failed_files"]:
            typer.echo(f"  - {fname}: {error}")


@app.command()
def decompress_only(
    input_dir: str = typer.Option(
        ...,
        "--input-dir",
        "-i",
        help="Directory containing .zst compressed CSV files",
    ),
    output_dir: Optional[str] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Output directory for decompressed files (default: same as input)",
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        "-l",
        help="Maximum number of files to decompress",
    ),
) -> None:
    """Decompress zst files without uploading to database.

    Args:
        input_dir: Directory containing .zst files.
        output_dir: Optional output directory for decompressed files.
        limit: Optional limit on number of files to decompress.
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir) if output_dir else input_path

    if not input_path.exists():
        typer.echo(f"Error: Directory {input_dir} does not exist")
        raise typer.Exit(1)

    output_path.mkdir(parents=True, exist_ok=True)

    zst_files = sorted(input_path.glob("*.zst"))
    if limit:
        zst_files = zst_files[:limit]

    typer.echo(f"Decompressing {len(zst_files)} files...")

    for file_path in zst_files:
        out_file = output_path / file_path.stem  # Remove .zst extension
        rows = decompress_zst_file_streaming(file_path)
        # Write as CSV
        if rows:
            import csv as csv_mod

            with open(out_file, "w", newline="") as f:
                writer = csv_mod.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
        typer.echo(f"  Decompressed: {file_path.name} -> {out_file.name}")

    typer.echo(f"\nDecompressed {len(zst_files)} files to {output_path}")


if __name__ == "__main__":
    app()
