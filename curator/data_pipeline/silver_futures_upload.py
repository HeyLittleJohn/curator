"""Silver futures MBO data upload script.

This module provides functionality to decompress Databento zstd-compressed CSV files
and upload the MBO (Market By Order) data to PostgreSQL.

Example usage:
    uv run python -m curator.data_pipeline.silver_futures_upload --input-dir /path/to/data
"""

import asyncio
import csv
import io
from collections.abc import Generator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import typer
import zstandard
from sqlalchemy.dialects.postgresql import insert

from curator.db_tools.schemas import SilverFuturesMBO, SilverFuturesMBOModel
from curator.proj_constants import POSTGRES_BATCH_MAX, async_session_maker, log

silver_app = typer.Typer(help="Silver futures MBO data upload commands")


def decompress_zst_file_streaming(file_path: Path) -> Generator[dict, None, None]:
    """Decompress a zstd-compressed CSV file, yielding rows as they are decompressed.

    Uses streaming decompression which is required for Databento zstd files.
    Rows are yielded one at a time so that callers can process them without
    holding the entire file in memory.

    Args:
        file_path: Path to the .zst compressed file.

    Yields:
        Dictionary for each CSV row.
    """
    dctx = zstandard.ZstdDecompressor()
    with open(file_path, "rb") as f:
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8")
            csv_reader = csv.DictReader(text_stream)
            for row in csv_reader:
                yield row


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


def transform_row(row: dict) -> dict:
    """Transform a raw CSV row into a format suitable for database insertion.

    Parses raw CSV string values and validates them through the
    SilverFuturesMBOModel pydantic model before returning a dict
    ready for database insertion.

    Args:
        row: Dictionary from CSV with string values.

    Returns:
        Dictionary with properly typed values for SilverFuturesMBO table.
    """
    model = SilverFuturesMBOModel(
        ts_recv=parse_iso8601(row["ts_recv"]),
        ts_event=parse_iso8601(row["ts_event"]),
        rtype=int(row["rtype"]),
        publisher_id=int(row["publisher_id"]),
        instrument_id=int(row["instrument_id"]),
        action=row["action"],
        side=row["side"] if row["side"] else None,
        price=Decimal(row["price"]) if row["price"] else None,
        size=int(row["size"]),
        channel_id=int(row["channel_id"]) if row["channel_id"] else None,
        order_id=int(row["order_id"]),
        flags=int(row["flags"]) if row["flags"] else None,
        ts_in_delta=int(row["ts_in_delta"]) if row["ts_in_delta"] else None,
        sequence=int(row["sequence"]) if row["sequence"] else None,
        symbol=row["symbol"],
    )
    return model.model_dump(exclude={"id"})


async def upload_batch(data: list[SilverFuturesMBOModel]) -> int:
    """Upload a batch of MBO records to the database.

    Args:
        data: List of dictionaries containing MBO record data.

    Returns:
        Number of records inserted/updated.

    Raises:
        Exception: Re-raises after logging a concise error summary.
    """
    if not data:
        return 0

    try:
        async with async_session_maker() as session:
            stmt = insert(SilverFuturesMBO).values(data)
            stmt = stmt.on_conflict_do_nothing(constraint="uq_silver_mbo_event")
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount if result.rowcount else len(data)
    except Exception as e:
        first_ts = data[0].get("ts_event", "unknown") if data else "N/A"
        log.error(
            f"Batch insert failed: {type(e).__name__}: {e!s:.200}"
            f" | batch_size={len(data)}, first_row_ts_event={first_ts}"
        )
        raise


async def process_file(file_path: Path) -> tuple[str, int, int]:
    """Process a single zst file: stream decompress, parse, and upload to database.

    Rows are streamed from the compressed file and uploaded in batches without
    ever holding the full file contents in memory.

    Args:
        file_path: Path to the .zst file to process.

    Returns:
        Tuple of (filename, total_rows, inserted_rows).
    """
    log.info(f"Processing file: {file_path.name}")

    try:
        # psycopg limits queries to 65,535 parameters; calculate max rows per batch
        num_columns = len(SilverFuturesMBO.__table__.columns)
        batch_size = min(POSTGRES_BATCH_MAX, 65535 // num_columns)
        batch: list[dict] = []
        total_rows = 0
        inserted = 0
        batch_count = 0

        for row in decompress_zst_file_streaming(file_path):
            batch.append(transform_row(row))
            total_rows += 1

            if len(batch) >= batch_size:
                count = await upload_batch(batch)
                inserted += count
                batch.clear()
                batch_count += 1

                if batch_count % 10 == 0:
                    log.info(f"  Streamed {total_rows} rows from {file_path.name}, batch {batch_count}")

        # Upload remaining rows
        if batch:
            count = await upload_batch(batch)
            inserted += count

        if total_rows == 0:
            log.warning(f"No rows found in {file_path.name}")
            return (file_path.name, 0, 0)

        log.info(f"Completed {file_path.name}: {inserted}/{total_rows} rows inserted, {batch_count} batches")
        return (file_path.name, total_rows, inserted)

    except Exception as e:
        log.error(f"Error processing {file_path.name}: {e}")
        raise


async def process_directory(
    input_dir: Path,
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


@silver_app.command()
def upload(
    input_dir: str = typer.Option(
        ...,
        "--input-dir",
        "-i",
        help="Directory containing .zst compressed CSV files",
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


@silver_app.command()
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
        import csv as csv_mod

        header_written = False
        with open(out_file, "w", newline="") as f:
            writer: csv_mod.DictWriter | None = None
            for row in decompress_zst_file_streaming(file_path):
                if not header_written:
                    writer = csv_mod.DictWriter(f, fieldnames=row.keys())
                    writer.writeheader()
                    header_written = True
                assert writer is not None
                writer.writerow(row)
        typer.echo(f"  Decompressed: {file_path.name} -> {out_file.name}")

    typer.echo(f"\nDecompressed {len(zst_files)} files to {output_path}")


@silver_app.command()
def test_upload(
    file_path: str = typer.Argument(
        ...,
        help="Path to a single .zst compressed CSV file to upload",
    ),
) -> None:
    """Test uploading a single .zst file to verify the pipeline works correctly.

    Decompresses the file, prints the first row, reports total row count,
    and uploads to the database.

    Args:
        file_path: Path to the .zst file to test.
        dry_run: If True, only decompress and preview without uploading.
    """
    path = Path(file_path)

    if not path.exists():
        typer.echo(f"Error: File {file_path} does not exist")
        raise typer.Exit(1)

    if not path.is_file():
        typer.echo(f"Error: {file_path} is not a file")
        raise typer.Exit(1)

    if not path.name.endswith(".zst"):
        typer.echo(f"Warning: {path.name} does not have a .zst extension")

    file_size_mb = path.stat().st_size / (1024 * 1024)
    typer.echo(f"File: {path.name}")
    typer.echo(f"Size: {file_size_mb:.2f} MB")

    row_count = 0
    for row in decompress_zst_file_streaming(path):
        if row_count == 5:
            typer.echo(row)
            break
        row_count += 1

    typer.echo("\nUploading...")
    filename, total_rows, inserted_rows = asyncio.run(process_file(path))

    typer.echo("\n--- Upload Result ---")
    typer.echo(f"File: {filename}")
    typer.echo(f"Total rows: {total_rows}")
    typer.echo(f"Inserted rows: {inserted_rows}")
    typer.echo(f"Duplicates skipped: {total_rows - inserted_rows}")


if __name__ == "__main__":
    test_upload("curator/temp/data/GLBX-20260203-HYH8YBP4HD/glbx-mdp3-20250316-20260202.mbo.SILH6.csv.zst")
