"""Silver futures MBO data upload script.

This module provides functionality to decompress Databento zstd-compressed CSV files
and upload the MBO (Market By Order) data to PostgreSQL using psycopg's COPY protocol
and aiomultiprocess for parallel file processing.

Example usage:
    uv run curator silver upload --input-dir /path/to/data
    uv run curator silver upload --files file1.zst file2.zst
"""

import asyncio
import csv
import io
from collections.abc import Generator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import psycopg
import typer
import zstandard
from aiomultiprocess import Pool

from curator.db_tools.schemas import SilverFuturesMBOModel
from curator.proj_constants import POOL_DEFAULT_KWARGS, log, psycopg_conninfo

silver_app = typer.Typer(help="Silver futures MBO data upload commands")

# Columns to COPY into -- derived from the pydantic model, excluding auto-generated `id`
COPY_COLUMNS = tuple(f for f in SilverFuturesMBOModel.model_fields if f != "id")

DEFAULT_WORKERS = POOL_DEFAULT_KWARGS["processes"]


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


def _row_to_copy_tuple(data: dict) -> tuple:
    """Convert a validated row dict to a tuple in COPY_COLUMNS order.

    Args:
        data: Dictionary from ``transform_row`` with properly typed values.

    Returns:
        Tuple of values in the same order as ``COPY_COLUMNS``.
    """
    return tuple(data[col] for col in COPY_COLUMNS)


async def copy_to_db(rows: Generator[dict, None, None], file_name: str) -> tuple[int, int]:
    """Stream rows into PostgreSQL via COPY through a staging temp table.

    Creates a temporary table matching ``silver_futures_mbo`` structure,
    COPYs all validated rows into it, then merges into the real table
    with ``ON CONFLICT DO NOTHING``.

    Args:
        rows: Generator of raw CSV row dicts (from ``decompress_zst_file_streaming``).
        file_name: Filename string used for progress logging.

    Returns:
        Tuple of (total_rows_streamed, rows_inserted_into_real_table).
    """
    conninfo = psycopg_conninfo()
    cols_csv = ", ".join(COPY_COLUMNS)

    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=False) as conn:
        async with conn.cursor() as cur:
            # Create an unlogged temp table matching the target schema (no constraints/indexes)
            await cur.execute(
                "CREATE TEMP TABLE _staging_mbo (LIKE silver_futures_mbo INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            # Drop constraints/indexes inherited from LIKE to keep COPY fast
            await cur.execute("ALTER TABLE _staging_mbo DROP CONSTRAINT IF EXISTS uq_silver_mbo_event")
            await cur.execute("ALTER TABLE _staging_mbo ALTER COLUMN id DROP DEFAULT")
            await cur.execute("ALTER TABLE _staging_mbo ALTER COLUMN id DROP NOT NULL")

            # Stream rows via COPY
            total_rows = 0
            copy_sql = f"COPY _staging_mbo ({cols_csv}) FROM STDIN"
            async with cur.copy(copy_sql) as copy:
                for raw_row in rows:
                    validated = transform_row(raw_row)
                    await copy.write_row(_row_to_copy_tuple(validated))
                    total_rows += 1
                    if total_rows % 500_000 == 0:
                        log.info(f"  Streamed {total_rows} rows from {file_name}")

            # Merge staging -> real table, skipping duplicates
            result = await cur.execute(
                f"INSERT INTO silver_futures_mbo ({cols_csv})"
                f" SELECT {cols_csv} FROM _staging_mbo"
                " ON CONFLICT ON CONSTRAINT uq_silver_mbo_event DO NOTHING"
            )
            inserted = result.rowcount if result.rowcount else 0

        await conn.commit()

    return total_rows, inserted


async def process_file(file_path: Path) -> tuple[str, int, int]:
    """Process a single zst file: stream decompress, validate, and COPY to database.

    Rows are streamed from the compressed file through pydantic validation
    directly into a psycopg COPY pipeline without holding the full file in memory.

    Args:
        file_path: Path to the .zst file to process.

    Returns:
        Tuple of (filename, total_rows, inserted_rows).
    """
    log.info(f"Processing file: {file_path.name}")

    try:
        rows = decompress_zst_file_streaming(file_path)
        total_rows, inserted = await copy_to_db(rows, file_path.name)

        if total_rows == 0:
            log.warning(f"No rows found in {file_path.name}")
            return (file_path.name, 0, 0)

        log.info(f"Completed {file_path.name}: {inserted}/{total_rows} rows inserted")
        return (file_path.name, total_rows, inserted)

    except Exception as e:
        log.error(f"Error processing {file_path.name}: {e}")
        raise


def _resolve_file_list(
    input_dir: Optional[Path] = None,
    files: Optional[list[Path]] = None,
) -> list[Path]:
    """Build a list of .zst file paths from either a directory or explicit file list.

    Args:
        input_dir: Optional directory to glob for .zst files.
        files: Optional explicit list of file paths.

    Returns:
        Sorted list of Path objects pointing to .zst files.

    Raises:
        typer.BadParameter: If neither input_dir nor files is provided.
    """
    if files:
        return sorted(files)
    if input_dir:
        return sorted(input_dir.glob("*.zst"))
    raise typer.BadParameter("Provide either --input-dir or --files")


async def process_files_parallel(
    file_list: list[Path],
    workers: int = DEFAULT_WORKERS,
) -> dict:
    """Process multiple zst files in parallel using aiomultiprocess.

    Spins up *workers* child processes, each processing one file at a time
    via ``process_file``.

    Args:
        file_list: List of .zst file paths to process.
        workers: Number of parallel worker processes.

    Returns:
        Dictionary with processing statistics (files_processed, files_failed,
        total_rows, inserted_rows, failed_files).
    """
    log.info(f"Processing {len(file_list)} files with {workers} workers")

    results: dict = {
        "files_processed": 0,
        "files_failed": 0,
        "total_rows": 0,
        "inserted_rows": 0,
        "failed_files": [],
    }

    if not file_list:
        return results

    async with Pool(processes=workers, childconcurrency=1) as pool:
        outcomes = await pool.map(process_file, file_list)

    for _filename, total, inserted in outcomes:
        results["files_processed"] += 1
        results["total_rows"] += total
        results["inserted_rows"] += inserted

    return results


def update_lookup_tables() -> None:
    """Populate the ``daily_contract_volumes`` lookup table for new dates.

    Only processes dates in ``silver_futures_mbo`` that are not yet
    present in the lookup table, using the expression index on
    ``ts_event::date`` to avoid scanning the full 132 GB source table.

    Should be called after new data has been uploaded.
    """
    conninfo = psycopg_conninfo()
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            log.info("Updating lookup table: daily_contract_volumes")
            cur.execute("""
                INSERT INTO daily_contract_volumes (trade_date, symbol, total_volume)
                SELECT
                    ts_event::date,
                    symbol,
                    SUM(CASE WHEN action = 'T' THEN size ELSE 0 END)
                FROM silver_futures_mbo
                WHERE ts_event::date NOT IN (
                    SELECT trade_date FROM daily_contract_volumes
                )
                GROUP BY ts_event::date, symbol
            """)
        conn.commit()
    log.info("Lookup table updated")


@silver_app.command()
def upload(
    input_dir: Optional[str] = typer.Option(
        None,
        "--input-dir",
        "-i",
        help="Directory containing .zst compressed CSV files",
    ),
    files: Optional[list[str]] = typer.Option(
        None,
        "--files",
        "-f",
        help="Explicit list of .zst file paths to upload",
    ),
    workers: int = typer.Option(
        DEFAULT_WORKERS,
        "--workers",
        "-w",
        help="Number of parallel worker processes",
    ),
) -> None:
    """Upload silver futures MBO data from zstd-compressed CSV files.

    Accepts either ``--input-dir`` for a directory of .zst files or ``--files``
    for an explicit list. Files are processed in parallel using *workers*
    child processes.

    Args:
        input_dir: Directory containing .zst files.
        files: Explicit list of .zst file paths.
        workers: Number of parallel worker processes (default 16).
    """
    input_path = Path(input_dir) if input_dir else None
    if files:
        file_paths = [input_path / f if input_path else Path(f) for f in files]
    elif input_path:
        file_paths = list(input_path.glob("*.zst"))
    else:
        file_paths = []

    if input_path and not input_path.exists():
        typer.echo(f"Error: Directory {input_dir} does not exist")
        raise typer.Exit(1)

    if input_path and not input_path.is_dir():
        typer.echo(f"Error: {input_dir} is not a directory")
        raise typer.Exit(1)

    if not input_path and not file_paths:
        typer.echo("Error: Provide either --input-dir or --files")
        raise typer.Exit(1)

    file_list = _resolve_file_list(input_dir=input_path, files=file_paths)
    typer.echo(f"Processing {len(file_list)} files with {workers} workers")

    results = asyncio.run(process_files_parallel(file_list, workers=workers))

    typer.echo("\n--- Upload Summary ---")
    typer.echo(f"Files processed: {results['files_processed']}")
    typer.echo(f"Files failed: {results['files_failed']}")
    typer.echo(f"Total rows: {results['total_rows']}")
    typer.echo(f"Inserted rows: {results['inserted_rows']}")

    if results["failed_files"]:
        typer.echo("\nFailed files:")
        for fname, error in results["failed_files"]:
            typer.echo(f"  - {fname}: {error}")

    if results["inserted_rows"] > 0:
        typer.echo("\nUpdating lookup tables...")
        update_lookup_tables()
        typer.echo("Lookup tables updated.")


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

    if inserted_rows > 0:
        typer.echo("\nUpdating lookup tables...")
        update_lookup_tables()
        typer.echo("Lookup tables updated.")


if __name__ == "__main__":
    # test_upload("curator/temp/data/GLBX-20260203-HYH8YBP4HD/glbx-mdp3-20250316-20260202.mbo.SILH6.csv.zst")
    update_lookup_tables()
