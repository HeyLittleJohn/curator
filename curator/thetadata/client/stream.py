"""Incremental CSV and NDJSON parsing.

Rows are yielded as soon as a newline arrives. The parser keeps the current
partial line only, not the rest of the body.
"""

from __future__ import annotations

import csv
import json
from collections.abc import AsyncIterator, Mapping


async def iter_text_lines(chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
    """Yield UTF-8 lines from a chunked byte stream.

    Args:
        chunks: Successive body chunks. A line may be split across chunks.

    Yields:
        One decoded line, without the trailing newline. Blank lines are skipped.
    """
    pending = bytearray()
    async for chunk in chunks:
        pending.extend(chunk)
        while True:
            newline = pending.find(b"\n")
            if newline < 0:
                break
            raw = bytes(pending[:newline])
            del pending[: newline + 1]
            line = _decode_line(raw)
            if line:
                yield line
    if pending:
        line = _decode_line(bytes(pending))
        if line:
            yield line


def _decode_line(raw: bytes) -> str:
    """Decode one CSV or NDJSON line.

    Args:
        raw: Line bytes, possibly ending in a carriage return.

    Returns:
        The stripped line. A leading BOM is removed.
    """
    if raw.endswith(b"\r"):
        raw = raw[:-1]
    text = raw.decode("utf-8").strip()
    if text.startswith("\ufeff"):
        return text.lstrip("\ufeff").strip()
    return text


async def iter_csv_records(lines: AsyncIterator[str]) -> AsyncIterator[dict[str, str]]:
    """Yield one dictionary per CSV data row.

    Args:
        lines: Header first, then data rows. A line may have been reassembled
            from several socket chunks.

    Yields:
        A mapping of header name to cell text. Missing cells are empty strings.
    """
    header: list[str] | None = None
    async for line in lines:
        cells = next(csv.reader([line]))
        if header is None:
            header = [name.strip() for name in cells]
            continue
        if len(cells) < len(header):
            cells = [*cells, *[""] * (len(header) - len(cells))]
        yield {name: cells[index] for index, name in enumerate(header)}


async def iter_ndjson_records(lines: AsyncIterator[str]) -> AsyncIterator[dict[str, object]]:
    """Yield one JSON object per line.

    Args:
        lines: NDJSON lines.

    Yields:
        The object on each line.

    Raises:
        ValueError: A line is not a JSON object.
    """
    async for line in lines:
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError("NDJSON row was not an object")
        record: dict[str, object] = {}
        for key, value in payload.items():
            record[str(key)] = value
        yield record


async def iter_response_records(
    lines: AsyncIterator[str],
    response_format: str,
) -> AsyncIterator[Mapping[str, object]]:
    """Yield row mappings for a CSV or NDJSON body.

    Args:
        lines: Decoded lines from the response body.
        response_format: ``csv`` or ``ndjson``.

    Yields:
        One row mapping per data line.

    Raises:
        ValueError: ``response_format`` is not streamed by this client.
    """
    if response_format == "csv":
        async for record in iter_csv_records(lines):
            yield record
        return
    if response_format == "ndjson":
        async for record in iter_ndjson_records(lines):
            yield record
        return
    raise ValueError(f"Unsupported response format {response_format!r}")
