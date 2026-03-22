"""
Input file parser — supports CSV and JSON formats.
Raises ParseError on empty or malformed content.
"""
from __future__ import annotations
import csv
import io
import json


class ParseError(Exception):
    """Raised when input content cannot be parsed or is empty."""


def parse_json(content: str) -> list[dict]:
    """
    Parse a JSON string into a list of record dicts.
    Accepts a JSON array or a newline-delimited JSON (one object per line).
    Raises ParseError if content is empty or malformed.
    """
    if not content or not content.strip():
        raise ParseError("Input content is empty")

    stripped = content.strip()
    try:
        # Try standard JSON array first
        data = json.loads(stripped)
        if isinstance(data, list):
            if len(data) == 0:
                raise ParseError("Input JSON array is empty")
            return data
        if isinstance(data, dict):
            return [data]
        raise ParseError(f"Expected JSON array or object, got {type(data).__name__}")
    except json.JSONDecodeError:
        # Fall back to newline-delimited JSON
        records = []
        for line_num, line in enumerate(stripped.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ParseError(f"Invalid JSON on line {line_num}: {exc}") from exc
        if not records:
            raise ParseError("Input content is empty")
        return records


def parse_csv(content: str) -> list[dict]:
    """
    Parse a CSV string into a list of record dicts.
    First row is treated as the header.
    Raises ParseError if content is empty or has no data rows.
    """
    if not content or not content.strip():
        raise ParseError("Input content is empty")

    reader = csv.DictReader(io.StringIO(content.strip()))
    records = [row for row in reader]

    if not records:
        raise ParseError("CSV file contains no data rows")

    return records


def parse_input(content: str, fmt: str) -> list[dict]:
    """
    Dispatch to the correct parser based on fmt ('csv' or 'json').
    Raises ParseError for unknown formats or parse failures.
    """
    fmt = fmt.lower().strip()
    if fmt == "json":
        return parse_json(content)
    if fmt == "csv":
        return parse_csv(content)
    raise ParseError(f"Unsupported format: {fmt!r}. Expected 'csv' or 'json'.")
