"""
Rule pack loader — reads JSON rule packs from S3 with module-level caching
for Lambda warm-start reuse.
"""
from __future__ import annotations
import json
from typing import Any

# Module-level cache: maps (bucket, key) -> parsed rule pack dict
_cache: dict[tuple[str, str], dict] = {}


class RulePackError(Exception):
    """Raised when a rule pack cannot be loaded or is invalid JSON."""


def load_rule_pack(s3_client: Any, bucket: str, key: str) -> dict:
    """
    Load and return a rule pack dict from S3.

    Results are cached by (bucket, key) for the lifetime of the Lambda
    execution environment (warm reuse).

    Raises RulePackError if the object is missing or contains invalid JSON.
    """
    cache_key = (bucket, key)
    if cache_key in _cache:
        return _cache[cache_key]

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw = response["Body"].read().decode("utf-8")
    except s3_client.exceptions.NoSuchKey:
        raise RulePackError(f"Rule pack not found: s3://{bucket}/{key}")
    except Exception as exc:
        raise RulePackError(f"Failed to fetch rule pack s3://{bucket}/{key}: {exc}") from exc

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RulePackError(
            f"Rule pack s3://{bucket}/{key} contains invalid JSON: {exc}"
        ) from exc

    if not isinstance(data, dict):
        raise RulePackError(
            f"Rule pack s3://{bucket}/{key} must be a JSON object, got {type(data).__name__}"
        )

    _cache[cache_key] = data
    return data


def clear_cache() -> None:
    """Clear the rule pack cache (useful in tests)."""
    _cache.clear()
