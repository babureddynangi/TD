"""
Schema Validator Lambda handler.
Validates required fields, data types, date formats, and enum values.
"""
from __future__ import annotations
import os
import re
import boto3
from datetime import datetime
from src.utils.rule_loader import load_rule_pack, RulePackError

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_TYPE_CHECKERS = {
    "str": lambda v: isinstance(v, str),
    "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "bool": lambda v: isinstance(v, bool),
    "date": lambda v: isinstance(v, str) and bool(_DATE_RE.match(v)),
}

_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _load_schema(bucket: str, key: str) -> dict:
    return load_rule_pack(_get_s3(), bucket, key)


def validate_record(record: dict, schema: dict) -> list[str]:
    """
    Validate a single record dict against the schema rule pack.
    Returns a list of failure reason codes (empty = PASS).
    """
    reasons: list[str] = []
    required_fields: list[str] = schema.get("required_fields", [])
    field_types: dict = schema.get("field_types", {})
    enum_fields: dict = schema.get("enum_fields", {})

    # 2.1 / 2.2 — required fields present
    for field in required_fields:
        if field not in record or record[field] is None or record[field] == "":
            reasons.append(f"SCHEMA_MISSING_FIELD:{field}")

    # 2.3 / 2.4 — type checks (skip if field already missing)
    for field, expected_type in field_types.items():
        if field not in record or record[field] is None:
            continue
        checker = _TYPE_CHECKERS.get(expected_type)
        if checker and not checker(record[field]):
            reasons.append(f"SCHEMA_TYPE_MISMATCH:{field}")

    # 2.5 / 2.6 — date format ISO 8601
    date_fields: list[str] = schema.get("date_fields", [])
    for field in date_fields:
        value = record.get(field)
        if value is None or value == "":
            continue  # missing field already caught above
        if not _DATE_RE.match(str(value)):
            reasons.append(f"SCHEMA_DATE_FORMAT:{field}")
        else:
            # Validate it's actually a real calendar date
            try:
                datetime.strptime(str(value), "%Y-%m-%d")
            except ValueError:
                reasons.append(f"SCHEMA_DATE_FORMAT:{field}")

    # 2.7 / 2.8 — enum values
    for field, allowed in enum_fields.items():
        value = record.get(field)
        if value is None or value == "":
            continue  # missing field already caught above
        if str(value) not in allowed:
            reasons.append(f"SCHEMA_INVALID_ENUM:{field}")

    return reasons


def handler(event: dict, context) -> dict:
    """
    Step Functions task handler.
    Input:  {run_id, bucket, key, records: list[dict]}
    Output: same structure with each record annotated with schema_status + failure_reasons
    """
    run_id: str = event["run_id"]
    bucket: str = event["bucket"]
    records: list[dict] = event["records"]

    rule_pack_bucket = os.environ.get("RULE_PACK_BUCKET", bucket)
    rule_pack_key = os.environ.get("SCHEMA_RULE_PACK_KEY", "rule-packs/schema_rules.json")

    schema = _load_schema(rule_pack_bucket, rule_pack_key)

    annotated = []
    for record in records:
        reasons = validate_record(record, schema)
        record["schema_status"] = "FAIL" if reasons else "PASS"
        existing = record.get("failure_reasons", [])
        record["failure_reasons"] = existing + reasons
        annotated.append(record)

    return {
        "run_id": run_id,
        "bucket": bucket,
        "key": event.get("key", ""),
        "records": annotated,
    }
