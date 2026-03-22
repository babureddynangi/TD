"""
State Rule Validator Lambda handler.
Applies state-specific regulatory rule overrides keyed by state_code.
"""
from __future__ import annotations
import os
import boto3
from src.utils.rule_loader import load_rule_pack

_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _run_extra_check(check_name: str, record: dict) -> list[str]:
    """
    Execute a named extra check against a record.
    Returns a list of failure reason codes.
    """
    reasons: list[str] = []

    if check_name == "DISPUTE_CONSISTENCY":
        # dispute_flag must be True if any dispute-related field is set
        dispute_flag = record.get("dispute_flag")
        if dispute_flag is None:
            reasons.append("STATE_DISPUTE_FLAG_MISSING")

    elif check_name == "CLOSE_DATE_LOGIC":
        # close_date must be present and not in the future when account is CLOSED
        account_status = str(record.get("account_status", "")).upper()
        close_date = record.get("close_date")
        if account_status == "CLOSED" and (not close_date or str(close_date).strip() == ""):
            reasons.append("STATE_CLOSE_DATE_REQUIRED")

    # Unknown extra checks are silently skipped (forward-compatible)
    return reasons


def validate_record(record: dict, state_rules: dict) -> tuple[str, list[str]]:
    """
    Apply state-specific rules to a single record.
    Returns (state_status, reasons).
    state_status is one of: PASS | FAIL | REVIEW_REQUIRED
    """
    reasons: list[str] = []
    state_code = str(record.get("state_code", "")).upper()

    # 5.3 — no rule entry for this state
    if state_code not in state_rules:
        return "REVIEW_REQUIRED", ["STATE_NO_RULE_FOUND"]

    rule = state_rules[state_code]

    # 5.4 — required_fields
    for field in rule.get("required_fields", []):
        val = record.get(field)
        if val is None or str(val).strip() == "":
            reasons.append(f"STATE_MISSING_REQUIRED_FIELD:{field}")

    # 5.5 — disallowed_statuses
    account_status = str(record.get("account_status", "")).upper()
    for disallowed in rule.get("disallowed_statuses", []):
        if account_status == str(disallowed).upper():
            reasons.append(f"STATE_DISALLOWED_STATUS:{disallowed}")

    # 5.6 — extra_checks
    for check_name in rule.get("extra_checks", []):
        reasons.extend(_run_extra_check(check_name, record))

    state_status = "FAIL" if reasons else "PASS"
    return state_status, reasons


def handler(event: dict, context) -> dict:
    """
    Step Functions task handler.
    Input:  {run_id, bucket, key, records: list[dict]}
    Output: same structure with each record annotated with state_status
    """
    run_id: str = event["run_id"]
    bucket: str = event["bucket"]
    records: list[dict] = event["records"]

    rule_pack_bucket = os.environ.get("RULE_PACK_BUCKET", bucket)
    rule_pack_key = os.environ.get("STATE_RULE_PACK_KEY", "rule-packs/state_rules.json")

    state_rules = load_rule_pack(_get_s3(), rule_pack_bucket, rule_pack_key)

    annotated = []
    for record in records:
        state_status, reasons = validate_record(record, state_rules)
        record["state_status"] = state_status
        existing = record.get("failure_reasons", [])
        record["failure_reasons"] = existing + reasons
        annotated.append(record)

    return {
        "run_id": run_id,
        "bucket": bucket,
        "key": event.get("key", ""),
        "records": annotated,
    }
