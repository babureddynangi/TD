"""
Business Rule Validator Lambda handler.
Enforces credit-card-specific business logic rules.
"""
from __future__ import annotations
import os
import math
import boto3
from src.utils.rule_loader import load_rule_pack

_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _float(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _approx_equal(a: float, b: float, tol: float = 0.01) -> bool:
    """Compare floats with a small tolerance to handle floating-point drift."""
    return math.isclose(a, b, abs_tol=tol)


def validate_record(record: dict, business_rules: dict) -> list[str]:
    """
    Apply business rule checks to a single record.
    Returns a list of failure reason codes (empty = PASS).
    """
    reasons: list[str] = []

    delinquency_map: dict = business_rules.get("delinquency_bucket_map", {})

    # 4.1 / 4.2 — product_type
    if str(record.get("product_type", "")).upper() != "CREDIT_CARD":
        reasons.append("BUS_WRONG_PRODUCT_TYPE")

    current_balance = _float(record.get("current_balance"))
    credit_limit = _float(record.get("credit_limit"))
    available_credit = _float(record.get("available_credit"))
    past_due_amount = _float(record.get("past_due_amount"))

    # 4.3 / 4.4 — current_balance >= 0
    if current_balance is not None and current_balance < 0:
        reasons.append("BUS_NEGATIVE_BALANCE")

    # 4.5 / 4.6 — credit_limit > 0
    if credit_limit is not None and credit_limit <= 0:
        reasons.append("BUS_INVALID_CREDIT_LIMIT")

    # 4.7 / 4.8 — available_credit == credit_limit - current_balance
    if (
        current_balance is not None
        and credit_limit is not None
        and available_credit is not None
    ):
        expected = credit_limit - current_balance
        if not _approx_equal(available_credit, expected):
            reasons.append("BUS_AVAILABLE_CREDIT_MISMATCH")

    # 4.9 / 4.10 — past_due_amount == 0 when payment_status == CURRENT
    payment_status = str(record.get("payment_status", "")).upper()
    if payment_status == "CURRENT":
        if past_due_amount is not None and not _approx_equal(past_due_amount, 0.0):
            reasons.append("BUS_PAST_DUE_CONFLICT")

    # 4.11 / 4.12 — close_date present when account_status == CLOSED
    account_status = str(record.get("account_status", "")).upper()
    close_date = record.get("close_date")
    if account_status == "CLOSED":
        if not close_date or str(close_date).strip() == "":
            reasons.append("BUS_MISSING_CLOSE_DATE")

    # 4.13 / 4.14 — delinquency_bucket consistent with payment_status
    delinquency_bucket = str(record.get("delinquency_bucket", ""))
    if delinquency_map and payment_status and delinquency_bucket:
        allowed_buckets = delinquency_map.get(payment_status, [])
        if allowed_buckets and delinquency_bucket not in allowed_buckets:
            reasons.append("BUS_DELINQUENCY_MISMATCH")

    return reasons


def handler(event: dict, context) -> dict:
    """
    Step Functions task handler.
    Input:  {run_id, bucket, key, records: list[dict]}
    Output: same structure with each record annotated with business_status
    """
    run_id: str = event["run_id"]
    bucket: str = event["bucket"]
    records: list[dict] = event["records"]

    rule_pack_bucket = os.environ.get("RULE_PACK_BUCKET", bucket)
    rule_pack_key = os.environ.get("BUSINESS_RULE_PACK_KEY", "rule-packs/business_rules.json")

    business_rules = load_rule_pack(_get_s3(), rule_pack_bucket, rule_pack_key)

    annotated = []
    for record in records:
        reasons = validate_record(record, business_rules)
        record["business_status"] = "FAIL" if reasons else "PASS"
        existing = record.get("failure_reasons", [])
        record["failure_reasons"] = existing + reasons
        annotated.append(record)

    return {
        "run_id": run_id,
        "bucket": bucket,
        "key": event.get("key", ""),
        "records": annotated,
    }
