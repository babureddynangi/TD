"""
DQ Validator Lambda handler.
Checks nulls, uniqueness, format correctness, and date constraints.
"""
from __future__ import annotations
import os
import re
from datetime import date, datetime
import boto3
from src.utils.rule_loader import load_rule_pack

_ZIP_RE = re.compile(r"^\d{5}(-\d{4})?$")
_STATE_CODES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}

_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def validate_dataset(records: list[dict], dq_rules: dict, run_date: date | None = None) -> list[dict]:
    """
    Apply DQ checks to the full dataset (uniqueness requires the full set).
    Annotates each record with dq_status and appends to failure_reasons.
    Returns the annotated list.
    """
    if run_date is None:
        run_date = date.today()

    mandatory_fields: list[str] = dq_rules.get("mandatory_fields", [])
    uniqueness_keys: list[str] = dq_rules.get("uniqueness_keys", [])

    # Build seen sets for uniqueness checks
    seen: dict[str, set] = {key: set() for key in uniqueness_keys}
    duplicates: dict[str, set] = {key: set() for key in uniqueness_keys}

    # First pass — find all duplicate values
    for record in records:
        for key in uniqueness_keys:
            val = record.get(key)
            if val is not None and val != "":
                if val in seen[key]:
                    duplicates[key].add(val)
                else:
                    seen[key].add(val)

    # Second pass — annotate each record
    for record in records:
        reasons: list[str] = []

        # 3.1 / 3.2 — mandatory fields non-null/non-empty
        for field in mandatory_fields:
            val = record.get(field)
            if val is None or str(val).strip() == "":
                reasons.append(f"DQ_NULL_MANDATORY:{field}")

        # 3.3–3.6 — uniqueness
        for key in uniqueness_keys:
            val = record.get(key)
            if val is not None and val in duplicates[key]:
                code_map = {
                    "account_id": "DQ_DUPLICATE_ACCOUNT_ID",
                    "tradeline_id": "DQ_DUPLICATE_TRADELINE_ID",
                }
                reason = code_map.get(key, f"DQ_DUPLICATE:{key}")
                reasons.append(reason)

        # 3.7 / 3.8 — state_code
        state_code = record.get("state_code", "")
        if state_code and str(state_code).upper() not in _STATE_CODES:
            reasons.append("DQ_INVALID_STATE_CODE")

        # 3.9 / 3.10 — ZIP code
        zip_code = record.get("zip_code", "")
        if zip_code and not _ZIP_RE.match(str(zip_code)):
            reasons.append("DQ_INVALID_ZIP")

        # 3.11 / 3.12 — reporting_date not in future
        reporting_date_str = record.get("reporting_date", "")
        if reporting_date_str:
            try:
                reporting_date = datetime.strptime(str(reporting_date_str), "%Y-%m-%d").date()
                if reporting_date > run_date:
                    reasons.append("DQ_FUTURE_REPORTING_DATE")
            except ValueError:
                pass  # malformed date already caught by schema validator

        record["dq_status"] = "FAIL" if reasons else "PASS"
        existing = record.get("failure_reasons", [])
        record["failure_reasons"] = existing + reasons

    return records


def handler(event: dict, context) -> dict:
    """
    Step Functions task handler.
    Input:  {run_id, bucket, key, records: list[dict]}
    Output: same structure with each record annotated with dq_status
    """
    run_id: str = event["run_id"]
    bucket: str = event["bucket"]
    records: list[dict] = event["records"]

    rule_pack_bucket = os.environ.get("RULE_PACK_BUCKET", bucket)
    rule_pack_key = os.environ.get("DQ_RULE_PACK_KEY", "rule-packs/dq_rules.json")

    dq_rules = load_rule_pack(_get_s3(), rule_pack_bucket, rule_pack_key)
    annotated = validate_dataset(records, dq_rules)

    return {
        "run_id": run_id,
        "bucket": bucket,
        "key": event.get("key", ""),
        "records": annotated,
    }
