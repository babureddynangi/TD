"""
Generate benchmark datasets with seeded defects for the credit-card DQ pipeline.
Produces small (100), medium (1000), and large (5000) row datasets.
Each dataset has a known truth label per record for evaluation.

Usage:
    python scripts/generate_benchmark_data.py
"""
from __future__ import annotations
import json
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

VALID_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
]
# States with rules in state_rules.json
KNOWN_STATES = ["CA", "NY", "TX", "FL", "IL", "WA", "GA", "OH", "PA", "AZ"]
UNKNOWN_STATES = [s for s in VALID_STATES if s not in KNOWN_STATES]

PAYMENT_STATUSES = ["CURRENT", "30_DAYS", "60_DAYS", "90_DAYS", "120_DAYS", "CHARGE_OFF"]
ACCOUNT_STATUSES_OPEN = ["OPEN", "DELINQUENT", "FROZEN"]
DELINQUENCY_MAP = {
    "CURRENT": "CURRENT",
    "30_DAYS": "30_DAYS",
    "60_DAYS": "60_DAYS",
    "90_DAYS": "90_DAYS",
    "120_DAYS": "120_DAYS",
    "CHARGE_OFF": "CHARGE_OFF",
}

TODAY = date(2026, 3, 1)  # fixed for reproducibility

# Defect categories and their injection rates (fraction of total records)
DEFECT_CATEGORIES = [
    "clean",                    # 60% — no defect
    "schema_missing_field",     # 5%
    "schema_type_mismatch",     # 3%
    "schema_date_format",       # 3%
    "schema_invalid_enum",      # 2%
    "dq_null_mandatory",        # 5%
    "dq_duplicate_account",     # 3%
    "dq_invalid_state",         # 3%
    "dq_invalid_zip",           # 2%
    "dq_future_date",           # 2%
    "bus_available_credit",     # 3%
    "bus_past_due_conflict",    # 3%
    "bus_missing_close_date",   # 2%
    "bus_delinquency_mismatch", # 2%
    "state_no_rule",            # 2%
]

DEFECT_WEIGHTS = [60, 5, 3, 3, 2, 5, 3, 3, 2, 2, 3, 3, 2, 2, 2]


def _clean_record(record_id: str, account_id: str, tradeline_id: str, state_code: str) -> dict:
    credit_limit = round(random.uniform(500, 20000), 2)
    current_balance = round(random.uniform(0, credit_limit), 2)
    available_credit = round(credit_limit - current_balance, 2)
    payment_status = random.choice(PAYMENT_STATUSES)
    past_due_amount = 0.0 if payment_status == "CURRENT" else round(random.uniform(0, current_balance), 2)
    account_status = random.choice(ACCOUNT_STATUSES_OPEN)
    days_back = random.randint(0, 730)
    reporting_date = (TODAY - timedelta(days=days_back)).isoformat()
    zip_code = f"{random.randint(10000, 99999):05d}"

    return {
        "record_id": record_id,
        "account_id": account_id,
        "tradeline_id": tradeline_id,
        "product_type": "CREDIT_CARD",
        "state_code": state_code,
        "zip_code": zip_code,
        "reporting_date": reporting_date,
        "account_status": account_status,
        "payment_status": payment_status,
        "current_balance": current_balance,
        "credit_limit": credit_limit,
        "available_credit": available_credit,
        "past_due_amount": past_due_amount,
        "delinquency_bucket": DELINQUENCY_MAP[payment_status],
        "dispute_flag": random.choice([True, False]),
        "close_date": None,
    }


def _inject_defect(record: dict, defect: str) -> tuple[dict, str]:
    """Inject a defect into a clean record. Returns (modified_record, expected_outcome)."""
    r = dict(record)

    if defect == "schema_missing_field":
        field = random.choice(["account_id", "tradeline_id", "zip_code", "reporting_date"])
        r.pop(field, None)
        return r, "FAIL"

    elif defect == "schema_type_mismatch":
        r["current_balance"] = "not_a_number"
        return r, "FAIL"

    elif defect == "schema_date_format":
        r["reporting_date"] = "15/01/2026"
        return r, "FAIL"

    elif defect == "schema_invalid_enum":
        r["product_type"] = "DEBIT_CARD"
        return r, "FAIL"

    elif defect == "dq_null_mandatory":
        r["account_id"] = ""
        return r, "FAIL"

    elif defect == "dq_duplicate_account":
        # Caller handles duplicate injection — mark as FAIL
        return r, "FAIL"

    elif defect == "dq_invalid_state":
        r["state_code"] = random.choice(["ZZ", "XX", "QQ", "99"])
        return r, "FAIL"

    elif defect == "dq_invalid_zip":
        r["zip_code"] = str(random.randint(100, 9999))  # too short
        return r, "FAIL"

    elif defect == "dq_future_date":
        days_ahead = random.randint(1, 365)
        r["reporting_date"] = (TODAY + timedelta(days=days_ahead)).isoformat()
        return r, "FAIL"

    elif defect == "bus_available_credit":
        r["available_credit"] = round(r["available_credit"] + random.uniform(50, 500), 2)
        return r, "FAIL"

    elif defect == "bus_past_due_conflict":
        r["payment_status"] = "CURRENT"
        r["delinquency_bucket"] = "CURRENT"
        r["past_due_amount"] = round(random.uniform(10, 500), 2)
        return r, "FAIL"

    elif defect == "bus_missing_close_date":
        r["account_status"] = "CLOSED"
        r["close_date"] = None
        return r, "FAIL"

    elif defect == "bus_delinquency_mismatch":
        r["payment_status"] = "CURRENT"
        r["delinquency_bucket"] = random.choice(["30_DAYS", "60_DAYS", "90_DAYS"])
        r["past_due_amount"] = 0.0
        return r, "FAIL"

    elif defect == "state_no_rule":
        r["state_code"] = random.choice(UNKNOWN_STATES)
        return r, "REVIEW_REQUIRED"

    return r, "PASS"


def generate_dataset(size: int, name: str) -> tuple[list[dict], list[dict]]:
    """
    Generate a dataset of `size` records with seeded defects.
    Returns (records, truth_labels).
    truth_labels: list of {record_id, defect_category, expected_outcome}
    """
    records = []
    truth_labels = []

    # Pre-assign defect categories
    defect_assignments = random.choices(DEFECT_CATEGORIES, weights=DEFECT_WEIGHTS, k=size)

    # Track duplicate account_ids for dq_duplicate_account defect
    duplicate_pool: list[str] = []
    seen_accounts: set[str] = set()

    for i, defect in enumerate(defect_assignments):
        record_id = f"{name.upper()}_{i+1:05d}"
        tradeline_id = str(uuid.uuid4())
        state_code = random.choice(KNOWN_STATES)

        if defect == "dq_duplicate_account" and duplicate_pool:
            account_id = random.choice(duplicate_pool)
        else:
            account_id = str(uuid.uuid4())
            if defect == "dq_duplicate_account":
                duplicate_pool.append(account_id)

        seen_accounts.add(account_id)
        base = _clean_record(record_id, account_id, tradeline_id, state_code)

        if defect == "clean":
            records.append(base)
            truth_labels.append({
                "record_id": record_id,
                "defect_category": "clean",
                "expected_outcome": "PASS",
            })
        else:
            modified, expected = _inject_defect(base, defect)
            records.append(modified)
            truth_labels.append({
                "record_id": record_id,
                "defect_category": defect,
                "expected_outcome": expected,
            })

    return records, truth_labels


def main():
    out_dir = Path("benchmark-data")
    out_dir.mkdir(exist_ok=True)

    sizes = {"small": 100, "medium": 1000, "large": 5000}

    for name, size in sizes.items():
        records, truth = generate_dataset(size, name)

        (out_dir / f"{name}_input.json").write_text(json.dumps(records, indent=2))
        (out_dir / f"{name}_truth.json").write_text(json.dumps(truth, indent=2))

        # Summary
        from collections import Counter
        defect_counts = Counter(t["defect_category"] for t in truth)
        outcome_counts = Counter(t["expected_outcome"] for t in truth)
        print(f"\n{name.upper()} ({size} records)")
        print(f"  Expected outcomes: {dict(outcome_counts)}")
        print(f"  Defect categories: {dict(defect_counts)}")

    print("\nDatasets written to benchmark-data/")


if __name__ == "__main__":
    main()
