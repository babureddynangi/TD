"""
Shared pytest fixtures and Hypothesis composite strategies for the
credit-card DQ validation test suite.
"""
from __future__ import annotations
import json
import uuid
from datetime import date, timedelta

import pytest
from hypothesis import strategies as st

# ── Constants ────────────────────────────────────────────────────────────────

VALID_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
]

KNOWN_STATES = ["CA", "NY", "TX", "FL", "IL", "WA", "GA", "OH", "PA", "AZ"]  # states with rules in state_rules.json
UNKNOWN_STATES = [s for s in VALID_STATES if s not in KNOWN_STATES]

PAYMENT_STATUSES = ["CURRENT", "30_DAYS", "60_DAYS", "90_DAYS", "120_DAYS", "CHARGE_OFF"]
ACCOUNT_STATUSES = ["OPEN", "CLOSED", "DELINQUENT", "CHARGED_OFF", "FROZEN"]
DELINQUENCY_BUCKETS = ["CURRENT", "30_DAYS", "60_DAYS", "90_DAYS", "120_DAYS", "CHARGE_OFF"]

TODAY = date.today()


# ── Hypothesis Strategies ────────────────────────────────────────────────────

@st.composite
def valid_record(draw, state_code=None) -> dict:
    """
    Generate a structurally and logically valid credit-card record.
    All business rules pass: available_credit = credit_limit - current_balance,
    payment_status/delinquency_bucket are consistent, etc.
    """
    record_id = draw(st.uuids().map(str))
    account_id = draw(st.uuids().map(str))
    tradeline_id = draw(st.uuids().map(str))

    sc = state_code or draw(st.sampled_from(KNOWN_STATES))

    zip_code = draw(
        st.one_of(
            st.from_regex(r"\d{5}", fullmatch=True),
            st.from_regex(r"\d{5}-\d{4}", fullmatch=True),
        )
    )

    # reporting_date: today or in the past (up to 2 years)
    days_back = draw(st.integers(min_value=0, max_value=730))
    reporting_date = (TODAY - timedelta(days=days_back)).isoformat()

    payment_status = draw(st.sampled_from(PAYMENT_STATUSES))
    # delinquency_bucket must match payment_status
    delinquency_bucket = payment_status

    account_status = draw(st.sampled_from(["OPEN", "DELINQUENT", "FROZEN"]))
    close_date = None  # only CLOSED accounts need close_date

    credit_limit = draw(st.floats(min_value=100.0, max_value=50000.0, allow_nan=False, allow_infinity=False))
    credit_limit = round(credit_limit, 2)

    current_balance = draw(st.floats(min_value=0.0, max_value=credit_limit, allow_nan=False, allow_infinity=False))
    current_balance = round(current_balance, 2)

    available_credit = round(credit_limit - current_balance, 2)

    past_due_amount = 0.0 if payment_status == "CURRENT" else draw(
        st.floats(min_value=0.0, max_value=current_balance, allow_nan=False, allow_infinity=False)
    )
    past_due_amount = round(past_due_amount, 2)

    return {
        "record_id": record_id,
        "account_id": account_id,
        "tradeline_id": tradeline_id,
        "product_type": "CREDIT_CARD",
        "state_code": sc,
        "zip_code": zip_code,
        "reporting_date": reporting_date,
        "account_status": account_status,
        "payment_status": payment_status,
        "current_balance": current_balance,
        "credit_limit": credit_limit,
        "available_credit": available_credit,
        "past_due_amount": past_due_amount,
        "delinquency_bucket": delinquency_bucket,
        "dispute_flag": draw(st.booleans()),
        "close_date": close_date,
    }


@st.composite
def closed_account_record(draw) -> dict:
    """Generate a valid record with account_status=CLOSED and a close_date."""
    record = draw(valid_record())
    record["account_status"] = "CLOSED"
    days_back = draw(st.integers(min_value=1, max_value=730))
    record["close_date"] = (TODAY - timedelta(days=days_back)).isoformat()
    return record


@st.composite
def invalid_record(draw, violation: str | None = None) -> dict:
    """
    Generate a record with a specific violation injected.
    If violation is None, a random violation is chosen.
    """
    record = draw(valid_record())
    violations = [
        "missing_required_field",
        "wrong_product_type",
        "negative_balance",
        "invalid_credit_limit",
        "available_credit_mismatch",
        "past_due_conflict",
        "missing_close_date",
        "future_reporting_date",
        "invalid_state_code",
        "invalid_zip",
    ]
    chosen = violation or draw(st.sampled_from(violations))

    if chosen == "missing_required_field":
        field = draw(st.sampled_from(["account_id", "tradeline_id", "state_code", "zip_code"]))
        record.pop(field, None)

    elif chosen == "wrong_product_type":
        record["product_type"] = draw(st.text(min_size=1, max_size=10).filter(lambda x: x != "CREDIT_CARD"))

    elif chosen == "negative_balance":
        record["current_balance"] = draw(st.floats(max_value=-0.01, allow_nan=False, allow_infinity=False))

    elif chosen == "invalid_credit_limit":
        record["credit_limit"] = draw(st.floats(max_value=0.0, allow_nan=False, allow_infinity=False))

    elif chosen == "available_credit_mismatch":
        record["available_credit"] = record["available_credit"] + draw(
            st.floats(min_value=1.0, max_value=100.0, allow_nan=False, allow_infinity=False)
        )

    elif chosen == "past_due_conflict":
        record["payment_status"] = "CURRENT"
        record["past_due_amount"] = draw(st.floats(min_value=0.01, max_value=500.0, allow_nan=False, allow_infinity=False))

    elif chosen == "missing_close_date":
        record["account_status"] = "CLOSED"
        record["close_date"] = None

    elif chosen == "future_reporting_date":
        days_ahead = draw(st.integers(min_value=1, max_value=365))
        record["reporting_date"] = (TODAY + timedelta(days=days_ahead)).isoformat()

    elif chosen == "invalid_state_code":
        record["state_code"] = draw(st.text(min_size=3, max_size=5).filter(lambda x: x.upper() not in VALID_STATES))

    elif chosen == "invalid_zip":
        record["zip_code"] = draw(st.text(min_size=1, max_size=4).filter(lambda x: not x.isdigit() or len(x) < 5))

    return record


@st.composite
def record_dataset(draw, min_size: int = 1, max_size: int = 20, allow_duplicates: bool = False) -> list[dict]:
    """
    Generate a list of valid records with unique account_id and tradeline_id.
    If allow_duplicates=True, may include duplicate account_ids.
    """
    size = draw(st.integers(min_value=min_size, max_value=max_size))
    records = []
    seen_accounts: set[str] = set()
    seen_tradelines: set[str] = set()

    for _ in range(size):
        record = draw(valid_record())
        if not allow_duplicates:
            # Ensure uniqueness
            while record["account_id"] in seen_accounts:
                record["account_id"] = str(uuid.uuid4())
            while record["tradeline_id"] in seen_tradelines:
                record["tradeline_id"] = str(uuid.uuid4())
        seen_accounts.add(record["account_id"])
        seen_tradelines.add(record["tradeline_id"])
        records.append(record)

    return records


# ── Shared Rule Pack Fixtures ─────────────────────────────────────────────────

@pytest.fixture
def schema_rules():
    with open("src/rules/schema_rules.json") as f:
        return json.load(f)


@pytest.fixture
def dq_rules():
    with open("src/rules/dq_rules.json") as f:
        return json.load(f)


@pytest.fixture
def business_rules():
    with open("src/rules/business_rules.json") as f:
        return json.load(f)


@pytest.fixture
def state_rules():
    with open("src/rules/state_rules.json") as f:
        return json.load(f)
