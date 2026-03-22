"""Unit tests for src/handlers/schema_validate.py"""
import json
import pytest
from src.handlers.schema_validate import validate_record


@pytest.fixture
def schema(schema_rules):
    return schema_rules


def _base_record():
    return {
        "record_id": "R1",
        "account_id": "A1",
        "tradeline_id": "T1",
        "product_type": "CREDIT_CARD",
        "state_code": "CA",
        "zip_code": "90210",
        "reporting_date": "2026-01-15",
        "account_status": "OPEN",
        "payment_status": "CURRENT",
        "current_balance": 1000.0,
        "credit_limit": 5000.0,
        "available_credit": 4000.0,
        "past_due_amount": 0.0,
        "delinquency_bucket": "CURRENT",
        "dispute_flag": False,
        "close_date": None,
    }


def test_valid_record_passes(schema):
    reasons = validate_record(_base_record(), schema)
    assert reasons == []


def test_missing_required_field(schema):
    record = _base_record()
    del record["account_id"]
    reasons = validate_record(record, schema)
    assert any("SCHEMA_MISSING_FIELD:account_id" in r for r in reasons)


def test_type_mismatch_balance(schema):
    record = _base_record()
    record["current_balance"] = "not_a_number"
    reasons = validate_record(record, schema)
    assert any("SCHEMA_TYPE_MISMATCH:current_balance" in r for r in reasons)


def test_invalid_date_format(schema):
    record = _base_record()
    record["reporting_date"] = "15-01-2026"  # wrong format
    reasons = validate_record(record, schema)
    assert any("SCHEMA_DATE_FORMAT:reporting_date" in r for r in reasons)


def test_invalid_enum_product_type(schema):
    record = _base_record()
    record["product_type"] = "DEBIT_CARD"
    reasons = validate_record(record, schema)
    assert any("SCHEMA_INVALID_ENUM:product_type" in r for r in reasons)


def test_invalid_enum_account_status(schema):
    record = _base_record()
    record["account_status"] = "UNKNOWN_STATUS"
    reasons = validate_record(record, schema)
    assert any("SCHEMA_INVALID_ENUM:account_status" in r for r in reasons)


def test_valid_date_passes(schema):
    record = _base_record()
    record["reporting_date"] = "2025-12-31"
    reasons = validate_record(record, schema)
    assert not any("SCHEMA_DATE_FORMAT" in r for r in reasons)


def test_impossible_date_fails(schema):
    record = _base_record()
    record["reporting_date"] = "2026-02-30"  # Feb 30 doesn't exist
    reasons = validate_record(record, schema)
    assert any("SCHEMA_DATE_FORMAT:reporting_date" in r for r in reasons)
