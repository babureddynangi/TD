"""Unit tests for src/handlers/dq_validate.py"""
from datetime import date
import pytest
from src.handlers.dq_validate import validate_dataset


def _base_record(record_id="R1", account_id="A1", tradeline_id="T1"):
    return {
        "record_id": record_id,
        "account_id": account_id,
        "tradeline_id": tradeline_id,
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
    }


def test_valid_record_passes(dq_rules):
    records = [_base_record()]
    result = validate_dataset(records, dq_rules, run_date=date(2026, 3, 1))
    assert result[0]["dq_status"] == "PASS"
    assert result[0]["failure_reasons"] == []


def test_null_mandatory_field(dq_rules):
    record = _base_record()
    record["account_id"] = ""
    result = validate_dataset([record], dq_rules, run_date=date(2026, 3, 1))
    assert result[0]["dq_status"] == "FAIL"
    assert "DQ_NULL_MANDATORY:account_id" in result[0]["failure_reasons"]


def test_duplicate_account_id(dq_rules):
    r1 = _base_record("R1", "ACC_DUP", "T1")
    r2 = _base_record("R2", "ACC_DUP", "T2")
    result = validate_dataset([r1, r2], dq_rules, run_date=date(2026, 3, 1))
    assert result[0]["dq_status"] == "FAIL"
    assert result[1]["dq_status"] == "FAIL"
    assert "DQ_DUPLICATE_ACCOUNT_ID" in result[0]["failure_reasons"]
    assert "DQ_DUPLICATE_ACCOUNT_ID" in result[1]["failure_reasons"]


def test_duplicate_tradeline_id(dq_rules):
    r1 = _base_record("R1", "A1", "TL_DUP")
    r2 = _base_record("R2", "A2", "TL_DUP")
    result = validate_dataset([r1, r2], dq_rules, run_date=date(2026, 3, 1))
    assert "DQ_DUPLICATE_TRADELINE_ID" in result[0]["failure_reasons"]
    assert "DQ_DUPLICATE_TRADELINE_ID" in result[1]["failure_reasons"]


def test_invalid_state_code(dq_rules):
    record = _base_record()
    record["state_code"] = "ZZ"
    result = validate_dataset([record], dq_rules, run_date=date(2026, 3, 1))
    assert "DQ_INVALID_STATE_CODE" in result[0]["failure_reasons"]


def test_invalid_zip_too_short(dq_rules):
    record = _base_record()
    record["zip_code"] = "9021"
    result = validate_dataset([record], dq_rules, run_date=date(2026, 3, 1))
    assert "DQ_INVALID_ZIP" in result[0]["failure_reasons"]


def test_valid_zip_plus4(dq_rules):
    record = _base_record()
    record["zip_code"] = "90210-1234"
    result = validate_dataset([record], dq_rules, run_date=date(2026, 3, 1))
    assert "DQ_INVALID_ZIP" not in result[0]["failure_reasons"]


def test_future_reporting_date(dq_rules):
    record = _base_record()
    record["reporting_date"] = "2027-01-01"
    result = validate_dataset([record], dq_rules, run_date=date(2026, 3, 1))
    assert "DQ_FUTURE_REPORTING_DATE" in result[0]["failure_reasons"]


def test_today_reporting_date_passes(dq_rules):
    record = _base_record()
    today = date.today()
    record["reporting_date"] = today.isoformat()
    result = validate_dataset([record], dq_rules, run_date=today)
    assert "DQ_FUTURE_REPORTING_DATE" not in result[0]["failure_reasons"]
