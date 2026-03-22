"""Unit tests for src/handlers/business_validate.py"""
import pytest
from src.handlers.business_validate import validate_record


def _base_record():
    return {
        "record_id": "R1",
        "product_type": "CREDIT_CARD",
        "current_balance": 1000.0,
        "credit_limit": 5000.0,
        "available_credit": 4000.0,
        "past_due_amount": 0.0,
        "payment_status": "CURRENT",
        "account_status": "OPEN",
        "delinquency_bucket": "CURRENT",
        "close_date": None,
    }


def test_valid_record_passes(business_rules):
    reasons = validate_record(_base_record(), business_rules)
    assert reasons == []


def test_wrong_product_type(business_rules):
    record = _base_record()
    record["product_type"] = "DEBIT_CARD"
    reasons = validate_record(record, business_rules)
    assert "BUS_WRONG_PRODUCT_TYPE" in reasons


def test_negative_balance(business_rules):
    record = _base_record()
    record["current_balance"] = -100.0
    reasons = validate_record(record, business_rules)
    assert "BUS_NEGATIVE_BALANCE" in reasons


def test_zero_credit_limit(business_rules):
    record = _base_record()
    record["credit_limit"] = 0.0
    reasons = validate_record(record, business_rules)
    assert "BUS_INVALID_CREDIT_LIMIT" in reasons


def test_negative_credit_limit(business_rules):
    record = _base_record()
    record["credit_limit"] = -500.0
    reasons = validate_record(record, business_rules)
    assert "BUS_INVALID_CREDIT_LIMIT" in reasons


def test_available_credit_mismatch(business_rules):
    record = _base_record()
    record["available_credit"] = 9999.0  # should be 4000
    reasons = validate_record(record, business_rules)
    assert "BUS_AVAILABLE_CREDIT_MISMATCH" in reasons


def test_available_credit_correct(business_rules):
    record = _base_record()
    record["available_credit"] = 4000.0
    reasons = validate_record(record, business_rules)
    assert "BUS_AVAILABLE_CREDIT_MISMATCH" not in reasons


def test_past_due_conflict(business_rules):
    record = _base_record()
    record["payment_status"] = "CURRENT"
    record["past_due_amount"] = 150.0
    reasons = validate_record(record, business_rules)
    assert "BUS_PAST_DUE_CONFLICT" in reasons


def test_past_due_zero_when_current(business_rules):
    record = _base_record()
    record["payment_status"] = "CURRENT"
    record["past_due_amount"] = 0.0
    reasons = validate_record(record, business_rules)
    assert "BUS_PAST_DUE_CONFLICT" not in reasons


def test_closed_account_missing_close_date(business_rules):
    record = _base_record()
    record["account_status"] = "CLOSED"
    record["close_date"] = None
    reasons = validate_record(record, business_rules)
    assert "BUS_MISSING_CLOSE_DATE" in reasons


def test_closed_account_with_close_date(business_rules):
    record = _base_record()
    record["account_status"] = "CLOSED"
    record["close_date"] = "2025-12-01"
    reasons = validate_record(record, business_rules)
    assert "BUS_MISSING_CLOSE_DATE" not in reasons


def test_delinquency_mismatch(business_rules):
    record = _base_record()
    record["payment_status"] = "CURRENT"
    record["delinquency_bucket"] = "90_DAYS"
    reasons = validate_record(record, business_rules)
    assert "BUS_DELINQUENCY_MISMATCH" in reasons


def test_delinquency_consistent(business_rules):
    record = _base_record()
    record["payment_status"] = "30_DAYS"
    record["delinquency_bucket"] = "30_DAYS"
    record["past_due_amount"] = 100.0
    reasons = validate_record(record, business_rules)
    assert "BUS_DELINQUENCY_MISMATCH" not in reasons
