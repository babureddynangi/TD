"""Unit tests for src/handlers/state_validate.py"""
import pytest
from src.handlers.state_validate import validate_record


def _base_record(state_code="CA"):
    return {
        "record_id": "R1",
        "state_code": state_code,
        "account_status": "OPEN",
        "reporting_date": "2026-01-15",
        "dispute_flag": False,
        "close_date": None,
    }


def test_known_state_passes(state_rules):
    record = _base_record("CA")
    status, reasons = validate_record(record, state_rules)
    assert status == "PASS"
    assert reasons == []


def test_unknown_state_review_required(state_rules):
    record = _base_record("OR")  # OR not in state_rules.json
    status, reasons = validate_record(record, state_rules)
    assert status == "REVIEW_REQUIRED"
    assert "STATE_NO_RULE_FOUND" in reasons


def test_disallowed_status(state_rules):
    # Inject a disallowed status for CA
    rules = {
        "CA": {
            "required_fields": [],
            "disallowed_statuses": ["FROZEN"],
            "extra_checks": [],
        }
    }
    record = _base_record("CA")
    record["account_status"] = "FROZEN"
    status, reasons = validate_record(record, rules)
    assert status == "FAIL"
    assert "STATE_DISALLOWED_STATUS:FROZEN" in reasons


def test_missing_required_field_for_state(state_rules):
    rules = {
        "NY": {
            "required_fields": ["reporting_date", "zip_code"],
            "disallowed_statuses": [],
            "extra_checks": [],
        }
    }
    record = _base_record("NY")
    # zip_code is missing
    status, reasons = validate_record(record, rules)
    assert status == "FAIL"
    assert "STATE_MISSING_REQUIRED_FIELD:zip_code" in reasons


def test_extra_check_dispute_consistency_missing_flag(state_rules):
    rules = {
        "CA": {
            "required_fields": [],
            "disallowed_statuses": [],
            "extra_checks": ["DISPUTE_CONSISTENCY"],
        }
    }
    record = _base_record("CA")
    record.pop("dispute_flag")  # remove the flag
    status, reasons = validate_record(record, rules)
    assert status == "FAIL"
    assert "STATE_DISPUTE_FLAG_MISSING" in reasons


def test_extra_check_close_date_logic(state_rules):
    rules = {
        "NY": {
            "required_fields": [],
            "disallowed_statuses": [],
            "extra_checks": ["CLOSE_DATE_LOGIC"],
        }
    }
    record = _base_record("NY")
    record["account_status"] = "CLOSED"
    record["close_date"] = None
    status, reasons = validate_record(record, rules)
    assert status == "FAIL"
    assert "STATE_CLOSE_DATE_REQUIRED" in reasons
