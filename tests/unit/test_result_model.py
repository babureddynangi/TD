"""Unit tests for ValidationResult model and result_builder."""
from src.models.schemas import ValidationResult, RunRecord
from src.utils.result_builder import build_result, finalize_result


def test_build_result_defaults():
    r = build_result("run-1", "rec-1")
    assert r.run_id == "run-1"
    assert r.record_id == "rec-1"
    assert r.schema_status == "PASS"
    assert r.dq_status == "PASS"
    assert r.business_status == "PASS"
    assert r.state_status == "PASS"
    assert r.failure_reasons == []


def test_finalize_all_pass():
    r = build_result("run-1", "rec-1")
    r = finalize_result(r)
    assert r.overall_status == "PASS"
    assert r.reportable is True


def test_finalize_schema_fail():
    r = build_result("run-1", "rec-1")
    r.schema_status = "FAIL"
    r = finalize_result(r)
    assert r.overall_status == "FAIL"
    assert r.reportable is False


def test_finalize_review_required_counts_as_fail():
    r = build_result("run-1", "rec-1")
    r.state_status = "REVIEW_REQUIRED"
    r = finalize_result(r)
    assert r.overall_status == "FAIL"
    assert r.reportable is False


def test_validation_result_round_trip():
    r = ValidationResult(
        run_id="run-1",
        record_id="rec-1",
        schema_status="PASS",
        dq_status="FAIL",
        business_status="PASS",
        state_status="REVIEW_REQUIRED",
        overall_status="FAIL",
        reportable=False,
        failure_reasons=["DQ_NULL_MANDATORY:account_id", "STATE_NO_RULE_FOUND"],
    )
    d = r.to_dict()
    r2 = ValidationResult.from_dict(d)
    assert r2.run_id == r.run_id
    assert r2.record_id == r.record_id
    assert r2.dq_status == r.dq_status
    assert r2.state_status == r.state_status
    assert r2.overall_status == r.overall_status
    assert r2.reportable == r.reportable
    assert r2.failure_reasons == r.failure_reasons


def test_run_record_round_trip():
    rr = RunRecord(
        run_id="run-1",
        status="COMPLETE",
        input_s3_key="input/test.json",
        execution_arn="arn:aws:states:us-east-1:123:execution:sm:run-1",
        total_records=10,
        passed_count=8,
        failed_count=2,
        created_at="2026-01-15T10:00:00Z",
        completed_at="2026-01-15T10:01:00Z",
    )
    d = rr.to_dict()
    rr2 = RunRecord.from_dict(d)
    assert rr2.run_id == rr.run_id
    assert rr2.passed_count == rr.passed_count
    assert rr2.failed_count == rr.failed_count
    assert rr2.execution_arn == rr.execution_arn
