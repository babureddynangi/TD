"""
Helper to construct and finalize ValidationResult objects.
"""
from src.models.schemas import ValidationResult

_NON_PASS = {"FAIL", "REVIEW_REQUIRED"}


def build_result(run_id: str, record_id: str) -> ValidationResult:
    """Create a fresh ValidationResult with all stages defaulting to PASS."""
    return ValidationResult(run_id=run_id, record_id=record_id)


def finalize_result(result: ValidationResult) -> ValidationResult:
    """
    Compute overall_status and reportable from the four stage statuses.

    Outcome rules:
    - PASS        — all four stages are PASS; record is reportable
    - REVIEW_REQUIRED — state_status is REVIEW_REQUIRED (and no other stage failed);
                        record is not yet reportable, needs human review
    - FAIL        — any stage is FAIL; record is rejected
    """
    stage_statuses = [
        result.schema_status,
        result.dq_status,
        result.business_status,
        result.state_status,
    ]

    if any(s == "FAIL" for s in stage_statuses):
        result.overall_status = "FAIL"
        result.reportable = False
    elif result.state_status == "REVIEW_REQUIRED":
        result.overall_status = "REVIEW_REQUIRED"
        result.reportable = False
    else:
        result.overall_status = "PASS"
        result.reportable = True

    return result
