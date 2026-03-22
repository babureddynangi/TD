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
    REVIEW_REQUIRED on state_status counts as non-PASS (overall = FAIL).
    """
    all_pass = (
        result.schema_status == "PASS"
        and result.dq_status == "PASS"
        and result.business_status == "PASS"
        and result.state_status == "PASS"
    )
    result.overall_status = "PASS" if all_pass else "FAIL"
    result.reportable = all_pass
    return result
