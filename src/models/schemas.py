"""
Core data models for the credit-card DQ validation pipeline.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class InputRecord:
    """A single credit-card tradeline entry from the input dataset."""
    record_id: str
    account_id: str
    tradeline_id: str
    product_type: str
    state_code: str
    zip_code: str
    reporting_date: str          # ISO 8601 YYYY-MM-DD
    account_status: str          # OPEN | CLOSED | DELINQUENT | ...
    payment_status: str          # CURRENT | 30_DAYS | 60_DAYS | 90_DAYS | ...
    current_balance: float
    credit_limit: float
    available_credit: float
    past_due_amount: float
    delinquency_bucket: str
    dispute_flag: bool
    close_date: Optional[str] = None  # ISO 8601, required when account_status=CLOSED

    @classmethod
    def from_dict(cls, data: dict) -> "InputRecord":
        return cls(
            record_id=str(data["record_id"]),
            account_id=str(data["account_id"]),
            tradeline_id=str(data["tradeline_id"]),
            product_type=str(data["product_type"]),
            state_code=str(data["state_code"]),
            zip_code=str(data["zip_code"]),
            reporting_date=str(data["reporting_date"]),
            account_status=str(data["account_status"]),
            payment_status=str(data["payment_status"]),
            current_balance=float(data["current_balance"]),
            credit_limit=float(data["credit_limit"]),
            available_credit=float(data["available_credit"]),
            past_due_amount=float(data["past_due_amount"]),
            delinquency_bucket=str(data["delinquency_bucket"]),
            dispute_flag=bool(data["dispute_flag"]),
            close_date=data.get("close_date"),
        )

    def to_dict(self) -> dict:
        return {
            "record_id": self.record_id,
            "account_id": self.account_id,
            "tradeline_id": self.tradeline_id,
            "product_type": self.product_type,
            "state_code": self.state_code,
            "zip_code": self.zip_code,
            "reporting_date": self.reporting_date,
            "account_status": self.account_status,
            "payment_status": self.payment_status,
            "current_balance": self.current_balance,
            "credit_limit": self.credit_limit,
            "available_credit": self.available_credit,
            "past_due_amount": self.past_due_amount,
            "delinquency_bucket": self.delinquency_bucket,
            "dispute_flag": self.dispute_flag,
            "close_date": self.close_date,
        }


@dataclass
class ValidationResult:
    """Per-record output written to DynamoDB and S3."""
    run_id: str
    record_id: str
    schema_status: str = "PASS"       # PASS | FAIL
    dq_status: str = "PASS"           # PASS | FAIL
    business_status: str = "PASS"     # PASS | FAIL
    state_status: str = "PASS"        # PASS | FAIL | REVIEW_REQUIRED
    overall_status: str = "PASS"      # PASS | FAIL | REVIEW_REQUIRED
    reportable: bool = True
    failure_reasons: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "record_id": self.record_id,
            "schema_status": self.schema_status,
            "dq_status": self.dq_status,
            "business_status": self.business_status,
            "state_status": self.state_status,
            "overall_status": self.overall_status,
            "reportable": self.reportable,
            "failure_reasons": list(self.failure_reasons),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ValidationResult":
        return cls(
            run_id=data["run_id"],
            record_id=data["record_id"],
            schema_status=data.get("schema_status", "PASS"),
            dq_status=data.get("dq_status", "PASS"),
            business_status=data.get("business_status", "PASS"),
            state_status=data.get("state_status", "PASS"),
            overall_status=data.get("overall_status", "PASS"),
            reportable=data.get("reportable", True),
            failure_reasons=list(data.get("failure_reasons", [])),
        )


@dataclass
class RunRecord:
    """Top-level run metadata stored in DynamoDB ValidationRuns table."""
    run_id: str
    status: str                        # RUNNING | COMPLETE | ERROR
    input_s3_key: str
    execution_arn: str = ""
    total_records: int = 0
    passed_count: int = 0
    failed_count: int = 0
    created_at: str = ""
    completed_at: str = ""

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "input_s3_key": self.input_s3_key,
            "execution_arn": self.execution_arn,
            "total_records": self.total_records,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RunRecord":
        return cls(
            run_id=data["run_id"],
            status=data["status"],
            input_s3_key=data["input_s3_key"],
            execution_arn=data.get("execution_arn", ""),
            total_records=int(data.get("total_records", 0)),
            passed_count=int(data.get("passed_count", 0)),
            failed_count=int(data.get("failed_count", 0)),
            created_at=data.get("created_at", ""),
            completed_at=data.get("completed_at", ""),
        )
