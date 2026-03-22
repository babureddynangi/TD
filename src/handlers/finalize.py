"""
Finalize Lambda handler.
Computes overall_status, writes valid/rejected records to S3,
stores ValidationResult items in DynamoDB, and updates the Run record.
"""
from __future__ import annotations
import json
import os
from datetime import datetime, timezone
import boto3
from src.utils.result_builder import finalize_result
from src.models.schemas import ValidationResult, RunRecord

_s3_client = None
_dynamodb = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def _build_validation_result(run_id: str, record: dict) -> ValidationResult:
    result = ValidationResult(
        run_id=run_id,
        record_id=str(record.get("record_id", "")),
        schema_status=record.get("schema_status", "PASS"),
        dq_status=record.get("dq_status", "PASS"),
        business_status=record.get("business_status", "PASS"),
        state_status=record.get("state_status", "PASS"),
        failure_reasons=list(record.get("failure_reasons", [])),
    )
    return finalize_result(result)


def handler(event: dict, context) -> dict:
    """
    Step Functions task handler (normal and error path).
    Input:  {run_id, bucket, key, records: list[dict]}
    Output: {run_id, passed_count, failed_count, status}
    """
    run_id: str = event["run_id"]
    bucket: str = event["bucket"]
    records: list[dict] = event.get("records", [])
    is_error_path: bool = event.get("error", False)

    output_bucket = os.environ.get("OUTPUT_BUCKET", bucket)
    runs_table_name = os.environ.get("RUNS_TABLE", "ValidationRuns")
    results_table_name = os.environ.get("RESULTS_TABLE", "ValidationResults")

    s3 = _get_s3()
    db = _get_dynamodb()
    runs_table = db.Table(runs_table_name)
    results_table = db.Table(results_table_name)

    valid_records = []
    rejected_records = []
    review_records = []
    passed_count = 0
    failed_count = 0
    review_count = 0

    for record in records:
        result = _build_validation_result(run_id, record)

        # Write result to DynamoDB
        results_table.put_item(Item=result.to_dict())

        if result.overall_status == "PASS":
            valid_records.append(result.to_dict())
            passed_count += 1
        elif result.overall_status == "REVIEW_REQUIRED":
            review_records.append(result.to_dict())
            review_count += 1
        else:
            rejected_records.append(result.to_dict())
            failed_count += 1

    # Write valid output to S3
    if valid_records:
        s3.put_object(
            Bucket=output_bucket,
            Key=f"valid/{run_id}/results.json",
            Body=json.dumps(valid_records, default=str),
            ContentType="application/json",
        )

    # Write rejected output to S3
    if rejected_records:
        s3.put_object(
            Bucket=output_bucket,
            Key=f"rejected/{run_id}/results.json",
            Body=json.dumps(rejected_records, default=str),
            ContentType="application/json",
        )

    # Write review queue output to S3 (first-class REVIEW_REQUIRED lane)
    if review_records:
        s3.put_object(
            Bucket=output_bucket,
            Key=f"review/{run_id}/results.json",
            Body=json.dumps(review_records, default=str),
            ContentType="application/json",
        )

    completed_at = datetime.now(timezone.utc).isoformat()
    final_status = "ERROR" if is_error_path else "COMPLETE"

    # Update Run record
    runs_table.update_item(
        Key={"run_id": run_id},
        UpdateExpression=(
            "SET #st = :status, passed_count = :passed, failed_count = :failed, "
            "review_count = :review, total_records = :total, completed_at = :completed_at"
        ),
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":status": final_status,
            ":passed": passed_count,
            ":failed": failed_count,
            ":review": review_count,
            ":total": passed_count + failed_count + review_count,
            ":completed_at": completed_at,
        },
    )

    return {
        "run_id": run_id,
        "status": final_status,
        "passed_count": passed_count,
        "failed_count": failed_count,
        "review_count": review_count,
    }
