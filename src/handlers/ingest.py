"""
Ingest Lambda handler.
Triggered by S3 event notification. Parses the uploaded file,
creates a Run record in DynamoDB, and starts the Step Functions execution.
"""
from __future__ import annotations
import json
import os
import uuid
from datetime import datetime, timezone
import boto3
from src.utils.parser import parse_input, ParseError

_s3_client = None
_dynamodb = None
_sfn_client = None


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


def _get_sfn():
    global _sfn_client
    if _sfn_client is None:
        _sfn_client = boto3.client("stepfunctions")
    return _sfn_client


def _detect_format(key: str) -> str:
    """Infer file format from S3 key extension."""
    key_lower = key.lower()
    if key_lower.endswith(".json") or key_lower.endswith(".jsonl"):
        return "json"
    if key_lower.endswith(".csv"):
        return "csv"
    # Default to JSON
    return "json"


def _write_error_run(runs_table, run_id: str, s3_key: str, reason: str) -> None:
    runs_table.put_item(Item={
        "run_id": run_id,
        "status": "ERROR",
        "input_s3_key": s3_key,
        "execution_arn": "",
        "total_records": 0,
        "passed_count": 0,
        "failed_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "error_reason": reason,
    })


def handler(event: dict, context) -> dict:
    """
    S3 event handler.
    For each S3 record in the event, parse the file and start a validation run.
    """
    runs_table_name = os.environ.get("RUNS_TABLE", "ValidationRuns")
    state_machine_arn = os.environ.get("STATE_MACHINE_ARN", "")

    s3 = _get_s3()
    db = _get_dynamodb()
    sfn = _get_sfn()
    runs_table = db.Table(runs_table_name)

    results = []

    for s3_record in event.get("Records", []):
        bucket = s3_record["s3"]["bucket"]["name"]
        key = s3_record["s3"]["object"]["key"]
        run_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()

        # Read file from S3
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
        except Exception as exc:
            _write_error_run(runs_table, run_id, key, f"S3_READ_ERROR:{exc}")
            results.append({"run_id": run_id, "status": "ERROR"})
            continue

        # Parse file
        fmt = _detect_format(key)
        try:
            records = parse_input(content, fmt)
        except ParseError as exc:
            _write_error_run(runs_table, run_id, key, f"PARSE_ERROR:{exc}")
            results.append({"run_id": run_id, "status": "ERROR"})
            continue

        # Create Run record
        runs_table.put_item(Item={
            "run_id": run_id,
            "status": "RUNNING",
            "input_s3_key": key,
            "execution_arn": "",
            "total_records": len(records),
            "passed_count": 0,
            "failed_count": 0,
            "created_at": created_at,
            "completed_at": "",
        })

        # Start Step Functions execution
        execution_input = json.dumps({
            "run_id": run_id,
            "bucket": bucket,
            "key": key,
            "records": records,
        }, default=str)

        sfn_response = sfn.start_execution(
            stateMachineArn=state_machine_arn,
            name=run_id,
            input=execution_input,
        )
        execution_arn = sfn_response["executionArn"]

        # Update Run record with execution ARN
        runs_table.update_item(
            Key={"run_id": run_id},
            UpdateExpression="SET execution_arn = :arn",
            ExpressionAttributeValues={":arn": execution_arn},
        )

        results.append({"run_id": run_id, "status": "RUNNING", "execution_arn": execution_arn})

    return {"runs": results}
