"""
Status Lambda handler.
Serves GET /runs/{run_id} and GET /runs/{run_id}/results via API Gateway.
"""
from __future__ import annotations
import json
import os
import boto3
from boto3.dynamodb.conditions import Key

_dynamodb = None


def _get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def _get_run(run_id: str, runs_table) -> dict | None:
    resp = runs_table.get_item(Key={"run_id": run_id})
    return resp.get("Item")


def _get_results(run_id: str, results_table, limit: int, last_key: dict | None) -> tuple[list, dict | None]:
    kwargs: dict = {
        "KeyConditionExpression": Key("run_id").eq(run_id),
        "Limit": limit,
    }
    if last_key:
        kwargs["ExclusiveStartKey"] = last_key

    resp = results_table.query(**kwargs)
    return resp.get("Items", []), resp.get("LastEvaluatedKey")


def handler(event: dict, context) -> dict:
    """
    API Gateway HTTP API handler.
    Routes:
      GET /runs/{run_id}          → run summary
      GET /runs/{run_id}/results  → paginated validation results
    """
    runs_table_name = os.environ.get("RUNS_TABLE", "ValidationRuns")
    results_table_name = os.environ.get("RESULTS_TABLE", "ValidationResults")

    db = _get_dynamodb()
    runs_table = db.Table(runs_table_name)
    results_table = db.Table(results_table_name)

    path = event.get("rawPath", event.get("path", ""))
    path_params = event.get("pathParameters") or {}
    run_id = path_params.get("run_id", "")

    if not run_id:
        return _response(400, {"error": "run_id is required"})

    # GET /runs/{run_id}/results
    if path.endswith("/results"):
        run = _get_run(run_id, runs_table)
        if not run:
            return _response(404, {"error": f"Run '{run_id}' not found"})

        query_params = event.get("queryStringParameters") or {}
        limit = int(query_params.get("limit", 50))
        last_key_raw = query_params.get("last_evaluated_key")
        last_key = json.loads(last_key_raw) if last_key_raw else None

        items, next_key = _get_results(run_id, results_table, limit, last_key)

        body: dict = {"run_id": run_id, "results": items}
        if next_key:
            body["last_evaluated_key"] = json.dumps(next_key, default=str)

        return _response(200, body)

    # GET /runs/{run_id}
    run = _get_run(run_id, runs_table)
    if not run:
        return _response(404, {"error": f"Run '{run_id}' not found"})

    return _response(200, run)
