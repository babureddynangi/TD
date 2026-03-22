"""
End-to-end audit trace simulator.
Simulates a single run through the full pipeline and produces a structured
audit trace showing every step, stage result, and final outcome.

Usage:
    python scripts/run_audit_trace.py
"""
from __future__ import annotations
import json
import uuid
import time
from datetime import datetime, timezone, date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.handlers.schema_validate import validate_record as schema_validate_record
from src.handlers.dq_validate import validate_dataset as dq_validate_dataset
from src.handlers.business_validate import validate_record as business_validate_record
from src.handlers.state_validate import validate_record as state_validate_record
from src.utils.result_builder import finalize_result
from src.models.schemas import ValidationResult, RunRecord


def ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def main():
    rules_dir = Path("src/rules")
    schema_rules = json.loads((rules_dir / "schema_rules.json").read_text())
    dq_rules = json.loads((rules_dir / "dq_rules.json").read_text())
    business_rules = json.loads((rules_dir / "business_rules.json").read_text())
    state_rules = json.loads((rules_dir / "state_rules.json").read_text())

    input_file = Path("sample-data/input/sample_input.json")
    records_raw = json.loads(input_file.read_text())

    run_id = str(uuid.uuid4())
    execution_arn = f"arn:aws:states:us-east-1:123456789012:execution:cc-dq-pipeline-dev:{run_id}"

    trace = {
        "run_id": run_id,
        "execution_arn": execution_arn,
        "input_s3_key": f"input/{input_file.name}",
        "total_records": len(records_raw),
        "steps": [],
    }

    print(f"\n{'='*60}")
    print(f"AUDIT TRACE — run_id: {run_id}")
    print(f"{'='*60}")

    # Step 1: Ingest
    step1_ts = ts()
    run_record = RunRecord(
        run_id=run_id,
        status="RUNNING",
        input_s3_key=f"input/{input_file.name}",
        execution_arn=execution_arn,
        total_records=len(records_raw),
        created_at=step1_ts,
    )
    trace["steps"].append({
        "step": 1,
        "name": "Ingest",
        "timestamp": step1_ts,
        "action": f"Parsed {len(records_raw)} records from {input_file.name}",
        "run_record_created": run_record.to_dict(),
    })
    print(f"\nStep 1 — Ingest [{step1_ts}]")
    print(f"  Parsed {len(records_raw)} records")
    print(f"  Run record created: status=RUNNING")
    print(f"  Step Functions execution started: {execution_arn}")

    records = json.loads(json.dumps(records_raw))

    # Step 2: Schema Validation
    t0 = time.perf_counter()
    schema_hits = []
    for record in records:
        reasons = schema_validate_record(record, schema_rules)
        record["schema_status"] = "FAIL" if reasons else "PASS"
        record["failure_reasons"] = reasons
        if reasons:
            schema_hits.append({"record_id": record["record_id"], "reasons": reasons})
    schema_ms = round((time.perf_counter() - t0) * 1000, 1)

    step2_ts = ts()
    schema_fail_count = sum(1 for r in records if r["schema_status"] == "FAIL")
    trace["steps"].append({
        "step": 2,
        "name": "SchemaValidate",
        "timestamp": step2_ts,
        "duration_ms": schema_ms,
        "records_processed": len(records),
        "schema_fail_count": schema_fail_count,
        "rule_hits": schema_hits,
    })
    print(f"\nStep 2 — Schema Validate [{step2_ts}] ({schema_ms}ms)")
    print(f"  Records processed: {len(records)}")
    print(f"  Schema FAILs: {schema_fail_count}")
    for hit in schema_hits:
        print(f"    {hit['record_id']}: {hit['reasons']}")

    # Step 3: DQ Validation
    t0 = time.perf_counter()
    records = dq_validate_dataset(records, dq_rules, run_date=date(2026, 3, 1))
    dq_ms = round((time.perf_counter() - t0) * 1000, 1)

    step3_ts = ts()
    dq_fail_count = sum(1 for r in records if r.get("dq_status") == "FAIL")
    dq_hits = [
        {"record_id": r["record_id"], "reasons": [x for x in r.get("failure_reasons", []) if x.startswith("DQ_")]}
        for r in records if r.get("dq_status") == "FAIL"
    ]
    trace["steps"].append({
        "step": 3,
        "name": "DQValidate",
        "timestamp": step3_ts,
        "duration_ms": dq_ms,
        "records_processed": len(records),
        "dq_fail_count": dq_fail_count,
        "rule_hits": dq_hits,
    })
    print(f"\nStep 3 — DQ Validate [{step3_ts}] ({dq_ms}ms)")
    print(f"  Records processed: {len(records)}")
    print(f"  DQ FAILs: {dq_fail_count}")
    for hit in dq_hits:
        print(f"    {hit['record_id']}: {hit['reasons']}")

    # Step 4: Business Validation
    t0 = time.perf_counter()
    bus_hits = []
    for record in records:
        reasons = business_validate_record(record, business_rules)
        record["business_status"] = "FAIL" if reasons else "PASS"
        record["failure_reasons"] = record.get("failure_reasons", []) + reasons
        if reasons:
            bus_hits.append({"record_id": record["record_id"], "reasons": reasons})
    bus_ms = round((time.perf_counter() - t0) * 1000, 1)

    step4_ts = ts()
    bus_fail_count = sum(1 for r in records if r.get("business_status") == "FAIL")
    trace["steps"].append({
        "step": 4,
        "name": "BusinessValidate",
        "timestamp": step4_ts,
        "duration_ms": bus_ms,
        "records_processed": len(records),
        "business_fail_count": bus_fail_count,
        "rule_hits": bus_hits,
    })
    print(f"\nStep 4 — Business Validate [{step4_ts}] ({bus_ms}ms)")
    print(f"  Records processed: {len(records)}")
    print(f"  Business FAILs: {bus_fail_count}")
    for hit in bus_hits:
        print(f"    {hit['record_id']}: {hit['reasons']}")

    # Step 5: State Validation
    t0 = time.perf_counter()
    state_hits = []
    for record in records:
        state_status, reasons = state_validate_record(record, state_rules)
        record["state_status"] = state_status
        record["failure_reasons"] = record.get("failure_reasons", []) + reasons
        if state_status != "PASS":
            state_hits.append({"record_id": record["record_id"], "state_status": state_status, "reasons": reasons})
    state_ms = round((time.perf_counter() - t0) * 1000, 1)

    step5_ts = ts()
    state_fail_count = sum(1 for r in records if r.get("state_status") == "FAIL")
    review_count = sum(1 for r in records if r.get("state_status") == "REVIEW_REQUIRED")
    trace["steps"].append({
        "step": 5,
        "name": "StateValidate",
        "timestamp": step5_ts,
        "duration_ms": state_ms,
        "records_processed": len(records),
        "state_fail_count": state_fail_count,
        "review_required_count": review_count,
        "rule_hits": state_hits,
    })
    print(f"\nStep 5 — State Validate [{step5_ts}] ({state_ms}ms)")
    print(f"  Records processed: {len(records)}")
    print(f"  State FAILs: {state_fail_count}")
    print(f"  REVIEW_REQUIRED: {review_count}")
    for hit in state_hits:
        print(f"    {hit['record_id']}: {hit['state_status']} — {hit['reasons']}")

    # Step 6: Finalize
    t0 = time.perf_counter()
    final_results = []
    for record in records:
        result = ValidationResult(
            run_id=run_id,
            record_id=str(record.get("record_id", "")),
            schema_status=record.get("schema_status", "PASS"),
            dq_status=record.get("dq_status", "PASS"),
            business_status=record.get("business_status", "PASS"),
            state_status=record.get("state_status", "PASS"),
            failure_reasons=list(record.get("failure_reasons", [])),
        )
        result = finalize_result(result)
        final_results.append(result.to_dict())
    finalize_ms = round((time.perf_counter() - t0) * 1000, 1)

    passed = [r for r in final_results if r["overall_status"] == "PASS"]
    failed = [r for r in final_results if r["overall_status"] == "FAIL"]
    review = [r for r in final_results if r["state_status"] == "REVIEW_REQUIRED"]

    step6_ts = ts()
    trace["steps"].append({
        "step": 6,
        "name": "Finalize",
        "timestamp": step6_ts,
        "duration_ms": finalize_ms,
        "passed_count": len(passed),
        "failed_count": len(failed),
        "review_required_count": len(review),
        "s3_valid_key": f"valid/{run_id}/results.json",
        "s3_rejected_key": f"rejected/{run_id}/results.json",
    })
    print(f"\nStep 6 — Finalize [{step6_ts}] ({finalize_ms}ms)")
    print(f"  PASS:             {len(passed)}")
    print(f"  FAIL:             {len(failed)}")
    print(f"  REVIEW_REQUIRED:  {len(review)}")
    print(f"  Written to: valid/{run_id}/results.json")
    print(f"  Written to: rejected/{run_id}/results.json")

    # Final run record update
    completed_at = ts()
    run_record.status = "COMPLETE"
    run_record.passed_count = len(passed)
    run_record.failed_count = len(failed)
    run_record.completed_at = completed_at

    trace["completed_at"] = completed_at
    trace["final_run_record"] = run_record.to_dict()
    trace["final_results_sample"] = final_results[:5]

    total_ms = sum(
        s.get("duration_ms", 0) for s in trace["steps"] if "duration_ms" in s
    )
    trace["total_pipeline_ms"] = round(total_ms, 1)

    print(f"\n{'='*60}")
    print(f"RUN COMPLETE — {completed_at}")
    print(f"  Total pipeline time: {total_ms:.1f}ms")
    print(f"  Run queryable at: GET /runs/{run_id}")
    print(f"{'='*60}")

    # Write outputs
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    (results_dir / "audit_trace.json").write_text(json.dumps(trace, indent=2))
    (results_dir / "audit_valid.json").write_text(json.dumps(passed, indent=2))
    (results_dir / "audit_rejected.json").write_text(json.dumps(failed, indent=2))
    print(f"\nAudit trace written to results/audit_trace.json")


if __name__ == "__main__":
    main()
