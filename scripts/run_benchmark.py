"""
Local benchmark runner for the credit-card DQ validation pipeline.
Runs the full validation pipeline (schema → DQ → business → state → finalize)
against small/medium/large datasets and produces metrics.

Usage:
    python scripts/run_benchmark.py
"""
from __future__ import annotations
import json
import time
import statistics
from collections import Counter
from datetime import date
from pathlib import Path

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.handlers.schema_validate import validate_record as schema_validate_record
from src.handlers.dq_validate import validate_dataset as dq_validate_dataset
from src.handlers.business_validate import validate_record as business_validate_record
from src.handlers.state_validate import validate_record as state_validate_record
from src.utils.result_builder import finalize_result, build_result
from src.models.schemas import ValidationResult


def load_rules() -> dict:
    rules_dir = Path("src/rules")
    return {
        "schema": json.loads((rules_dir / "schema_rules.json").read_text()),
        "dq": json.loads((rules_dir / "dq_rules.json").read_text()),
        "business": json.loads((rules_dir / "business_rules.json").read_text()),
        "state": json.loads((rules_dir / "state_rules.json").read_text()),
    }


def run_pipeline(records: list[dict], rules: dict, run_date: date) -> list[dict]:
    """Run the full 4-stage validation pipeline locally."""
    # Stage 1: Schema
    for record in records:
        reasons = schema_validate_record(record, rules["schema"])
        record["schema_status"] = "FAIL" if reasons else "PASS"
        record["failure_reasons"] = record.get("failure_reasons", []) + reasons

    # Stage 2: DQ
    records = dq_validate_dataset(records, rules["dq"], run_date=run_date)

    # Stage 3: Business
    for record in records:
        reasons = business_validate_record(record, rules["business"])
        record["business_status"] = "FAIL" if reasons else "PASS"
        record["failure_reasons"] = record.get("failure_reasons", []) + reasons

    # Stage 4: State
    for record in records:
        state_status, reasons = state_validate_record(record, rules["state"])
        record["state_status"] = state_status
        record["failure_reasons"] = record.get("failure_reasons", []) + reasons

    # Finalize
    results = []
    for record in records:
        result = ValidationResult(
            run_id="benchmark",
            record_id=str(record.get("record_id", "")),
            schema_status=record.get("schema_status", "PASS"),
            dq_status=record.get("dq_status", "PASS"),
            business_status=record.get("business_status", "PASS"),
            state_status=record.get("state_status", "PASS"),
            failure_reasons=list(record.get("failure_reasons", [])),
        )
        result = finalize_result(result)
        results.append(result.to_dict())

    return results


def evaluate(results: list[dict], truth: list[dict]) -> dict:
    """Compare pipeline results against truth labels."""
    truth_map = {t["record_id"]: t for t in truth}

    tp = fp = fn = tn = 0
    review_correct = review_missed = 0
    defect_stats: dict[str, dict] = {}

    for result in results:
        rid = result["record_id"]
        actual = result["overall_status"]
        truth_entry = truth_map.get(rid, {})
        expected = truth_entry.get("expected_outcome", "PASS")
        defect = truth_entry.get("defect_category", "clean")

        if defect not in defect_stats:
            defect_stats[defect] = {"injected": 0, "detected": 0, "missed": 0, "false_positive": 0}
        defect_stats[defect]["injected"] += 1

        if expected == "PASS":
            if actual == "PASS":
                tn += 1
            else:
                fp += 1
                defect_stats[defect]["false_positive"] += 1
        elif expected == "REVIEW_REQUIRED":
            if actual in ("FAIL", "REVIEW_REQUIRED"):
                review_correct += 1
                defect_stats[defect]["detected"] += 1
            else:
                review_missed += 1
                defect_stats[defect]["missed"] += 1
        else:  # expected FAIL
            if actual == "FAIL":
                tp += 1
                defect_stats[defect]["detected"] += 1
            else:
                fn += 1
                defect_stats[defect]["missed"] += 1

    total = len(results)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 1.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return {
        "total_records": total,
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "true_negatives": tn,
        "review_correct": review_correct,
        "review_missed": review_missed,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "defect_breakdown": defect_stats,
    }


def benchmark_dataset(name: str, rules: dict, repetitions: int = 5) -> dict:
    bench_dir = Path("benchmark-data")
    records_raw = json.loads((bench_dir / f"{name}_input.json").read_text())
    truth = json.loads((bench_dir / f"{name}_truth.json").read_text())

    run_date = date(2026, 3, 1)
    latencies_ms = []

    print(f"\nRunning {name} ({len(records_raw)} records) x{repetitions}...")

    last_results = None
    for i in range(repetitions):
        # Deep copy records each run (pipeline mutates in place)
        records = json.loads(json.dumps(records_raw))
        t0 = time.perf_counter()
        results = run_pipeline(records, rules, run_date)
        t1 = time.perf_counter()
        latencies_ms.append((t1 - t0) * 1000)
        last_results = results
        print(f"  Run {i+1}: {latencies_ms[-1]:.1f}ms")

    outcome_counts = Counter(r["overall_status"] for r in last_results)
    evaluation = evaluate(last_results, truth)

    metrics = {
        "dataset": name,
        "record_count": len(records_raw),
        "repetitions": repetitions,
        "latency_ms": {
            "mean": round(statistics.mean(latencies_ms), 1),
            "min": round(min(latencies_ms), 1),
            "max": round(max(latencies_ms), 1),
            "p95": round(sorted(latencies_ms)[int(len(latencies_ms) * 0.95)], 1),
        },
        "throughput_records_per_sec": round(len(records_raw) / (statistics.mean(latencies_ms) / 1000), 0),
        "outcome_counts": dict(outcome_counts),
        "evaluation": evaluation,
    }

    return metrics, last_results


def main():
    rules = load_rules()
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)

    all_metrics = []

    for name in ["small", "medium", "large"]:
        bench_path = Path("benchmark-data") / f"{name}_input.json"
        if not bench_path.exists():
            print(f"Skipping {name} — run generate_benchmark_data.py first")
            continue

        metrics, results = benchmark_dataset(name, rules, repetitions=5)
        all_metrics.append(metrics)

        # Write per-dataset results
        (results_dir / f"{name}_results.json").write_text(json.dumps(results, indent=2))
        (results_dir / f"{name}_metrics.json").write_text(json.dumps(metrics, indent=2))

        print(f"\n  {name.upper()} METRICS:")
        print(f"    Mean latency:  {metrics['latency_ms']['mean']}ms")
        print(f"    p95 latency:   {metrics['latency_ms']['p95']}ms")
        print(f"    Throughput:    {metrics['throughput_records_per_sec']:.0f} records/sec")
        print(f"    Outcomes:      {metrics['outcome_counts']}")
        print(f"    Precision:     {metrics['evaluation']['precision']}")
        print(f"    Recall:        {metrics['evaluation']['recall']}")
        print(f"    F1:            {metrics['evaluation']['f1']}")

    # Write combined summary
    (results_dir / "benchmark_summary.json").write_text(json.dumps(all_metrics, indent=2))
    print(f"\nAll results written to results/")


if __name__ == "__main__":
    main()
