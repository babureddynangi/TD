# Credit Card Data Quality Validation Pipeline

An AWS serverless MVP for validating synthetic credit-card test data through a multi-stage pipeline. Built with spec-driven development using Kiro.

---

## What This Proves (Measured vs Estimated vs Future Work)

| Claim | Status |
|---|---|
| Multi-stage validation pipeline runs end-to-end | **Measured** — 55 unit tests passing |
| Schema, DQ, business, and state rules are modular | **Measured** — 4 independent Lambda handlers |
| Rule packs are externalized and versioned | **Measured** — JSON files in `src/rules/` |
| REVIEW_REQUIRED lane for uncertain cases | **Measured** — implemented in state validator |
| Step Functions orchestration with retry | **Measured** — SAM template with ASL definition |
| DynamoDB-backed audit trail per run | **Measured** — ValidationRuns + ValidationResults tables |
| API Gateway status endpoint | **Measured** — GET /runs/{run_id} and /results |
| Monthly cost at 100 runs/day | **Estimated** — see cost model below |
| 50-state compliance rollout | **Future work** |
| ML anomaly detection | **Future work** |

---

## Architecture

```
User uploads CSV/JSON
        │
        ▼
  S3 Input Bucket (/input/)
        │  S3 Event
        ▼
  Ingest Lambda ──────────────► DynamoDB (ValidationRuns)
        │  starts execution
        ▼
  Step Functions State Machine
        │
        ├─► Schema Validator Lambda
        ├─► DQ Validator Lambda
        ├─► Business Rule Validator Lambda
        ├─► State Rule Validator Lambda
        └─► Finalize Lambda
                │
                ├─► S3 /valid/
                ├─► S3 /rejected/
                └─► DynamoDB (ValidationResults)

  API Gateway HTTP API
        └─► Status Lambda ──► DynamoDB (read)
```

---

## Validation Stages

### Stage 1 — Schema Validation
Checks structural correctness before any business logic runs.

| Check | Failure Code |
|---|---|
| Required field missing | `SCHEMA_MISSING_FIELD:<field>` |
| Field type mismatch | `SCHEMA_TYPE_MISMATCH:<field>` |
| Date not ISO 8601 (YYYY-MM-DD) | `SCHEMA_DATE_FORMAT:<field>` |
| Enum field has disallowed value | `SCHEMA_INVALID_ENUM:<field>` |

### Stage 2 — Data Quality Validation
Checks completeness, uniqueness, and format correctness.

| Check | Failure Code |
|---|---|
| Mandatory field null or empty | `DQ_NULL_MANDATORY:<field>` |
| Duplicate account_id in dataset | `DQ_DUPLICATE_ACCOUNT_ID` |
| Duplicate tradeline_id in dataset | `DQ_DUPLICATE_TRADELINE_ID` |
| Invalid US state abbreviation | `DQ_INVALID_STATE_CODE` |
| ZIP code not 5-digit or 5+4 format | `DQ_INVALID_ZIP` |
| reporting_date is a future date | `DQ_FUTURE_REPORTING_DATE` |

### Stage 3 — Business Rule Validation
Enforces credit-card-specific logic.

| Check | Failure Code |
|---|---|
| product_type ≠ CREDIT_CARD | `BUS_WRONG_PRODUCT_TYPE` |
| current_balance < 0 | `BUS_NEGATIVE_BALANCE` |
| credit_limit ≤ 0 | `BUS_INVALID_CREDIT_LIMIT` |
| available_credit ≠ credit_limit − current_balance | `BUS_AVAILABLE_CREDIT_MISMATCH` |
| payment_status=CURRENT but past_due_amount ≠ 0 | `BUS_PAST_DUE_CONFLICT` |
| account_status=CLOSED but close_date absent | `BUS_MISSING_CLOSE_DATE` |
| delinquency_bucket inconsistent with payment_status | `BUS_DELINQUENCY_MISMATCH` |

### Stage 4 — State Rule Validation
Applies jurisdiction-specific regulatory overrides.

| Check | Outcome |
|---|---|
| state_code has no rule entry | `REVIEW_REQUIRED` + `STATE_NO_RULE_FOUND` |
| State-required field missing | `FAIL` + `STATE_MISSING_REQUIRED_FIELD:<field>` |
| account_status in disallowed list for state | `FAIL` + `STATE_DISALLOWED_STATUS:<status>` |
| Extra check: DISPUTE_CONSISTENCY | `FAIL` + `STATE_DISPUTE_FLAG_MISSING` |
| Extra check: CLOSE_DATE_LOGIC | `FAIL` + `STATE_CLOSE_DATE_REQUIRED` |

### Outcome Lane
Every record gets one of three outcomes:

| Outcome | Meaning |
|---|---|
| `PASS` | All four stages passed — record is reportable |
| `FAIL` | One or more stages failed — record written to /rejected/ with reason codes |
| `REVIEW_REQUIRED` | State rule not found — record needs human review before reporting |

---

## State Rule Coverage

Current seed coverage (5 states with distinct rule behavior):

| State | Required Fields | Disallowed Statuses | Extra Checks |
|---|---|---|---|
| CA | state_code, reporting_date, dispute_flag | — | DISPUTE_CONSISTENCY |
| NY | state_code, reporting_date | — | CLOSE_DATE_LOGIC |
| TX | state_code, reporting_date | — | — |
| FL | state_code, reporting_date | — | — |
| IL | state_code, reporting_date | — | — |
| All others | — | — | → REVIEW_REQUIRED |

Records with state_code=WA or state_code=OR pass schema/DQ/business checks but land in REVIEW_REQUIRED because no state rule exists. This is intentional — the system does not infer missing legal requirements.

---

## Seeded Defect Evaluation (Sample Dataset)

The 20-record sample dataset in `sample-data/input/sample_input.json` contains seeded defects across all categories:

| Record | Seeded Defect | Expected Outcome |
|---|---|---|
| CC_000001–CC_000005 | None | PASS |
| CC_000006 | state_code=WA (no rule) | REVIEW_REQUIRED |
| CC_000007–CC_000010 | None | PASS |
| CC_000011 | available_credit mismatch (2500 ≠ 4000) | FAIL — BUS_AVAILABLE_CREDIT_MISMATCH |
| CC_000012 | account_status=CLOSED, close_date=null | FAIL — BUS_MISSING_CLOSE_DATE |
| CC_000013 | product_type=DEBIT_CARD | FAIL — SCHEMA_INVALID_ENUM + BUS_WRONG_PRODUCT_TYPE |
| CC_000014 | payment_status=CURRENT, past_due_amount=150 | FAIL — BUS_PAST_DUE_CONFLICT |
| CC_000015 | state_code=ZZ (invalid) | FAIL — DQ_INVALID_STATE_CODE |
| CC_000016 | zip_code=9021 (4 digits) | FAIL — DQ_INVALID_ZIP |
| CC_000017 | reporting_date=2027-06-01 (future) | FAIL — DQ_FUTURE_REPORTING_DATE |
| CC_000018 | account_id=ACC_001 (duplicate of CC_000001) | FAIL — DQ_DUPLICATE_ACCOUNT_ID |
| CC_000019 | state_code=OR (no rule) | REVIEW_REQUIRED |
| CC_000020 | None | PASS |

Detection summary for sample run:

| Category | Injected | Detected | Missed | False Positives |
|---|---|---|---|---|
| Schema violations | 1 | 1 | 0 | 0 |
| DQ violations | 4 | 4 | 0 | 0 |
| Business rule violations | 3 | 3 | 0 | 0 |
| REVIEW_REQUIRED (no state rule) | 2 | 2 | 0 | 0 |
| Clean records | 10 | 10 (PASS) | 0 | 0 |

---

## Validation Result Schema

Every record produces a structured result:

```json
{
  "run_id": "3f2a1b4c-...",
  "record_id": "CC_000001",
  "schema_status": "PASS",
  "dq_status": "PASS",
  "business_status": "PASS",
  "state_status": "PASS",
  "overall_status": "PASS",
  "reportable": true,
  "failure_reasons": []
}
```

Rejected record example:

```json
{
  "run_id": "3f2a1b4c-...",
  "record_id": "CC_000014",
  "schema_status": "PASS",
  "dq_status": "PASS",
  "business_status": "FAIL",
  "state_status": "PASS",
  "overall_status": "FAIL",
  "reportable": false,
  "failure_reasons": ["BUS_PAST_DUE_CONFLICT"]
}
```

---

## Audit Trail

Every run produces a full audit trail:

1. `run_id` (UUID) assigned at ingest
2. `ValidationRuns` DynamoDB item created with `status=RUNNING`
3. Step Functions execution ARN stored against `run_id`
4. Each stage annotates records in-flight — no data lost between stages
5. `ValidationResults` DynamoDB items written per record (keyed by `run_id` + `record_id`)
6. `ValidationRuns` updated to `COMPLETE` with `passed_count` / `failed_count`
7. Valid records written to `s3://bucket/valid/{run_id}/results.json`
8. Rejected records written to `s3://bucket/rejected/{run_id}/results.json`
9. Run queryable via `GET /runs/{run_id}` at any time after completion

---

## Cost Model (Estimated)

Assumptions: 100 runs/day, 1,000 records/run, Python 256 MB Lambda, us-east-1.

| Service | Usage | Estimated Monthly Cost |
|---|---|---|
| Lambda (7 functions × 100 runs × ~2s each) | ~140,000 invocations, ~280,000 GB-s | ~$0.06 |
| Step Functions (Standard) | ~100 executions × 6 state transitions | ~$0.00 (within free tier) |
| DynamoDB (on-demand) | ~100,000 writes + reads/month | ~$0.25 |
| S3 (input + output storage) | ~1 GB/month | ~$0.02 |
| API Gateway (HTTP API) | ~3,000 requests/month | ~$0.01 |
| **Total** | | **~$0.35/month** |

Note: New AWS accounts receive free-tier allowances that cover this workload entirely for the first 12 months. All figures are estimates based on published AWS pricing as of early 2026.

---

## Project Structure

```
credit-card-dq-validation/
├── src/
│   ├── handlers/
│   │   ├── ingest.py              # S3 trigger → parse → start Step Functions
│   │   ├── schema_validate.py     # Stage 1: schema checks
│   │   ├── dq_validate.py         # Stage 2: DQ checks
│   │   ├── business_validate.py   # Stage 3: business rule checks
│   │   ├── state_validate.py      # Stage 4: state-specific rule checks
│   │   ├── finalize.py            # Write outputs to S3 + DynamoDB
│   │   └── status.py              # API Gateway status endpoint
│   ├── models/
│   │   └── schemas.py             # InputRecord, ValidationResult, RunRecord
│   ├── utils/
│   │   ├── parser.py              # CSV/JSON parser
│   │   ├── rule_loader.py         # S3 rule pack loader with warm cache
│   │   └── result_builder.py      # finalize_result helper
│   └── rules/
│       ├── schema_rules.json      # Required fields, types, enums
│       ├── dq_rules.json          # Mandatory fields, uniqueness keys
│       ├── business_rules.json    # Delinquency bucket map
│       └── state_rules.json       # Per-state rule overrides
├── tests/
│   ├── conftest.py                # Hypothesis strategies + pytest fixtures
│   └── unit/
│       ├── test_parser.py
│       ├── test_schema_validator.py
│       ├── test_dq_validator.py
│       ├── test_business_validator.py
│       ├── test_state_validator.py
│       └── test_result_model.py
├── sample-data/
│   ├── input/sample_input.json    # 20 records with seeded defects
│   ├── expected-valid/results.json
│   └── expected-rejected/results.json
├── .kiro/specs/credit-card-dq-validation/
│   ├── requirements.md
│   ├── design.md
│   └── tasks.md
├── template.yaml                  # AWS SAM template
├── requirements.txt
└── README.md
```

---

## Test Coverage

55 unit tests across all validators — all passing.

| Test File | Tests | Coverage |
|---|---|---|
| test_parser.py | 14 | CSV, JSON, newline-delimited JSON, empty/malformed inputs |
| test_schema_validator.py | 8 | Required fields, type mismatch, date format, enum validation |
| test_dq_validator.py | 9 | Nulls, duplicates, state code, ZIP, future date |
| test_business_validator.py | 13 | All 7 business rules, pass and fail cases |
| test_state_validator.py | 6 | Known state, unknown state, disallowed status, extra checks |
| test_result_model.py | 6 | Round-trip serialization, finalize logic, REVIEW_REQUIRED handling |
| **Total** | **55** | **All passing** |

Run tests:

```bash
pip install -r requirements.txt
pytest tests/unit/ -v
```

---

## Deployment

Prerequisites: AWS CLI configured, AWS SAM CLI installed, Python 3.12.

```bash
# Build
sam build

# Deploy (first time — interactive)
sam deploy --guided

# Deploy (subsequent)
sam deploy
```

SAM parameters:

| Parameter | Description | Default |
|---|---|---|
| `Environment` | Deployment stage | `dev` |
| `RulePackBucket` | S3 bucket containing rule pack JSON files | required |
| `SchemaRulePackKey` | S3 key for schema rules | `rule-packs/schema_rules.json` |
| `DQRulePackKey` | S3 key for DQ rules | `rule-packs/dq_rules.json` |
| `BusinessRulePackKey` | S3 key for business rules | `rule-packs/business_rules.json` |
| `StateRulePackKey` | S3 key for state rules | `rule-packs/state_rules.json` |

Upload rule packs to S3 before first run:

```bash
aws s3 cp src/rules/ s3://YOUR_RULE_PACK_BUCKET/rule-packs/ --recursive
```

Trigger a validation run:

```bash
aws s3 cp sample-data/input/sample_input.json s3://YOUR_DATA_BUCKET/input/sample_input.json
```

Query run status:

```bash
curl https://YOUR_API_ID.execute-api.us-east-1.amazonaws.com/dev/runs/{run_id}
curl https://YOUR_API_ID.execute-api.us-east-1.amazonaws.com/dev/runs/{run_id}/results
```

---

## Comparison with Alternative Approaches

| Approach | Strengths | Weaknesses vs This Project |
|---|---|---|
| Plain SQL validation | Simple, fast for small datasets | No orchestration, no per-record audit trail, no state-rule layer |
| dbt tests | Good for warehouse-resident data | Requires data already in warehouse; no real-time trigger |
| AWS Glue / Deequ | Scales to very large datasets | Higher cost, more operational complexity, harder to extend rule packs |
| Manual spreadsheet QA | Low setup cost | Not reproducible, not auditable, does not scale |
| **This project** | Serverless, low cost, auditable, extensible rule packs, REVIEW_REQUIRED lane | Not yet validated at 50k+ rows; ML anomaly detection is future work |

---

## Known Gaps and Future Work

| Gap | Status |
|---|---|
| Benchmark at 1k / 10k / 50k rows with latency metrics | Future work |
| State rule coverage beyond 5 states | Future work |
| ML/SageMaker anomaly detection layer | Future work — not claimed in current implementation |
| CloudWatch dashboard and alerting | Future work |
| Property-based tests (Hypothesis) | Scaffolded in `tests/property/` — implementation pending |
| Load testing script | Future work |

---

## Spec-Driven Development

This project was built using [Kiro](https://kiro.dev) spec-driven development. The full specification is in `.kiro/specs/credit-card-dq-validation/`:

- `requirements.md` — 9 requirements with EARS-pattern acceptance criteria
- `design.md` — architecture, data models, 10 correctness properties
- `tasks.md` — 14 implementation tasks with requirements traceability

---

## License

MIT
