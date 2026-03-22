# Implementation Plan: Credit Card DQ Validation

## Overview

Incremental implementation of the AWS serverless credit-card data quality validation pipeline. Each task builds on the previous, ending with a fully wired system deployable via `sam deploy`.

## Tasks

- [x] 1. Project structure and core data models
  - Create the folder layout: `src/handlers/`, `src/rules/`, `src/models/`, `src/utils/`, `tests/unit/`, `tests/property/`
  - Implement `src/models/schemas.py` — `InputRecord`, `ValidationResult`, `RunRecord` dataclasses with all fields from the design data models
  - Implement `src/utils/result_builder.py` — helper to construct a `ValidationResult` from a record and accumulated failure reasons
  - Add `requirements.txt` with `boto3`, `hypothesis`, `pytest`, `python-dateutil`
  - _Requirements: 6.3, 6.4, 6.5_

- [x] 2. Rule pack loading and parser utilities
  - [x] 2.1 Implement `src/utils/parser.py`
    - `parse_csv(content: str) -> list[dict]` and `parse_json(content: str) -> list[dict]`
    - Raise `ParseError` on empty or malformed input
    - _Requirements: 1.2, 1.3_
  - [ ]* 2.2 Write property test for parser round trip
    - **Property: Parsing round trip** — for any list of dicts, `parse_json(json.dumps(records))` should return equivalent records
    - **Validates: Requirements 1.2**
  - [x] 2.3 Implement `src/utils/rule_loader.py`
    - `load_rule_pack(s3_client, bucket, key) -> dict` — loads and JSON-parses a rule pack from S3
    - Cache result in module-level variable for Lambda warm reuse
    - Raise `RulePackError` if key is missing or content is invalid JSON
    - _Requirements: 2.9, 5.1_

- [x] 3. Schema Validator Lambda
  - [x] 3.1 Implement `src/handlers/schema_validate.py`
    - `handler(event, context)` — receives `{run_id, records: list[dict]}`, returns annotated records
    - Check required fields present (→ `SCHEMA_MISSING_FIELD`)
    - Check field types match declared types (→ `SCHEMA_TYPE_MISMATCH`)
    - Check date fields are ISO 8601 (→ `SCHEMA_DATE_FORMAT`)
    - Check enum fields contain allowed values (→ `SCHEMA_INVALID_ENUM`)
    - Load schema from rule pack via `rule_loader`
    - _Requirements: 2.1–2.9_
  - [ ]* 3.2 Write property test for schema validation (Property 1)
    - **Property 1: Schema validation rejects structurally invalid records**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.7, 2.8**
  - [ ]* 3.3 Write unit tests for schema validator
    - Test: all-valid record passes; missing required field fails with correct code; wrong type fails; bad date format fails; bad enum fails
    - _Requirements: 2.1–2.8_

- [x] 4. DQ Validator Lambda
  - [x] 4.1 Implement `src/handlers/dq_validate.py`
    - `handler(event, context)` — receives annotated records from schema stage, returns further annotated records
    - Check mandatory fields non-null/non-empty (→ `DQ_NULL_MANDATORY`)
    - Check `account_id` uniqueness within dataset (→ `DQ_DUPLICATE_ACCOUNT_ID`)
    - Check `tradeline_id` uniqueness within dataset (→ `DQ_DUPLICATE_TRADELINE_ID`)
    - Check `state_code` is valid two-letter US abbreviation (→ `DQ_INVALID_STATE_CODE`)
    - Check ZIP code pattern (→ `DQ_INVALID_ZIP`)
    - Check `reporting_date` is not future (→ `DQ_FUTURE_REPORTING_DATE`)
    - _Requirements: 3.1–3.12_
  - [ ]* 4.2 Write property test for DQ null check (Property 2)
    - **Property 2: DQ validation rejects records with null mandatory fields**
    - **Validates: Requirements 3.1, 3.2**
  - [ ]* 4.3 Write property test for DQ duplicate detection (Property 3)
    - **Property 3: DQ duplicate detection**
    - **Validates: Requirements 3.3, 3.4, 3.5, 3.6**
  - [ ]* 4.4 Write unit tests for DQ validator
    - Test: valid record passes all DQ checks; null field fails; duplicate account_id fails; duplicate tradeline_id fails; invalid state code fails; bad ZIP fails; future date fails
    - _Requirements: 3.1–3.12_

- [ ] 5. Checkpoint — unit tests for schema and DQ validators pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Business Rule Validator Lambda
  - [x] 6.1 Implement `src/handlers/business_validate.py`
    - `handler(event, context)` — receives annotated records, returns further annotated records
    - Check `product_type == CREDIT_CARD` (→ `BUS_WRONG_PRODUCT_TYPE`)
    - Check `current_balance >= 0` (→ `BUS_NEGATIVE_BALANCE`)
    - Check `credit_limit > 0` (→ `BUS_INVALID_CREDIT_LIMIT`)
    - Check `available_credit == credit_limit - current_balance` (→ `BUS_AVAILABLE_CREDIT_MISMATCH`)
    - Check `past_due_amount == 0` when `payment_status == CURRENT` (→ `BUS_PAST_DUE_CONFLICT`)
    - Check `close_date` present when `account_status == CLOSED` (→ `BUS_MISSING_CLOSE_DATE`)
    - Check `delinquency_bucket` consistent with `payment_status` using business rules from rule pack (→ `BUS_DELINQUENCY_MISMATCH`)
    - _Requirements: 4.1–4.14_
  - [ ]* 6.2 Write property test for available credit invariant (Property 4)
    - **Property 4: Business rule — available credit invariant**
    - **Validates: Requirements 4.7, 4.8**
  - [ ]* 6.3 Write property test for payment status consistency (Property 5)
    - **Property 5: Business rule — payment status consistency**
    - **Validates: Requirements 4.9, 4.10**
  - [ ]* 6.4 Write property test for closed account close date (Property 6)
    - **Property 6: Business rule — closed account close date**
    - **Validates: Requirements 4.11, 4.12**
  - [ ]* 6.5 Write unit tests for business validator
    - Test each rule with a passing and failing example
    - _Requirements: 4.1–4.14_

- [x] 7. State Rule Validator Lambda
  - [x] 7.1 Implement `src/handlers/state_validate.py`
    - `handler(event, context)` — receives annotated records, returns further annotated records
    - Load `state_rules.json` rule pack
    - For each record: look up `state_code`; if no entry → `REVIEW_REQUIRED` + `STATE_NO_RULE_FOUND`
    - Apply `required_fields`, `disallowed_statuses`, and `extra_checks` from the matching state rule
    - _Requirements: 5.1–5.6_
  - [ ]* 7.2 Write property test for unknown state fallback (Property 7)
    - **Property 7: State rule — unknown state falls back to REVIEW_REQUIRED**
    - **Validates: Requirements 5.3**
  - [ ]* 7.3 Write unit tests for state validator
    - Test: known state with passing record; known state with disallowed status; unknown state_code → REVIEW_REQUIRED
    - _Requirements: 5.1–5.6_

- [x] 8. Finalize Lambda and result output
  - [x] 8.1 Implement `src/handlers/finalize.py`
    - `handler(event, context)` — receives all annotated records, computes `overall_status` and `reportable`
    - Write PASS records to S3 `/valid/` prefix as JSON
    - Write FAIL records to S3 `/rejected/` prefix as JSON with `failure_reasons`
    - Write all `ValidationResult` items to DynamoDB `ValidationResults` table
    - Update `ValidationRuns` record with `passed_count`, `failed_count`, `status=COMPLETE`
    - _Requirements: 6.1–6.6_
  - [ ]* 8.2 Write property test for overall status conjunction (Property 8)
    - **Property 8: Overall status is the conjunction of all stage statuses**
    - **Validates: Requirements 6.1, 6.2, 6.3**
  - [ ]* 8.3 Write property test for non-empty failure reasons (Property 9)
    - **Property 9: Failure reasons are non-empty for every FAIL record**
    - **Validates: Requirements 6.4**
  - [ ]* 8.4 Write property test for DynamoDB serialization round trip (Property 10)
    - **Property 10: Validation result round trip (DynamoDB serialization)**
    - **Validates: Requirements 6.5**
  - [ ]* 8.5 Write unit tests for finalize handler
    - Test: all-pass dataset writes to /valid/; mixed dataset splits correctly; run record updated with correct counts
    - _Requirements: 6.1–6.6_

- [ ] 9. Checkpoint — all validator and finalize tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Ingest Lambda
  - Implement `src/handlers/ingest.py`
    - `handler(event, context)` — triggered by S3 event notification
    - Parse S3 event to get bucket and key
    - Read file from S3, detect format (CSV or JSON), call `parser.py`
    - If parse fails → write Run record with `status=ERROR`, reason `PARSE_ERROR`; return without starting Step Functions
    - Assign unique `run_id` (UUID)
    - Write `ValidationRuns` record to DynamoDB with `status=RUNNING`
    - Start Step Functions execution with `{run_id, bucket, key, records}`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 7.3, 7.5_

- [x] 11. Status Lambda and API Gateway
  - [x] 11.1 Implement `src/handlers/status.py`
    - `GET /runs/{run_id}` → read `ValidationRuns` item from DynamoDB; return 404 if not found
    - `GET /runs/{run_id}/results` → query `ValidationResults` with `run_id` partition key; support `limit` and `last_evaluated_key` for pagination
    - _Requirements: 8.1–8.4_
  - [ ]* 11.2 Write unit tests for status handler
    - Test: valid run_id returns run record; unknown run_id returns 404; results endpoint returns paginated list
    - _Requirements: 8.1–8.4_

- [x] 12. SAM template and rule pack seed files
  - [x] 12.1 Create `template.yaml`
    - Define S3 bucket with event notification to `ingest` Lambda
    - Define all six Lambda functions with IAM roles (S3 read/write, DynamoDB read/write, Step Functions start)
    - Define Step Functions Standard Workflow with ASL matching the design state machine
    - Define `ValidationRuns` and `ValidationResults` DynamoDB tables
    - Define API Gateway HTTP API with routes for status Lambda
    - Use SAM parameters for environment-specific values (bucket name, table names, rule pack prefix)
    - _Requirements: 9.1–9.5_
  - [x] 12.2 Create seed rule pack JSON files
    - `src/rules/schema_rules.json` — required fields list, field types map, enum definitions
    - `src/rules/dq_rules.json` — mandatory fields list, uniqueness keys, format patterns
    - `src/rules/business_rules.json` — delinquency bucket to payment_status mapping, allowed product types
    - `src/rules/state_rules.json` — entries for CA, NY, TX, FL, IL, WA, GA, OH, PA, AZ with required_fields, disallowed_statuses, extra_checks
    - _Requirements: 2.9, 4.13, 5.1_

- [x] 13. Sample data and conftest generators
  - [x] 13.1 Create `tests/conftest.py`
    - Implement `valid_record`, `invalid_record`, and `record_dataset` Hypothesis composite strategies as specified in the design
    - _Requirements: all property tests_
  - [x] 13.2 Create `sample-data/input/sample_input.json` — 20 synthetic records covering pass, fail, and REVIEW_REQUIRED cases
  - [x] 13.3 Create `sample-data/expected-valid/` and `sample-data/expected-rejected/` with expected outputs for the sample input

- [x] 14. Final checkpoint — full test suite passes
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Each task references specific requirements for traceability
- Property tests use Hypothesis with `@settings(max_examples=100)` minimum
- Unit tests use pytest; no mocks for core validation logic — only for AWS SDK calls
- All Lambda handlers follow the same contract: receive `{run_id, records}`, return annotated records
