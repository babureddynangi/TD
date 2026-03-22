# Requirements Document

## Introduction

An AWS serverless MVP for validating synthetic credit-card test data. Input records are uploaded to S3 and processed through a multi-stage validation pipeline (schema, data quality, business rules, state rules). Valid records are written to a separate output location; rejected records are written with failure reason codes. The system is orchestrated via Step Functions, with results stored in DynamoDB and optionally exposed through API Gateway.

## Glossary

- **Record**: A single credit-card tradeline entry in the input dataset (CSV or JSON)
- **Validation_Pipeline**: The ordered sequence of validation stages applied to each record
- **Schema_Validator**: Lambda function that checks required fields, data types, date formats, and enum values
- **DQ_Validator**: Lambda function that checks data quality rules (nulls, uniqueness, format correctness)
- **Business_Validator**: Lambda function that checks credit-card-specific business logic rules
- **State_Validator**: Lambda function that applies state-specific regulatory rule overrides
- **Rule_Engine**: The component that loads and evaluates rules from JSON rule packs stored in S3/DynamoDB
- **Run**: A single execution of the Validation_Pipeline triggered by an S3 upload event
- **Validation_Result**: A per-record output object containing stage statuses, overall status, and failure reason codes
- **Rule_Pack**: A JSON file containing rules for a specific validation stage (schema, DQ, business, state)
- **State_Rule**: A state-specific override or additional check applied based on the record's state_code field
- **Orchestrator**: The Step Functions state machine that sequences Lambda validators and handles branching

---

## Requirements

### Requirement 1: Input Ingestion

**User Story:** As a data engineer, I want to upload a CSV or JSON file of synthetic credit-card records to S3, so that the validation pipeline is automatically triggered.

#### Acceptance Criteria

1. WHEN a file is uploaded to the designated S3 input prefix, THE Orchestrator SHALL start a new Run within 60 seconds
2. THE Schema_Validator SHALL accept input files in both CSV and JSON formats
3. IF the uploaded file is empty or unparseable, THEN THE Schema_Validator SHALL mark all records as FAIL with reason code PARSE_ERROR and write them to the rejected output
4. THE System SHALL assign a unique run_id to each Run for traceability

---

### Requirement 2: Schema Validation

**User Story:** As a data engineer, I want each record validated against a defined schema, so that structurally invalid records are caught before business logic runs.

#### Acceptance Criteria

1. THE Schema_Validator SHALL verify that all required columns are present in the input record
2. WHEN a required field is missing, THE Schema_Validator SHALL assign status FAIL and reason code SCHEMA_MISSING_FIELD to that record
3. THE Schema_Validator SHALL verify that each field value matches its declared data type
4. WHEN a field value does not match its declared data type, THE Schema_Validator SHALL assign reason code SCHEMA_TYPE_MISMATCH
5. THE Schema_Validator SHALL verify that date fields conform to ISO 8601 format (YYYY-MM-DD)
6. WHEN a date field is not in ISO 8601 format, THE Schema_Validator SHALL assign reason code SCHEMA_DATE_FORMAT
7. THE Schema_Validator SHALL verify that enum fields contain only allowed values
8. WHEN an enum field contains a disallowed value, THE Schema_Validator SHALL assign reason code SCHEMA_INVALID_ENUM
9. THE Schema_Validator SHALL load the schema definition from a Rule_Pack stored in S3

---

### Requirement 3: Data Quality Validation

**User Story:** As a data engineer, I want each record checked for data quality issues, so that incomplete or duplicate records are identified.

#### Acceptance Criteria

1. THE DQ_Validator SHALL verify that all mandatory fields contain non-null, non-empty values
2. WHEN a mandatory field is null or empty, THE DQ_Validator SHALL assign reason code DQ_NULL_MANDATORY
3. THE DQ_Validator SHALL verify that account_id is unique within the input dataset
4. WHEN a duplicate account_id is detected, THE DQ_Validator SHALL assign reason code DQ_DUPLICATE_ACCOUNT_ID
5. THE DQ_Validator SHALL verify that tradeline_id is unique within the input dataset
6. WHEN a duplicate tradeline_id is detected, THE DQ_Validator SHALL assign reason code DQ_DUPLICATE_TRADELINE_ID
7. THE DQ_Validator SHALL verify that state_code is a valid two-letter US state abbreviation
8. WHEN state_code is invalid, THE DQ_Validator SHALL assign reason code DQ_INVALID_STATE_CODE
9. THE DQ_Validator SHALL verify that ZIP code fields match the pattern of 5 digits or 5+4 digits
10. WHEN a ZIP code does not match the expected pattern, THE DQ_Validator SHALL assign reason code DQ_INVALID_ZIP
11. THE DQ_Validator SHALL verify that reporting_date is not a future date relative to the Run's execution date
12. WHEN reporting_date is a future date, THE DQ_Validator SHALL assign reason code DQ_FUTURE_REPORTING_DATE

---

### Requirement 4: Credit-Card Business Rule Validation

**User Story:** As a compliance analyst, I want credit-card-specific business rules enforced on each record, so that logically inconsistent data is flagged before reporting.

#### Acceptance Criteria

1. THE Business_Validator SHALL verify that product_type equals CREDIT_CARD for all records in the pipeline
2. WHEN product_type is not CREDIT_CARD, THE Business_Validator SHALL assign reason code BUS_WRONG_PRODUCT_TYPE
3. THE Business_Validator SHALL verify that current_balance is greater than or equal to zero
4. WHEN current_balance is negative, THE Business_Validator SHALL assign reason code BUS_NEGATIVE_BALANCE
5. THE Business_Validator SHALL verify that credit_limit is greater than zero
6. WHEN credit_limit is zero or negative, THE Business_Validator SHALL assign reason code BUS_INVALID_CREDIT_LIMIT
7. THE Business_Validator SHALL verify that available_credit equals credit_limit minus current_balance
8. WHEN available_credit does not equal credit_limit minus current_balance, THE Business_Validator SHALL assign reason code BUS_AVAILABLE_CREDIT_MISMATCH
9. WHEN payment_status is CURRENT, THE Business_Validator SHALL verify that past_due_amount equals zero
10. WHEN payment_status is CURRENT and past_due_amount is not zero, THE Business_Validator SHALL assign reason code BUS_PAST_DUE_CONFLICT
11. WHEN account_status is CLOSED, THE Business_Validator SHALL verify that close_date is present
12. WHEN account_status is CLOSED and close_date is absent, THE Business_Validator SHALL assign reason code BUS_MISSING_CLOSE_DATE
13. THE Business_Validator SHALL verify that delinquency_bucket is consistent with payment_status according to the business rule definitions
14. WHEN delinquency_bucket is inconsistent with payment_status, THE Business_Validator SHALL assign reason code BUS_DELINQUENCY_MISMATCH

---

### Requirement 5: State-Rule Validation

**User Story:** As a compliance analyst, I want state-specific regulatory rules applied to each record, so that jurisdiction-specific requirements are enforced.

#### Acceptance Criteria

1. THE State_Validator SHALL load state rules from a Rule_Pack keyed by state_code
2. WHEN a state_code has a corresponding State_Rule entry, THE State_Validator SHALL apply all checks defined in that entry
3. WHEN a state_code has no corresponding State_Rule entry, THE State_Validator SHALL assign status REVIEW_REQUIRED and reason code STATE_NO_RULE_FOUND
4. WHEN a State_Rule defines required_fields, THE State_Validator SHALL verify those fields are present and non-empty
5. WHEN a State_Rule defines disallowed_statuses, THE State_Validator SHALL verify the record's account_status is not in that list
6. WHEN a State_Rule defines extra_checks, THE State_Validator SHALL execute each named check against the record

---

### Requirement 6: Validation Result Output

**User Story:** As a data engineer, I want validated records written to separate S3 locations with structured result objects, so that downstream consumers can easily distinguish valid from rejected data.

#### Acceptance Criteria

1. WHEN all four validation stages pass for a record, THE System SHALL write the record to the S3 /valid/ prefix with overall_status PASS
2. WHEN any validation stage fails for a record, THE System SHALL write the record to the S3 /rejected/ prefix with overall_status FAIL
3. THE System SHALL include per-stage statuses (schema_status, dq_status, business_status, state_status) in every Validation_Result
4. THE System SHALL include all accumulated failure_reasons as a list in every rejected Validation_Result
5. THE System SHALL write Validation_Result summaries to DynamoDB keyed by run_id and record_id
6. WHEN a Run completes, THE System SHALL update the Run record in DynamoDB with total counts of passed and failed records

---

### Requirement 7: Orchestration

**User Story:** As a platform engineer, I want the validation stages orchestrated by Step Functions, so that the pipeline is auditable, retryable, and easy to extend.

#### Acceptance Criteria

1. THE Orchestrator SHALL execute validation stages in the order: Schema → DQ → Business → State
2. WHEN a Lambda function fails with an unhandled exception, THE Orchestrator SHALL retry the stage up to 2 times before marking the Run as ERROR
3. THE Orchestrator SHALL pass the run_id and S3 input location between stages as state machine input
4. THE Orchestrator SHALL branch to a finalization step after all records have been processed regardless of individual record outcomes
5. THE System SHALL record the Step Functions execution ARN in the DynamoDB Run record for traceability

---

### Requirement 8: API Gateway Status Endpoint

**User Story:** As a data engineer, I want to query the status and summary results of a validation run via an HTTP endpoint, so that I can monitor progress and retrieve results without accessing DynamoDB directly.

#### Acceptance Criteria

1. THE System SHALL expose a GET /runs/{run_id} endpoint via API Gateway
2. WHEN a valid run_id is provided, THE System SHALL return the Run record including status, record counts, and execution ARN
3. WHEN an invalid or unknown run_id is provided, THE System SHALL return HTTP 404 with a descriptive error message
4. THE System SHALL expose a GET /runs/{run_id}/results endpoint that returns paginated Validation_Result records for the run

---

### Requirement 9: Infrastructure and Deployment

**User Story:** As a platform engineer, I want the entire system defined as infrastructure-as-code and deployable with a single command, so that the MVP can be stood up and torn down reliably.

#### Acceptance Criteria

1. THE System SHALL be defined using AWS SAM (template.yaml)
2. THE System SHALL be deployable using the command `sam deploy`
3. THE System SHALL define all S3 buckets, Lambda functions, Step Functions state machine, DynamoDB tables, and API Gateway resources in the SAM template
4. THE System SHALL use Python as the Lambda runtime
5. WHERE environment-specific configuration is needed, THE System SHALL use SAM parameters rather than hardcoded values
