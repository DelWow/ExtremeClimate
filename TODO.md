# Extreme Climate Pipeline — Build Plan

This checklist is the implementation sequence. Each numbered task is intended to be a small, independently reviewable commit. Only one task will be implemented and verified at a time; after its checkbox is marked complete, work pauses for review.

## Planning checkpoint

- [x] Create this build plan before writing application code.

## Build tasks

### 1. Repository scaffolding

- [x] Create the initial application, configuration, SQL, test, DAG, and report-output directories.
- [x] Add `.gitignore`, `requirements.txt`, and a deliberately minimal `README.md` placeholder.
- [x] Verify the expected skeleton exists and Python dependencies are internally consistent.

### 2. Docker Compose: PostgreSQL only

- [x] Add a Compose configuration containing only PostgreSQL and its persistent volume/health check.
- [x] Add a checked-in environment-variable example without secrets.
- [x] Start PostgreSQL and verify it becomes healthy and accepts a connection.

### 3. Database schema

- [x] Add `schema.sql` with `raw_weather`, `historical_baselines`, and `weather_daily_summary` tables, including appropriate keys, constraints, and indexes.
- [x] Arrange for the schema to initialize in PostgreSQL.
- [x] Verify all tables and constraints with manual SQL queries.

### 4. Region configuration

- [x] Add `regions.yaml` with the regions and weather-query metadata used by the pipeline.
- [x] Add a small loader/validator script with clear failures for malformed configuration.
- [x] Add focused tests for valid and invalid configuration, then run them.

### 5. Docker Compose: Kafka

- [x] Add Kafka and the single coordination mode selected for this project (KRaft or ZooKeeper) to Compose.
- [x] Add broker health/readiness configuration without changing the PostgreSQL behavior.
- [x] Start the services and verify that a client can connect to the broker and use the weather topic.

### 6. Weather producer: fetch and print

- [x] Add an API client that fetches current weather for configured regions and normalizes the response into the pipeline event shape.
- [x] Add a command-line producer entry point that prints normalized events; do not connect it to Kafka yet.
- [x] Add tests with mocked API responses, including an API failure case, then run them.

### 7. Weather producer: publish to Kafka

- [x] Extend the producer to serialize and publish normalized weather events to the configured Kafka topic.
- [x] Keep external endpoints and credentials environment-configurable.
- [x] Add tests with a mocked Kafka producer and verify one real broker round trip.

### 8. Kafka consumer: persist raw weather

- [x] Add a consumer that deserializes weather events and inserts them into `raw_weather` safely.
- [x] Define offset/commit and duplicate-event behavior explicitly.
- [x] Add focused tests and verify an event can travel from Kafka into PostgreSQL.

### 9. Historical baseline seed script

- [x] Add a repeatable seed script for `historical_baselines` using the agreed baseline data source/fixture.
- [x] Make repeated runs safe and document the seed input assumptions.
- [x] Verify expected row counts and a representative region/date query.

### 10. Data validation logic

- [x] Add reusable validation for required fields, timestamps, region identifiers, and plausible weather-value ranges.
- [x] Add unit tests for accepted records and each rejection category.
- [x] Run the focused unit test suite.

### 11. Daily transformation and aggregation

- [x] Add reusable logic that transforms valid `raw_weather` rows into `weather_daily_summary` records.
- [x] Define grouping, units, missing-data handling, and idempotent writes.
- [x] Add unit tests for aggregation and edge cases, then run them.

### 12. Anomaly detection

- [ ] Add reusable logic that compares daily summaries with historical baselines and records/labels anomalies using explicit thresholds.
- [ ] Add unit tests for normal, boundary, anomalous, and missing-baseline cases.
- [ ] Run the focused unit test suite.

### 13. Excel report: basic output

- [ ] Add a report generator that creates a single-sheet Excel report from daily summaries and anomaly results.
- [ ] Add unit tests for workbook creation, columns, values, and deterministic output structure.
- [ ] Run the focused tests and open/read the generated workbook programmatically.

### 14. Excel report: chart and formatting

- [ ] Add a trend chart and conditional formatting for anomalous values to the existing report.
- [ ] Add tests that inspect the workbook for the chart series and formatting rules.
- [ ] Generate and inspect a representative report.

### 15. Docker Compose: Airflow

- [ ] Add the minimum Airflow services, metadata initialization, volumes, and health checks needed for local orchestration.
- [ ] Add an empty/smoke-test DAG without wiring pipeline behavior yet.
- [ ] Start Airflow and verify the UI/scheduler are healthy and the smoke DAG loads without import errors.

### 16. Airflow pipeline DAG

- [ ] Replace the smoke DAG with an ordered workflow that calls the already-tested validation, transformation, anomaly, and report functions.
- [ ] Configure dependencies, retries, and runtime settings without duplicating business logic in the DAG.
- [ ] Add DAG-structure/import tests and run them.

### 17. End-to-end pipeline test

- [ ] Run the complete local stack and execute one controlled pipeline cycle from weather event through Excel report.
- [ ] Verify Kafka delivery, database records at each stage, anomaly output, Airflow task success, and report contents.
- [ ] Record the reproducible commands and expected verification results.

### 18. README documentation

- [ ] Replace the placeholder README with architecture, prerequisites, configuration, setup, operation, testing, troubleshooting, and teardown instructions.
- [ ] Include the verified end-to-end workflow and report location.
- [ ] Follow the README from a clean-start perspective and correct any gaps found.

### 19. Final polish

- [ ] Review application code for useful docstrings, complete type hints, consistent logging, and actionable error handling.
- [ ] Run formatting/static checks and the complete test suite.
- [ ] Perform a final Compose/DAG configuration validation and ensure no generated artifacts or secrets are tracked.

## Change-control rule

If implementation reveals additional necessary work, add a new unchecked task here before making that change. Split any task that cannot remain independently implementable and verifiable.
