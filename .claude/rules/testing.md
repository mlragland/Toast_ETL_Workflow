# Testing Standards — Toast ETL Pipeline

## Current Score: 30/40 (Grade B+) — up from 3/40
| Category | Score | Notes |
|----------|-------|-------|
| Smoke Tests | 3 | 5 smoke tests: health, 14 dashboards, 404 |
| Integration Tests | 3 | 13 route tests with mocked BigQuery (bank + ETL) |
| Unit Tests | 3 | 17 tests: BofACSVParser, DataTransformer, models, config |
| Data Validation | 3 | 14 tests: schema validation, transform quality, bank CSV quality |
| Test Infrastructure | 4 | pytest + conftest.py + fixtures + GitHub Actions CI on push |
| Error Recovery | 3 | 9 tests: BQ down, SFTP failures, malformed CSV, missing params |
| Regression Protection | 3 | 65 tests catch regressions across all layers |
| Observability | 2 | Structured JSON logging, request IDs, Cloud Run trace correlation |

**Grade B achieved. Safe for regular deploys.**

## Test Infrastructure
- Framework: pytest with conftest.py fixtures
- Run all tests: `pytest tests/ -v`
- Run smoke only: `pytest tests/test_smoke.py -v`
- Minimum gate before deploy: smoke tests must pass

## What to Test (Priority Order)
1. **Smoke tests** — `/` returns healthy, `/bank-review` returns HTML, `/api/bank-transactions` returns JSON
2. **Business logic** — BofACSVParser categorization, DataTransformer cleaning, business day SQL
3. **API contracts** — analytics endpoints return expected JSON shape for known date ranges
4. **Error paths** — invalid dates, missing params, malformed CSV uploads
5. **Data validation** — BigQuery row counts after ETL, null checks on key columns

## Test Conventions
- Test files mirror source: `services.py` → `tests/test_services.py`
- Each test tests ONE behavior: `test_{what}_{condition}_{expected}`
- Use fixtures for Flask test client and mock BigQuery — never hardcode
- Tests must be independent and fast (full suite < 5 minutes)

## When Writing New Code
- New business logic → unit test with known input/output
- New API endpoint → integration test with test client
- New dashboard → test that it returns HTML containing expected elements
- Bug fix → regression test that reproduces the bug first

## Mocking Strategy
- Mock BigQuery: `unittest.mock.patch` on `bigquery.Client`, return known rows
- Mock SFTP: patch `paramiko.Transport` — never connect to real SFTP in tests
- Mock SendGrid: patch `SendGridAPIClient.send`
- Mock Google Sheets API: patch `googleapiclient.discovery.build`
- NEVER mock Flask test client — use `app.test_client()` directly

## Data Validation (ETL Pipeline)
- After BigQuery loads: verify row count > 0, key columns not null
- Compare SFTP file row count to BigQuery loaded count
- Validate processing_date matches expected date
- Alert on anomalies: sudden row count drops, unexpected nulls
