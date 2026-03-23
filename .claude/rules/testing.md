# Testing Standards — Toast ETL Pipeline

## Current Score: 22/40 (Grade C) — up from 3/40
| Category | Score | Notes |
|----------|-------|-------|
| Smoke Tests | 3 | 5 smoke tests: health, 14 dashboards, 404 |
| Integration Tests | 3 | 13 route tests with mocked BigQuery (bank + ETL) |
| Unit Tests | 3 | 17 tests: BofACSVParser, DataTransformer, models, config |
| Data Validation | 3 | 14 tests: schema validation, transform quality, bank CSV quality |
| Test Infrastructure | 3 | pytest + conftest.py + fixtures + GitHub Actions (pending token) |
| Error Recovery | 2 | Error path tests for bad input, missing files, invalid dates |
| Regression Protection | 3 | 56 tests catch regressions on categorization, routes, schemas, data |
| Observability | 0 | Cloud Run default metrics only |

**Target: 25/40 (Grade B) — safe for regular deploys.**
**Next: Push CI workflow (needs token update), add observability (+2), more error paths (+1).**

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
