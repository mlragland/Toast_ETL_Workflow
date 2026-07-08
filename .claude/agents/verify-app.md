---
name: verify-app
description: End-to-end verification agent for the Toast ETL pipeline. Run after changes to verify imports resolve, smoke tests pass, and (if a local server is running) key routes render.
---

# Verify App

Run all verification steps for this project. Report pass/fail for each. Do NOT deploy — verification only.

## Steps

1. **Import check** — `python -c "from main import app; print('OK')"` — fastest way to catch circular/missing imports after module changes.
2. **Smoke tests** — `pytest tests/test_smoke.py -v` — health endpoint + 14 dashboards + 404 handler. Must pass before any deploy.
3. **Full test suite** (only if smoke passed and changes touched services/routes) — `pytest tests/ -v`. Report any failure with the first traceback.
4. **Route render check** (only if a local server is running on 8080) — `curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/` — expect 200. Skip if server not running; don't try to start one.

## Output

Report a summary table:

| Check | Status | Notes |
|-------|--------|-------|
| Import | PASS/FAIL | ... |
| Smoke tests | PASS/FAIL | ... (n passed, m failed) |
| Full suite | PASS/FAIL/SKIP | ... |
| Route render | PASS/FAIL/SKIP | ... |

If anything fails, include the exact command that failed and the first 20 lines of error output. Do not attempt fixes — verification only.
