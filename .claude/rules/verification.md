# Verification — The #1 Priority

"Give Claude a way to verify its work. If Claude has that feedback loop, it will 2-3x the quality of the final result." — Boris Cherny

## After Every Change
- Run `python -c "from main import app; print('OK')"` — catches import errors and circular imports (modules were reorganized; this is the fastest signal)
- Run `pytest tests/test_smoke.py -v` — 5 smoke tests, <30s, gates every deploy
- Run `pytest tests/ -v` when touching business logic, services, or routes
- For dashboard/HTML changes: `curl -s http://localhost:8080/<route> | head -1` to confirm the route renders

## Before Marking Work Complete
- All verification commands pass
- No new warnings introduced
- New endpoints have at least one smoke assertion in `tests/test_smoke.py`
- Any new POST route on `routes_etl.py` has `require_auth` applied

## When Verification Fails
- Same error twice → re-read the relevant code, don't retry blindly
- Approach isn't working after 2-3 attempts → switch to plan mode
- Unfamiliar API or library → read docs/source before guessing
- Say "Knowing everything you know now, scrap this and implement the elegant solution"

## Post-Deploy Verification (Cloud Run)
- Curl the health endpoint and confirm `status: healthy`:
  `curl -s https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app/ | python -c "import json,sys; print(json.load(sys.stdin)['status'])"`
- Cache-bust re-checks (`?_=<unix-ts>`) — CDN/tool caches mask deploys
- If a fetch tool reports the site "down", verify with a real browser before assuming a deploy issue (Cloud Run cold-start timeouts, WAFs 403 headless clients)
- Re-query BigQuery before reporting metrics — cached numbers are dead after any sync/backfill
- Confirm the new revision is serving 100% of traffic:
  `gcloud run revisions list --service=toast-etl-pipeline --region=us-central1 --limit=3`

## Domain-Specific Verification
- After ETL runs: `SELECT COUNT(*), MAX(processing_date) FROM toast_raw.<Table>` — row count > 0 AND max date matches expected
- After bank CSV upload: compare uploaded row count to `BankTransactions_raw` insert count
- After BigQuery schema changes: run one query against the changed columns before shipping
- After weekly report changes: send to a test Slack channel before pointing at `#lov3-leader-report`
- After gratuity report changes: `curl -X POST /gratuity-report?dry_run=true` on the local dev server before scheduling
