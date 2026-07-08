# Guardrails — Preventing Hallucination and Scope Creep

## Before Writing Code
- ALWAYS read the file before editing it — never assume file contents
- ALWAYS check existing patterns in the codebase before creating new ones
- NEVER create new files unless absolutely necessary — prefer editing existing files
- When uncertain about how something works, read the code first — don't guess

## Scope Control
- Only make changes that are directly requested or clearly necessary
- Don't add features, refactor code, or make "improvements" beyond what was asked
- Don't add docstrings, comments, or type annotations to code you didn't change
- If a task is going sideways after 2-3 attempts, STOP and switch to plan mode (shift+tab)

## Plan Mode Discipline
- Start every complex task in plan mode — refine the plan before writing code
- The moment something goes sideways, return to plan mode and re-plan
- Don't retry the same failing approach — diagnose why it failed first

## External Services & Data Safety
- NEVER send real Slack/email/SMS without explicit user confirmation — test channel first (Resend to a test address, Slack to a scratch channel, never `#lov3-leader-report` in dev)
- NEVER guess API field names (Toast, Resend, Slack) — read the handler + response schema and show the exact shape first
- NEVER type secrets from memory — read the live value from Secret Manager, verify it works, then use it
- User-facing timestamps in America/Chicago, never UTC (LOV3 operates in Central time; business day cutoff is 4 AM CT)
- GET endpoints must never mutate state — Slack/iMessage link-preview crawlers fire GETs. Confirmation page on GET, mutation on POST.

## Verification
- After making changes, run `python -c "from main import app; print('OK')"` before declaring done
- Verify import paths exist before using them — modules were recently reorganized
- Check that constants referenced in routes actually exist in config.py

## BigQuery Safety
- NEVER add `DROP TABLE` or `DELETE` without a `WHERE` clause
- ALL date parameters in SQL queries MUST use `bigquery.ScalarQueryParameter` — never f-string interpolation
- New endpoints: use `_validate_date_range()` from routes_analytics.py for date validation
- Bank transaction deletion uses row-level DML, not table drops
- `require_auth` decorator required on all POST routes in routes_etl.py

## Security
- NEVER commit secrets (API keys, webhook URLs, Sheet IDs, service account emails) to source files
- deploy.sh reads `SLACK_WEBHOOK_URL` from env — never hardcode it
- SFTP key and SendGrid API key live in Secret Manager — reference by name only
- Cloud Run allows `allUsers` for dashboards — auth-sensitive routes (`/run`, `/backfill`, `/weekly-report`) require OIDC tokens. Do not add data-mutating routes without auth.

## When Unsure
- Ask the user rather than guessing
- Say "I don't know" rather than making up an answer
- If a file path or function name isn't certain, search for it first
