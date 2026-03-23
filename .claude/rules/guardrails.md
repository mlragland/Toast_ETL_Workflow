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
- If a task is going sideways after 2-3 attempts, STOP and re-plan instead of pushing through

## Verification
- After making changes, run `python -c "from main import app; print('OK')"` before declaring done
- Verify import paths exist before using them — modules were recently reorganized
- Check that constants referenced in routes actually exist in config.py

## BigQuery Safety
- NEVER add `DROP TABLE` or `DELETE` without a `WHERE` clause
- Several routes build SQL with f-strings using user-supplied dates — always validate `YYYY-MM-DD` format before interpolating
- Prefer parameterized queries for any new endpoints
- Bank transaction deletion uses row-level DML, not table drops

## Security
- NEVER commit secrets (API keys, webhook URLs, Sheet IDs, service account emails) to source files
- deploy.sh reads `SLACK_WEBHOOK_URL` from env — never hardcode it
- SFTP key and SendGrid API key live in Secret Manager — reference by name only
- Cloud Run allows `allUsers` for dashboards — auth-sensitive routes (`/run`, `/backfill`, `/weekly-report`) require OIDC tokens. Do not add data-mutating routes without auth.

## When Unsure
- Ask the user rather than guessing
- Say "I don't know" rather than making up an answer
- If a file path or function name isn't certain, search for it first
