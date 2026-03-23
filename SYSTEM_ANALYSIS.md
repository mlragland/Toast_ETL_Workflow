# Toast ETL Pipeline — System Analysis & Fix Plan
**Date:** 2026-03-23 | **Analyst:** Claude Opus 4.6 | **Owner:** Maurice Ragland

---

## 1. System Overview

Flask app on Cloud Run serving two functions for LOV3 Houston restaurant:
1. **Daily ETL** — Toast POS → SFTP → BigQuery (7 CSV types, 6 AM CST)
2. **Financial Intelligence** — 14 HTML dashboards + 18 REST APIs for bank categorization, P&L, labor, loyalty, budgets, KPIs

### Connections Map
```
Toast POS ──SFTP──→ Cloud Run ──→ BigQuery (toast_raw)
                        │
Bank of America CSV ────┘         ├─ OrderDetails_raw
(manual upload)                   ├─ CheckDetails_raw
                                  ├─ PaymentDetails_raw
Google Sheets ──────────┐         ├─ ItemSelectionDetails_raw
(check register)        │         ├─ AllItemsReport_raw
                        ▼         ├─ CashEntries_raw
                   Cloud Run      ├─ KitchenTimings_raw
                        │         ├─ BankTransactions_raw
                        ├──→ SendGrid (weekly report email)
                        ├──→ Slack (pipeline alerts)
                        └──→ Secret Manager (SFTP key, SendGrid key)
```

---

## 2. Stakeholder Analysis

### Maurice Ragland — Owner / Operator
**Needs:** Real-time financial visibility, profit margin tracking, event ROI, labor cost control
**Current State:** 14 dashboards provide comprehensive financial intelligence. Budget tracker targets 15% margin. Event ROI tracks 6 recurring weekly events.
**Gaps:** No mobile notifications when margins slip. No automated alerts when COGS exceeds threshold. Dashboard load times depend on BigQuery cold queries (no caching).

### Head Accountant (Sarah persona)
**Needs:** Daily reconciliation, audit trail, accurate categorization, tax reporting
**Current State:** Bank review dashboard with auto-categorization (68% auto, 629 rules). Check register integration. P&L with category hierarchy.
**Gaps:** 5 uncategorized transactions pending. No export to QuickBooks/Xero. No audit log of who categorized what. Category rules not versioned.

### Assistant Manager (Jessica persona)
**Needs:** Quick transaction validation on mobile, shift-end reporting
**Current State:** All dashboards are responsive HTML. Bank review allows bulk categorization.
**Gaps:** No user authentication on dashboards (anyone with URL can view). No role-based access (manager vs accountant views). No mobile push notifications.

### Developer (Maurice / Claude Code)
**Needs:** Safe deployments, observable systems, testable code, fast iteration
**Current State:** 11-module architecture, 65 tests (Grade B), CI on push, structured logging, rollback capability.
**Gaps:** 15 SQL injection vulnerabilities. No staging environment. No automated integration tests against live BigQuery. No load testing.

---

## 3. SWOT Analysis

### Strengths
- **Modular architecture** — 14K monolith → 11 clean modules with no circular deps
- **Comprehensive dashboards** — 14 self-contained HTML pages covering all financial KPIs
- **Idempotent ETL** — MERGE-based loading, safe to re-run any date
- **Strong categorization engine** — 629 auto-rules, Toast ACH detection, check register integration, wire vendor parsing
- **Business rule codification** — 4 AM business day cutoff, gratuity split, true labor calculation
- **Test coverage** — 65 tests (Grade B), CI pipeline, structured logging
- **Zero-downtime deploys** — Cloud Run revisions with instant rollback

### Weaknesses
- **15 SQL injection vulnerabilities** — f-string date interpolation in routes_analytics.py and services.py
- **No application-level auth** — Cloud Run IAM only; anyone with the URL can access dashboards AND data-mutating endpoints
- **No caching** — Every analytics request runs fresh BigQuery queries (api_kpi_benchmarks = 9 queries, ~45 MB per call)
- **No staging environment** — Changes go directly to production
- **No audit trail** — Bank transaction categorizations not logged with user/timestamp
- **No accounting software integration** — Manual re-entry into QuickBooks/Xero
- **BigQuery client created per-request** — No connection pooling or client reuse

### Opportunities
- **Parameterize SQL** — Convert 15 vulnerable queries to use bigquery.ScalarQueryParameter (effort: 1-2 hours)
- **Add Redis caching** — Cache KPI/analytics results for 15-minute TTL (reduces BQ cost ~80%)
- **QuickBooks integration** — Export categorized transactions to GL via API
- **Multi-location support** — Architecture already handles single location; extend config for multiple
- **Automated alerts** — Slack/email when labor% > 35%, COGS > 30%, or cash gap detected
- **User authentication** — Add Firebase Auth or simple JWT to gate dashboard access by role

### Threats
- **SQL injection** — CRITICAL: User-supplied dates in analytics endpoints can be exploited to exfiltrate financial data
- **Data exposure** — All financial dashboards publicly accessible via Cloud Run URL
- **Vendor lock-in** — Deep BigQuery dependency (all queries use BQ SQL dialect)
- **Single point of failure** — One Cloud Run service serves ETL + dashboards + APIs; outage affects all
- **SFTP deprecation risk** — Toast may change export mechanisms; no API-based alternative configured
- **Regulatory risk** — Financial data without access controls or audit trail may not meet compliance requirements

---

## 4. Detailed Gap Analysis

### CRITICAL (P0 — Fix within 1 week)

| # | Gap | Current State | Desired State | Impact | Effort |
|---|-----|--------------|---------------|--------|--------|
| 1 | **SQL injection in 15 queries** | f-string date interpolation in routes_analytics.py (labor, kpi-benchmarks) and services.py (delete_by_partition) | All queries use bigquery.ScalarQueryParameter | Financial data exfiltration, data deletion | S (2hr) |
| 2 | **No auth on data-mutating routes** | /run, /backfill, /weekly-report accessible without OIDC token when allUsers IAM is set | Flask middleware checks Authorization header on POST routes | Unauthorized pipeline runs, data corruption | S (1hr) |

### HIGH (P1 — Fix within 2-4 weeks)

| # | Gap | Current State | Desired State | Impact | Effort |
|---|-----|--------------|---------------|--------|--------|
| 3 | **No dashboard authentication** | Anyone with URL views all financial data | JWT or API key gating, role-based views | Financial data exposure | M (4hr) |
| 4 | **No caching layer** | 27 BQ queries per full analytics suite, 9 per KPI call | Redis or in-memory cache with 15-min TTL | $$ BQ costs, slow dashboards | M (4hr) |
| 5 | **No audit trail** | Bank categorizations not logged | Log user, timestamp, old/new category per change | Compliance risk, no accountability | S (2hr) |
| 6 | **No date input validation** | Analytics endpoints accept any string as date | Regex validate YYYY-MM-DD before query execution | Reduces SQL injection surface even before parameterization | S (1hr) |

### MEDIUM (P2 — Fix within 1-3 months)

| # | Gap | Current State | Desired State | Impact | Effort |
|---|-----|--------------|---------------|--------|--------|
| 7 | **No staging environment** | All deploys go to production | Cloud Run staging service with test dataset | Reduces change failure rate | M (4hr) |
| 8 | **No accounting export** | Manual re-entry to QuickBooks | CSV/API export in GL format | Hours of manual accountant work | L (8hr) |
| 9 | **No automated financial alerts** | Must check dashboards manually | Slack alerts when labor% > 35%, COGS > 30%, cash gap > $500 | Delayed response to financial issues | M (4hr) |
| 10 | **BigQuery client per-request** | New client on every API call | Shared client or connection pooling | Memory usage, latency | S (1hr) |
| 11 | **No load testing** | Unknown performance under concurrent users | Artillery or Locust tests for top 5 endpoints | Performance blind spot | M (4hr) |

### LOW (P3 — Fix within 3-6 months)

| # | Gap | Current State | Desired State | Impact | Effort |
|---|-----|--------------|---------------|--------|--------|
| 12 | **Nav bar duplication** | Same HTML in all 14 dashboards | Shared _nav_bar_html() helper | Maintenance overhead | S (1hr) |
| 13 | **No multi-location support** | Hardcoded to LOV3 Houston | Config-driven location support | Limits expansion | L (16hr) |
| 14 | **No mobile push notifications** | Dashboard-only visibility | Firebase or Twilio push alerts | Delayed owner awareness | L (8hr) |
| 15 | **Weekly report is 1,929 lines** | Inline HTML email templates | Template engine (Jinja2) | Hard to maintain | M (4hr) |

---

## 5. Prioritized Fix Plan

### Week 1: Security Hardening (P0)
```
Fix #1: Parameterize all 15 SQL injection queries
  → routes_analytics.py: api_labor_analysis (5 queries)
  → routes_analytics.py: api_kpi_benchmarks (9 queries)
  → services.py: BigQueryLoader.delete_by_partition (1 query)
  → Convert f-string dates to bigquery.ScalarQueryParameter

Fix #2: Add auth middleware for POST routes
  → Check Authorization header or X-Scheduler-Source on /run, /backfill, /weekly-report
  → Return 401 if missing (Cloud Scheduler includes OIDC token)

Fix #6: Add date input validation
  → Validate YYYY-MM-DD regex on all analytics POST body dates
  → Return 400 with clear error message on invalid format
```

### Weeks 2-4: Data Protection (P1)
```
Fix #3: Add dashboard authentication
  → Simple API key or JWT check on all GET dashboard routes
  → Environment variable for dashboard access key

Fix #5: Add audit trail for categorizations
  → Log (timestamp, old_category, new_category, source) on every categorize call
  → Store in BankTransactions_audit BigQuery table

Fix #4: Add caching for analytics
  → In-memory dict cache with 15-min TTL (no Redis needed for single instance)
  → Cache key = endpoint + date range hash
  → Cache api_kpi_benchmarks first (9 queries, highest cost)
```

### Months 1-3: Operational Excellence (P2)
```
Fix #7: Create staging environment
  → Second Cloud Run service: toast-etl-staging
  → Separate BQ dataset: toast_staging
  → deploy.sh --staging flag

Fix #9: Automated financial alerts
  → Post-ETL: check if labor% > 35% or COGS > 30%
  → Slack alert with current values and trend

Fix #10: Shared BigQuery client
  → Module-level client in routes_analytics.py
  → Lazy initialization on first request

Fix #8: Accounting export
  → GET /api/export/gl?start_date=...&end_date=...
  → CSV in QuickBooks GL import format
```

### Months 3-6: Polish (P3)
```
Fix #12: Deduplicate nav bar
Fix #15: Jinja2 templates for weekly report
Fix #13: Multi-location config
Fix #14: Mobile push notifications
```

---

## 6. Scoring Summary

### Well-Architected Pillar Scores (1-5 maturity)

| Pillar | Score | Notes |
|--------|-------|-------|
| Operational Excellence | 3 | CI/CD, structured logging, no staging |
| Security | 1 | SQL injection, no app-level auth, no audit trail |
| Reliability | 4 | Idempotent ETL, rollback capability, error recovery tests |
| Performance | 2 | No caching, 27 BQ queries per suite, cold starts |
| Cost Optimization | 3 | Min 0 instances, but no query caching increases BQ spend |

### DORA Metrics (estimated)

| Metric | Current | Target |
|--------|---------|--------|
| Deployment Frequency | Weekly | Daily |
| Lead Time for Changes | ~1 hour | < 1 hour (already good) |
| Change Failure Rate | ~10% (no staging) | < 5% |
| Failed Deployment Recovery | < 5 min (rollback) | < 5 min (already good) |

### Testing Rubric

| Category | Score |
|----------|-------|
| Current | 25/40 (Grade B) |
| After P0 fixes | 27/40 |
| After P1 fixes | 30/40 (Grade B+) |
| After P2 fixes | 35/40 (Grade A) |

---

## 7. Reusable Analysis Template

This analysis follows a repeatable framework for any LOV3 project:

1. **System Overview** — What it does, who it serves, what it connects to
2. **Stakeholder Analysis** — Needs, current state, gaps per persona
3. **SWOT** — Strengths, Weaknesses, Opportunities, Threats
4. **Gap Analysis** — Prioritized by P0-P3 with effort estimates
5. **Fix Plan** — Time-boxed phases with specific deliverables
6. **Scoring** — Well-Architected pillars + DORA + Testing rubric

Template saved at: `~/Dropbox/Developer/shared/system-analysis-template.md`
