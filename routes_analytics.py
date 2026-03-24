"""Analytics API routes: all POST /api/* endpoints plus profit-summary, comprehensive-analysis, etc."""

import io
import re
import csv
import json
import logging
import calendar
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, request, jsonify, Response
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from googleapiclient.errors import HttpError as GoogleHttpError

from config import (
    PROJECT_ID, DATASET_ID,
    BUSINESS_DAY_SQL, BUSINESS_DOW_SQL, BUSINESS_DAY_CUTOFF_HOUR,
    GRAT_RETAIN_PCT, GRAT_PASSTHROUGH_PCT,
    LOV3_EVENTS,
    EVENT_VENDOR_MAP, RECURRING_EVENTS,
    DIRECT_EVENT_CATEGORIES, SHARED_EVENT_CATEGORIES,
    OPERATIONAL_LABOR_CATEGORIES,
    LABOR_DOW_PCT, FIXED_LABOR_MONTHLY,
    BUDGET_TARGETS, BUDGET_SUBCATEGORIES, UNBUDGETED_SECTIONS,
    CHECK_REGISTER_SHEET_ID,
)
from services import BofACSVParser, BankCategoryManager, CheckRegisterSync
from weekly_report import WeeklyReportGenerator

logger = logging.getLogger(__name__)

bp = Blueprint("analytics", __name__)


# ─── In-memory cache for analytics queries ───────────────────────────────────
# Simple TTL cache to avoid redundant BigQuery calls for the same date range.
# Cloud Run instances are ephemeral — cache lives only for the instance lifetime.
# No shared state between instances. No external dependency (no Redis needed).

import hashlib as _hashlib
import time as _time

_CACHE: Dict[str, Any] = {}
_CACHE_TTL = 900  # 15 minutes


def _cache_key(endpoint: str, params: dict) -> str:
    """Generate a deterministic cache key from endpoint + params."""
    raw = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
    return _hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key: str) -> Optional[Any]:
    """Get cached value if not expired."""
    entry = _CACHE.get(key)
    if entry and _time.time() - entry["ts"] < _CACHE_TTL:
        return entry["data"]
    return None


def _cache_set(key: str, data: Any):
    """Store value in cache with current timestamp."""
    # Evict old entries if cache grows too large (>100 entries)
    if len(_CACHE) > 100:
        cutoff = _time.time() - _CACHE_TTL
        expired = [k for k, v in _CACHE.items() if v["ts"] < cutoff]
        for k in expired:
            del _CACHE[k]
    _CACHE[key] = {"data": data, "ts": _time.time()}


# ─── Validation helpers ──────────────────────────────────────────────────────

import re

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_date_range(data: dict) -> Optional[tuple]:
    """Validate start_date and end_date from request body.

    Returns (start_date, end_date) if valid, or a (jsonify, status_code) error tuple.
    """
    if not data:
        return jsonify({"error": "Request body required with start_date and end_date"}), 400

    start_date = data.get("start_date", "")
    end_date = data.get("end_date", "")

    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date are required (YYYY-MM-DD)"}), 400

    if not _DATE_RE.match(start_date):
        return jsonify({"error": f"Invalid start_date format: {start_date}. Use YYYY-MM-DD"}), 400

    if not _DATE_RE.match(end_date):
        return jsonify({"error": f"Invalid end_date format: {end_date}. Use YYYY-MM-DD"}), 400

    if start_date > end_date:
        return jsonify({"error": "start_date must be before or equal to end_date"}), 400

    return start_date, end_date


# ─── Helper functions ────────────────────────────────────────────────────────

def _compute_event_insights(
    all_weekly: List[dict],
    year_events: List[dict],
    avg_weekly: float,
) -> List[dict]:
    """Compute insights comparing event-week revenue to baseline."""
    insights: List[dict] = []

    if not all_weekly or avg_weekly <= 0:
        return insights

    baseline = avg_weekly

    # Helper: find weeks overlapping an event
    def event_week_revenues(event_name_fragment: str) -> List[float]:
        revs = []
        for ev in LOV3_EVENTS:
            if event_name_fragment.lower() not in ev["name"].lower():
                continue
            ev_start = datetime.strptime(ev["start_date"], "%Y-%m-%d")
            ev_end = datetime.strptime(ev["end_date"], "%Y-%m-%d")
            for w in all_weekly:
                if not w["week_start"]:
                    continue
                ws = datetime.strptime(w["week_start"], "%Y-%m-%d")
                we = ws + timedelta(days=6)
                if ws <= ev_end and we >= ev_start and w["revenue"] > 0:
                    revs.append(w["revenue"])
        return revs

    # Rodeo insight
    rodeo_revs = event_week_revenues("rodeo")
    if rodeo_revs:
        avg_rodeo = sum(rodeo_revs) / len(rodeo_revs)
        lift = ((avg_rodeo - baseline) / baseline * 100) if baseline > 0 else 0
        insights.append({
            "title": "Houston Rodeo Impact",
            "text": f"Rodeo weeks averaged {_fmt_k(avg_rodeo)} vs {_fmt_k(baseline)} baseline = +{lift:.0f}% lift. Plan for peak staffing and inventory during the 3-week run.",
        })

    # Afrotech / Halloween
    afro_revs = event_week_revenues("afrotech")
    if afro_revs:
        peak_afro = max(afro_revs)
        insights.append({
            "title": "Afrotech + Halloween Week",
            "text": f"Afrotech week drove {_fmt_k(peak_afro)} peak revenue. Conference attendees + Halloween create a powerful combo \u2014 consider themed events and extended hours.",
        })

    # Summer dip
    summer_weeks = [w for w in all_weekly if w["week_start"] and w["week_start"][5:7] in ("06", "07")]
    if summer_weeks:
        avg_summer = sum(w["revenue"] for w in summer_weeks) / len(summer_weeks)
        dip_pct = ((baseline - avg_summer) / baseline * 100) if baseline > 0 else 0
        if dip_pct > 5:
            insights.append({
                "title": "Summer Revenue Dip",
                "text": f"Jun-Jul weekly average of {_fmt_k(avg_summer)} is {dip_pct:.0f}% below the annual mean. Consider themed events, happy hour specials, or partnerships to drive traffic.",
            })

    # Promotional windows
    promo_windows = []
    for ev in year_events:
        if ev["category"] in ("holiday", "cultural", "conference"):
            promo_windows.append(ev["name"])
    if promo_windows:
        insights.append({
            "title": "Recommended Promo Windows",
            "text": "Key dates to plan marketing around: " + ", ".join(promo_windows[:8]) + ". Build social media and email campaigns 2-3 weeks before each event.",
        })

    return insights


def _fmt_k(val: float) -> str:
    """Format dollar amount as $XXK."""
    if val >= 1000:
        return f"${val / 1000:.1f}K"
    return f"${val:.0f}"


# ─── Events Calendar API ────────────────────────────────────────────────────

@bp.route("/api/events-calendar", methods=["POST"])
def api_events_calendar():
    """
    Events calendar data: weekly revenue overlay + event metadata.

    Request body: {"year": 2026}
    """
    try:
        body = request.get_json(silent=True) or {}
        year = int(body.get("year", datetime.now().year))

        bq = bigquery.Client(project=PROJECT_ID)

        # Business day SQL for PaymentDetails
        bd_sql = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")

        query = f"""
        SELECT
          DATE_TRUNC({bd_sql}, WEEK(MONDAY)) AS week_start,
          COUNT(DISTINCT order_id) AS orders,
          SUM(CAST(amount AS FLOAT64)) AS revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE {bd_sql} BETWEEN @start_date AND @end_date
          AND status IN ('CAPTURED','AUTHORIZED','CAPTURE_IN_PROGRESS')
          AND paid_date IS NOT NULL AND paid_date != ''
        GROUP BY week_start
        ORDER BY week_start
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", f"{year - 1}-01-01"),
                bigquery.ScalarQueryParameter("end_date", "DATE", f"{year}-12-31"),
            ]
        )

        rows = list(bq.query(query, job_config=job_config).result())

        # Split into prior-year and current-year
        prior_year_weekly = []
        current_year_weekly = []
        for row in rows:
            ws = row.week_start.isoformat() if row.week_start else None
            entry = {
                "week_start": ws,
                "revenue": round(float(row.revenue or 0), 2),
                "orders": int(row.orders or 0),
            }
            if ws and ws.startswith(str(year - 1)):
                prior_year_weekly.append(entry)
            else:
                current_year_weekly.append(entry)

        all_weekly = prior_year_weekly + current_year_weekly

        # Filter events for requested year
        year_events = [
            e for e in LOV3_EVENTS
            if e["start_date"].startswith(str(year))
        ]

        # ── KPIs ──
        today_str = datetime.now().strftime("%Y-%m-%d")
        upcoming = [e for e in year_events if e["start_date"] >= today_str]
        upcoming.sort(key=lambda e: e["start_date"])
        next_event = upcoming[0] if upcoming else None

        days_until = None
        if next_event:
            ne_date = datetime.strptime(next_event["start_date"], "%Y-%m-%d")
            days_until = max(0, (ne_date - datetime.now()).days)

        # Best month from all weekly data
        monthly_rev: Dict[str, float] = {}
        for w in all_weekly:
            if w["week_start"]:
                month_key = w["week_start"][:7]  # YYYY-MM
                monthly_rev[month_key] = monthly_rev.get(month_key, 0) + w["revenue"]

        best_month = None
        best_month_revenue = 0
        for mk, rev in monthly_rev.items():
            if rev > best_month_revenue:
                best_month_revenue = rev
                best_month = mk

        best_month_label = None
        if best_month:
            try:
                bm_date = datetime.strptime(best_month, "%Y-%m")
                best_month_label = bm_date.strftime("%b %Y")
            except Exception:
                best_month_label = best_month

        # Avg / peak weekly revenue (current year)
        curr_revs = [w["revenue"] for w in current_year_weekly if w["revenue"] > 0]
        avg_weekly = round(sum(curr_revs) / len(curr_revs), 2) if curr_revs else 0
        peak_week = max(all_weekly, key=lambda w: w["revenue"]) if all_weekly else None

        kpis = {
            "next_event": next_event["name"] if next_event else None,
            "next_event_date": next_event["start_date"] if next_event else None,
            "days_until": days_until,
            "best_month": best_month_label,
            "best_month_revenue": round(best_month_revenue, 2),
            "avg_weekly_revenue": avg_weekly,
            "peak_week_revenue": round(peak_week["revenue"], 2) if peak_week else 0,
            "peak_week_date": peak_week["week_start"] if peak_week else None,
        }

        # ── Top 20 weeks ──
        sorted_weeks = sorted(all_weekly, key=lambda w: w["revenue"], reverse=True)[:20]
        top_weeks = []
        for rank, w in enumerate(sorted_weeks, 1):
            ws_date = datetime.strptime(w["week_start"], "%Y-%m-%d") if w["week_start"] else None
            we_date = ws_date + timedelta(days=6) if ws_date else None
            overlapping = []
            if ws_date and we_date:
                for ev in LOV3_EVENTS:
                    ev_start = datetime.strptime(ev["start_date"], "%Y-%m-%d")
                    ev_end = datetime.strptime(ev["end_date"], "%Y-%m-%d")
                    if ev_start <= we_date and ev_end >= ws_date:
                        overlapping.append({"name": ev["name"], "category": ev["category"]})
            top_weeks.append({
                "rank": rank,
                "week_start": w["week_start"],
                "revenue": w["revenue"],
                "orders": w["orders"],
                "events": overlapping,
            })

        # ── Upcoming events with historical revenue context ──
        # Upcoming events are future dates, so we find the same-named event
        # from the prior year and look up revenue weeks that overlapped it.
        prior_year_events = [
            e for e in LOV3_EVENTS
            if e["start_date"].startswith(str(year - 1))
        ]
        upcoming_events = []
        for ev in upcoming[:10]:
            hist_rev = None
            # Find same-named event in prior year
            prior_match = next(
                (pe for pe in prior_year_events if pe["name"] == ev["name"]),
                None,
            )
            if prior_match:
                pm_start = datetime.strptime(prior_match["start_date"], "%Y-%m-%d")
                pm_end = datetime.strptime(prior_match["end_date"], "%Y-%m-%d")
                # Find best revenue week overlapping the prior-year event
                for w in sorted_weeks:
                    if not w["week_start"]:
                        continue
                    ws = datetime.strptime(w["week_start"], "%Y-%m-%d")
                    we = ws + timedelta(days=6)
                    if ws <= pm_end and we >= pm_start:
                        hist_rev = w["revenue"]
                        break
            upcoming_events.append({
                "name": ev["name"],
                "start_date": ev["start_date"],
                "end_date": ev["end_date"],
                "category": ev["category"],
                "historical_revenue": hist_rev,
            })

        # ── Insights ──
        insights = _compute_event_insights(all_weekly, year_events, avg_weekly)

        return jsonify({
            "year": year,
            "events": year_events,
            "weekly_revenue": current_year_weekly,
            "prior_year_weekly": prior_year_weekly,
            "top_weeks": top_weeks,
            "upcoming_events": upcoming_events,
            "kpis": kpis,
            "insights": insights,
        })

    except Exception as e:
        logger.error(f"Events calendar error: {e}")
        return jsonify({"error": str(e)}), 500


# ─── Server Performance API ────────────────────────────────────────────────
@bp.route("/api/server-performance", methods=["POST"])
def api_server_performance():
    """
    Server performance from OrderDetails + CheckDetails + PaymentDetails.

    Request body:
    {
        "start_date": "2025-12-01",
        "end_date": "2026-02-27"
    }
    """
    try:
        body = request.get_json(silent=True) or {}
        start_date = body.get("start_date", "")
        end_date = body.get("end_date", "")
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400

        bq = bigquery.Client(project=PROJECT_ID)

        # 1) Server summary from OrderDetails_raw
        order_sql = f"""
        SELECT
            server,
            COUNT(DISTINCT order_id) AS orders,
            SUM(guest_count) AS guests,
            SUM(amount) AS revenue,
            SUM(tip) AS tips,
            SUM(gratuity) AS gratuity,
            SUM(discount_amount) AS discounts,
            COUNTIF(discount_amount > 0) AS discounted_orders,
            SAFE_DIVIDE(SUM(amount), COUNT(DISTINCT order_id)) AS avg_check,
            SAFE_DIVIDE(SUM(amount), NULLIF(SUM(guest_count), 0)) AS rev_per_guest,
            SAFE_DIVIDE(SUM(tip), NULLIF(SUM(amount), 0)) * 100 AS tip_pct,
            SAFE_DIVIDE(SUM(discount_amount), NULLIF(SUM(amount + discount_amount), 0)) * 100 AS discount_pct
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND (voided IS NULL OR LOWER(voided) != 'true')
            AND server IS NOT NULL AND TRIM(server) != ''
        GROUP BY server
        ORDER BY revenue DESC
        """

        # 2) DOW + hourly per server from PaymentDetails_raw
        bd_sql = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
        dow_sql = BUSINESS_DOW_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
        detail_sql = f"""
        SELECT
            server,
            {dow_sql} AS dow,
            EXTRACT(HOUR FROM CAST(paid_date AS DATETIME)) AS hour,
            COUNT(*) AS txns,
            SUM(amount) AS revenue,
            SAFE_DIVIDE(SUM(amount), COUNT(*)) AS avg_check
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND status IN ('CAPTURED', 'AUTHORIZED', 'CAPTURE_IN_PROGRESS')
            AND server IS NOT NULL AND TRIM(server) != ''
        GROUP BY server, dow, hour
        ORDER BY server, dow, hour
        """

        params = [
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=params)

        order_rows = list(bq.query(order_sql, job_config=job_config).result())
        detail_rows = list(bq.query(detail_sql, job_config=job_config).result())

        # Build DOW + hourly maps per server
        dow_map: dict = {}  # server -> {dow: {revenue, orders, avg_check}}
        hourly_map: dict = {}  # server -> {hour: {revenue, orders}}
        for r in detail_rows:
            srv = r.server
            if srv not in dow_map:
                dow_map[srv] = {}
                hourly_map[srv] = {}
            dow = r.dow
            hr = r.hour
            if dow not in dow_map[srv]:
                dow_map[srv][dow] = {"dow": dow, "revenue": 0.0, "orders": 0, "avg_check": 0.0}
            dow_map[srv][dow]["revenue"] += float(r.revenue or 0)
            dow_map[srv][dow]["orders"] += int(r.txns or 0)
            if hr not in hourly_map[srv]:
                hourly_map[srv][hr] = {"hour": hr, "revenue": 0.0, "orders": 0}
            hourly_map[srv][hr]["revenue"] += float(r.revenue or 0)
            hourly_map[srv][hr]["orders"] += int(r.txns or 0)

        # Compute avg_check for DOW entries
        for srv in dow_map:
            for dow in dow_map[srv]:
                entry = dow_map[srv][dow]
                entry["avg_check"] = entry["revenue"] / entry["orders"] if entry["orders"] else 0

        servers = []
        for r in order_rows:
            srv = r.server
            servers.append({
                "server": srv,
                "orders": int(r.orders or 0),
                "guests": int(r.guests or 0),
                "revenue": round(float(r.revenue or 0), 2),
                "tips": round(float(r.tips or 0), 2),
                "gratuity": round(float(r.gratuity or 0), 2),
                "discounts": round(float(r.discounts or 0), 2),
                "discounted_orders": int(r.discounted_orders or 0),
                "avg_check": round(float(r.avg_check or 0), 2),
                "rev_per_guest": round(float(r.rev_per_guest or 0), 2),
                "tip_pct": round(float(r.tip_pct or 0), 1),
                "discount_pct": round(float(r.discount_pct or 0), 1),
                "dow": sorted(dow_map.get(srv, {}).values(), key=lambda x: [
                    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
                ].index(x["dow"]) if x["dow"] in [
                    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
                ] else 99),
                "hourly": sorted(hourly_map.get(srv, {}).values(), key=lambda x: x["hour"]),
            })

        # KPIs
        total_servers = len(servers)
        total_revenue = sum(s["revenue"] for s in servers)
        total_tips = sum(s["tips"] for s in servers)
        kpis = {
            "total_servers": total_servers,
            "avg_revenue_per_server": round(total_revenue / total_servers, 2) if total_servers else 0,
            "top_server_revenue": servers[0]["revenue"] if servers else 0,
            "avg_check_size": round(
                sum(s["avg_check"] * s["orders"] for s in servers) /
                max(sum(s["orders"] for s in servers), 1), 2
            ),
            "avg_tip_pct": round(
                total_tips / max(total_revenue, 1) * 100, 1
            ),
        }

        return jsonify({"kpis": kpis, "servers": servers})

    except Exception as e:
        logging.exception("server-performance API error")
        return jsonify({"error": str(e)}), 500


# ─── Kitchen Speed API ─────────────────────────────────────────────────────
@bp.route("/api/kitchen-speed", methods=["POST"])
def api_kitchen_speed():
    """
    Kitchen speed analysis from KitchenTimings_raw.

    Request body:
    {
        "start_date": "2025-12-01",
        "end_date": "2026-02-27"
    }
    """
    try:
        body = request.get_json(silent=True) or {}
        start_date = body.get("start_date", "")
        end_date = body.get("end_date", "")
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400

        bq = bigquery.Client(project=PROJECT_ID)

        params = [
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=params)

        # 1) Station summary
        station_sql = f"""
        SELECT
            station,
            COUNT(*) AS tickets,
            COUNTIF(fulfilled_date IS NOT NULL) AS fulfilled,
            AVG(CASE WHEN fulfilled_date IS NOT NULL THEN
                TIMESTAMP_DIFF(CAST(fulfilled_date AS DATETIME), CAST(fired_date AS DATETIME), SECOND) END) AS avg_sec,
            APPROX_QUANTILES(
                CASE WHEN fulfilled_date IS NOT NULL THEN
                TIMESTAMP_DIFF(CAST(fulfilled_date AS DATETIME), CAST(fired_date AS DATETIME), SECOND) END, 100
            )[OFFSET(50)] AS median_sec,
            MIN(CASE WHEN fulfilled_date IS NOT NULL THEN
                TIMESTAMP_DIFF(CAST(fulfilled_date AS DATETIME), CAST(fired_date AS DATETIME), SECOND) END) AS min_sec,
            MAX(CASE WHEN fulfilled_date IS NOT NULL THEN
                TIMESTAMP_DIFF(CAST(fulfilled_date AS DATETIME), CAST(fired_date AS DATETIME), SECOND) END) AS max_sec,
            SAFE_DIVIDE(COUNTIF(fulfilled_date IS NOT NULL), COUNT(*)) * 100 AS fulfillment_pct
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND fired_date IS NOT NULL
            AND station IS NOT NULL AND TRIM(station) != ''
        GROUP BY station
        ORDER BY avg_sec ASC
        """

        # 2) Hourly profile
        hourly_sql = f"""
        SELECT
            EXTRACT(HOUR FROM CAST(fired_date AS DATETIME)) AS hour,
            COUNT(*) AS tickets,
            AVG(CASE WHEN fulfilled_date IS NOT NULL THEN
                TIMESTAMP_DIFF(CAST(fulfilled_date AS DATETIME), CAST(fired_date AS DATETIME), SECOND) END) AS avg_sec
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND fired_date IS NOT NULL
        GROUP BY hour
        ORDER BY hour
        """

        # 3) Cook summary
        cook_sql = f"""
        SELECT
            fulfilled_by AS cook,
            COUNT(*) AS tickets,
            AVG(TIMESTAMP_DIFF(CAST(fulfilled_date AS DATETIME), CAST(fired_date AS DATETIME), SECOND)) AS avg_sec,
            MIN(TIMESTAMP_DIFF(CAST(fulfilled_date AS DATETIME), CAST(fired_date AS DATETIME), SECOND)) AS min_sec
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND fulfilled_date IS NOT NULL
            AND fired_date IS NOT NULL
            AND fulfilled_by IS NOT NULL AND TRIM(fulfilled_by) != ''
        GROUP BY cook
        ORDER BY avg_sec ASC
        """

        # 4) Weekly trend
        bd_sql = BUSINESS_DAY_SQL.format(dt_col="CAST(check_opened AS DATETIME)")
        weekly_sql = f"""
        SELECT
            FORMAT_DATE('%Y-%m-%d', DATE_TRUNC({bd_sql}, WEEK(MONDAY))) AS week,
            COUNT(*) AS tickets,
            COUNTIF(fulfilled_date IS NOT NULL) AS fulfilled,
            AVG(CASE WHEN fulfilled_date IS NOT NULL THEN
                TIMESTAMP_DIFF(CAST(fulfilled_date AS DATETIME), CAST(fired_date AS DATETIME), SECOND) END) AS avg_sec
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND fired_date IS NOT NULL
        GROUP BY week
        ORDER BY week
        """

        station_rows = list(bq.query(station_sql, job_config=job_config).result())
        hourly_rows = list(bq.query(hourly_sql, job_config=job_config).result())
        cook_rows = list(bq.query(cook_sql, job_config=job_config).result())
        weekly_rows = list(bq.query(weekly_sql, job_config=job_config).result())

        stations = [{
            "station": r.station,
            "tickets": int(r.tickets or 0),
            "fulfilled": int(r.fulfilled or 0),
            "avg_sec": round(float(r.avg_sec or 0), 1),
            "median_sec": round(float(r.median_sec or 0), 1) if r.median_sec else None,
            "min_sec": round(float(r.min_sec or 0), 1) if r.min_sec else None,
            "max_sec": round(float(r.max_sec or 0), 1) if r.max_sec else None,
            "fulfillment_pct": round(float(r.fulfillment_pct or 0), 1),
        } for r in station_rows]

        hourly = [{
            "hour": int(r.hour),
            "tickets": int(r.tickets or 0),
            "avg_sec": round(float(r.avg_sec or 0), 1) if r.avg_sec else None,
        } for r in hourly_rows]

        cooks = [{
            "cook": r.cook,
            "tickets": int(r.tickets or 0),
            "avg_sec": round(float(r.avg_sec or 0), 1),
            "min_sec": round(float(r.min_sec or 0), 1) if r.min_sec else None,
        } for r in cook_rows]

        weekly = [{
            "week": r.week,
            "tickets": int(r.tickets or 0),
            "fulfilled": int(r.fulfilled or 0),
            "avg_sec": round(float(r.avg_sec or 0), 1) if r.avg_sec else None,
        } for r in weekly_rows]

        # KPIs
        total_tickets = sum(s["tickets"] for s in stations)
        total_fulfilled = sum(s["fulfilled"] for s in stations)
        all_avg = [s["avg_sec"] for s in stations if s["avg_sec"] and s["avg_sec"] > 0]
        kpis = {
            "total_tickets": total_tickets,
            "avg_fulfillment_sec": round(sum(
                s["avg_sec"] * s["fulfilled"] for s in stations if s["avg_sec"]
            ) / max(total_fulfilled, 1), 1),
            "fastest_station": stations[0]["station"] if stations else None,
            "slowest_station": stations[-1]["station"] if stations else None,
            "fulfillment_rate": round(total_fulfilled / max(total_tickets, 1) * 100, 1),
        }

        return jsonify({
            "kpis": kpis,
            "stations": stations,
            "hourly": hourly,
            "cooks": cooks,
            "weekly": weekly,
        })

    except Exception as e:
        logging.exception("kitchen-speed API error")
        return jsonify({"error": str(e)}), 500


# ─── Labor Analysis API ────────────────────────────────────────────────────
@bp.route("/api/labor-analysis", methods=["POST"])
def api_labor_analysis():
    """
    Labor cost analysis: weekly/monthly labor vs revenue, vendor breakdown.

    Request body:
    {
        "start_date": "2025-09-01",
        "end_date": "2026-02-27"
    }
    """
    try:
        body = request.get_json(silent=True) or {}
        start_date = body.get("start_date", "")
        end_date = body.get("end_date", "")
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400

        bq = bigquery.Client(project=PROJECT_ID)

        # Q1: Weekly revenue from OrderDetails_raw
        rev_sql = f"""
        SELECT
            FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(DATE(processing_date), WEEK(MONDAY))) AS week_start,
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS tips,
            COALESCE(SUM(gratuity), 0) AS gratuity,
            COUNT(DISTINCT order_id) AS order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND (voided IS NULL OR voided = 'false')
        GROUP BY week_start ORDER BY week_start
        """

        # Q2: Weekly labor + all expenses from BankTransactions_raw
        exp_sql = f"""
        SELECT
            FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(CAST(transaction_date AS DATE), WEEK(MONDAY))) AS week_start,
            category,
            ROUND(SUM(abs_amount), 2) AS total,
            COUNT(*) AS txn_count
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
        GROUP BY week_start, category ORDER BY week_start
        """

        # Q3: Monthly revenue
        mrev_sql = f"""
        SELECT
            FORMAT_DATE('%Y-%m', processing_date) AS month,
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS tips,
            COALESCE(SUM(gratuity), 0) AS gratuity
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND (voided IS NULL OR voided = 'false')
        GROUP BY month ORDER BY month
        """

        # Q4: Monthly expenses
        mexp_sql = f"""
        SELECT
            FORMAT_DATE('%Y-%m', CAST(transaction_date AS DATE)) AS month,
            category,
            ROUND(SUM(abs_amount), 2) AS total
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
        GROUP BY month, category ORDER BY month
        """

        # Q5: Vendor breakdown (labor only)
        vendor_sql = f"""
        SELECT
            COALESCE(vendor_normalized, description) AS vendor,
            ROUND(SUM(abs_amount), 2) AS total,
            COUNT(*) AS txn_count
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
            AND (LOWER(category) LIKE '%labor%' OR LOWER(category) LIKE '%payroll%')
        GROUP BY vendor ORDER BY total DESC
        """

        date_params = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ])
        rev_rows = list(bq.query(rev_sql, job_config=date_params).result())
        exp_rows = list(bq.query(exp_sql, job_config=date_params).result())
        mrev_rows = list(bq.query(mrev_sql, job_config=date_params).result())
        mexp_rows = list(bq.query(mexp_sql, job_config=date_params).result())
        vendor_rows = list(bq.query(vendor_sql, job_config=date_params).result())

        # Helper: sum categories matching keywords
        def _sum_match(cats: dict, keywords: list) -> float:
            return sum(v for k, v in cats.items()
                       if any(kw.lower() in k.lower() for kw in keywords))

        # --- Build weekly data ---
        rev_by_week = {r.week_start: r for r in rev_rows}
        exp_by_week: dict = {}
        for r in exp_rows:
            w = r.week_start
            if w not in exp_by_week:
                exp_by_week[w] = {}
            exp_by_week[w][r.category] = float(r.total or 0)

        all_weeks = sorted(set(list(rev_by_week.keys()) + list(exp_by_week.keys())))
        weekly = []
        for w in all_weeks:
            rv = rev_by_week.get(w)
            ns = float(rv.net_sales or 0) if rv else 0
            tips = float(rv.tips or 0) if rv else 0
            grat = float(rv.gratuity or 0) if rv else 0

            grat_retained = round(grat * GRAT_RETAIN_PCT, 2)
            pass_through = round(tips + grat * GRAT_PASSTHROUGH_PCT, 2)
            adj_rev = round(ns + grat_retained, 2)

            cats = exp_by_week.get(w, {})
            labor_gross = _sum_match(cats, ["3. labor", "labor cost", "payroll"])
            labor_true = round(max(labor_gross - pass_through, 0), 2)
            rev_denom = adj_rev if adj_rev > 0 else 1
            labor_pct = round(labor_true / rev_denom * 100, 1)

            weekly.append({
                "week_start": w,
                "revenue": adj_rev,
                "labor_gross": round(labor_gross, 2),
                "labor_true": labor_true,
                "labor_pct": labor_pct,
                "pass_through": pass_through,
                "order_count": int(rv.order_count or 0) if rv else 0,
            })

        # --- Build monthly data ---
        mrev_map = {r.month: r for r in mrev_rows}
        mexp_map: dict = {}
        for r in mexp_rows:
            m = r.month
            if m not in mexp_map:
                mexp_map[m] = {}
            mexp_map[m][r.category] = float(r.total or 0)

        all_months = sorted(set(list(mrev_map.keys()) + list(mexp_map.keys())))
        monthly = []
        for m in all_months:
            rv = mrev_map.get(m)
            ns = float(rv.net_sales or 0) if rv else 0
            tips = float(rv.tips or 0) if rv else 0
            grat = float(rv.gratuity or 0) if rv else 0

            grat_retained = round(grat * GRAT_RETAIN_PCT, 2)
            pass_through = round(tips + grat * GRAT_PASSTHROUGH_PCT, 2)
            adj_rev = round(ns + grat_retained, 2)

            cats = mexp_map.get(m, {})
            cogs = _sum_match(cats, ["cost of goods", "cogs"])
            labor_gross = _sum_match(cats, ["3. labor", "labor cost", "payroll"])
            labor_true = round(max(labor_gross - pass_through, 0), 2)
            rev_denom = adj_rev if adj_rev > 0 else 1
            prime_cost = round(cogs + labor_true, 2)

            monthly.append({
                "month": m,
                "revenue": adj_rev,
                "labor_gross": round(labor_gross, 2),
                "labor_true": labor_true,
                "labor_pct": round(labor_true / rev_denom * 100, 1),
                "cogs": round(cogs, 2),
                "prime_cost_pct": round(prime_cost / rev_denom * 100, 1),
            })

        # --- Vendors ---
        by_vendor = [{
            "vendor": r.vendor or "(unknown)",
            "total": round(float(r.total or 0), 2),
            "txn_count": int(r.txn_count or 0),
        } for r in vendor_rows]

        # --- KPIs ---
        weeks_with_labor = [w for w in weekly if w["labor_true"] > 0]
        labor_pcts = [w["labor_pct"] for w in weeks_with_labor if w["revenue"] > 0]
        total_labor_true = sum(w["labor_true"] for w in weekly)

        kpis = {
            "avg_weekly_labor": round(total_labor_true / max(len(weeks_with_labor), 1), 2),
            "avg_labor_pct": round(sum(labor_pcts) / max(len(labor_pcts), 1), 1) if labor_pcts else 0,
            "best_week_pct": round(min(labor_pcts), 1) if labor_pcts else 0,
            "worst_week_pct": round(max(labor_pcts), 1) if labor_pcts else 0,
            "total_labor_true": round(total_labor_true, 2),
        }

        return jsonify({
            "kpis": kpis,
            "weekly": weekly,
            "monthly": monthly,
            "by_vendor": by_vendor,
        })

    except Exception as e:
        logging.exception("labor-analysis API error")
        return jsonify({"error": str(e)}), 500


# ─── Menu Engineering API ──────────────────────────────────────────────────
@bp.route("/api/menu-engineering", methods=["POST"])
def api_menu_engineering():
    """
    Menu engineering matrix: classify items as Stars/Plowhorses/Puzzles/Dogs.

    Request body:
    {
        "start_date": "2025-12-01",
        "end_date": "2026-02-27"
    }
    """
    try:
        body = request.get_json(silent=True) or {}
        start_date = body.get("start_date", "")
        end_date = body.get("end_date", "")
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400

        bq = bigquery.Client(project=PROJECT_ID)
        bd = BUSINESS_DAY_SQL.format(dt_col="CAST(order_date AS DATETIME)")

        base_filter = (
            f"{bd} BETWEEN @start_date AND @end_date "
            "AND (voided = 'false' OR voided IS NULL) "
            "AND (deferred = 'false' OR deferred IS NULL) "
            "AND order_date IS NOT NULL AND order_date != ''"
        )

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )

        dedup_cte = f"""deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY item_selection_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
            WHERE order_date IS NOT NULL AND order_date != ''
          ) WHERE _rn = 1
        )"""

        # All items with qty + revenue
        item_sql = f"""
        WITH {dedup_cte}
        SELECT
          COALESCE(menu_item, '(unknown)') AS menu_item,
          COALESCE(sales_category, '(uncategorized)') AS sales_category,
          COALESCE(menu_group, '(none)') AS menu_group,
          SUM(CAST(qty AS INT64)) AS qty_sold,
          SUM(CAST(net_price AS FLOAT64)) AS net_revenue,
          SAFE_DIVIDE(SUM(CAST(net_price AS FLOAT64)), NULLIF(SUM(CAST(qty AS INT64)), 0)) AS avg_price,
          SUM(CAST(gross_price AS FLOAT64)) AS gross_revenue
        FROM deduped
        WHERE {base_filter}
        GROUP BY menu_item, sales_category, menu_group
        HAVING SUM(CAST(qty AS INT64)) > 0
        ORDER BY net_revenue DESC
        """

        item_rows = list(bq.query(item_sql, job_config=job_config).result())

        if not item_rows:
            return jsonify({
                "kpis": {"total_items": 0, "stars_count": 0, "plowhorses_count": 0,
                         "puzzles_count": 0, "dogs_count": 0,
                         "stars_revenue_pct": 0, "plowhorses_revenue_pct": 0,
                         "puzzles_revenue_pct": 0, "dogs_revenue_pct": 0},
                "items": [], "categories": [],
                "matrix_thresholds": {"avg_qty": 0, "avg_price": 0},
            })

        # Compute thresholds
        total_qty = sum(int(r.qty_sold or 0) for r in item_rows)
        total_rev = sum(float(r.net_revenue or 0) for r in item_rows)
        n_items = len(item_rows)
        avg_qty = total_qty / n_items
        avg_price = total_rev / max(total_qty, 1)

        # Classify each item
        items = []
        for r in item_rows:
            qty = int(r.qty_sold or 0)
            rev = float(r.net_revenue or 0)
            price = float(r.avg_price or 0)
            pop_idx = round(qty / avg_qty, 2) if avg_qty > 0 else 0
            prof_idx = round(price / avg_price, 2) if avg_price > 0 else 0

            if pop_idx >= 1.0 and prof_idx >= 1.0:
                classification = "Star"
            elif pop_idx >= 1.0:
                classification = "Plowhorse"
            elif prof_idx >= 1.0:
                classification = "Puzzle"
            else:
                classification = "Dog"

            items.append({
                "menu_item": r.menu_item,
                "sales_category": r.sales_category,
                "menu_group": r.menu_group,
                "qty_sold": qty,
                "net_revenue": round(rev, 2),
                "avg_price": round(price, 2),
                "popularity_index": pop_idx,
                "profitability_index": prof_idx,
                "classification": classification,
                "revenue_pct": round(rev / max(total_rev, 1) * 100, 1),
                "qty_pct": round(qty / max(total_qty, 1) * 100, 1),
            })

        # KPIs by classification
        class_counts = {"Star": 0, "Plowhorse": 0, "Puzzle": 0, "Dog": 0}
        class_rev = {"Star": 0.0, "Plowhorse": 0.0, "Puzzle": 0.0, "Dog": 0.0}
        for item in items:
            c = item["classification"]
            class_counts[c] += 1
            class_rev[c] += item["net_revenue"]

        kpis = {
            "total_items": n_items,
            "stars_count": class_counts["Star"],
            "plowhorses_count": class_counts["Plowhorse"],
            "puzzles_count": class_counts["Puzzle"],
            "dogs_count": class_counts["Dog"],
            "stars_revenue_pct": round(class_rev["Star"] / max(total_rev, 1) * 100, 1),
            "plowhorses_revenue_pct": round(class_rev["Plowhorse"] / max(total_rev, 1) * 100, 1),
            "puzzles_revenue_pct": round(class_rev["Puzzle"] / max(total_rev, 1) * 100, 1),
            "dogs_revenue_pct": round(class_rev["Dog"] / max(total_rev, 1) * 100, 1),
        }

        # Category breakdown
        cat_map: dict = {}
        for item in items:
            cat = item["sales_category"]
            if cat not in cat_map:
                cat_map[cat] = {"category": cat, "revenue": 0, "qty": 0, "item_count": 0,
                                "stars": 0, "plowhorses": 0, "puzzles": 0, "dogs": 0}
            cat_map[cat]["revenue"] += item["net_revenue"]
            cat_map[cat]["qty"] += item["qty_sold"]
            cat_map[cat]["item_count"] += 1
            cat_map[cat][item["classification"].lower() + "s"] += 1

        categories = sorted(cat_map.values(), key=lambda x: x["revenue"], reverse=True)
        for c in categories:
            c["revenue"] = round(c["revenue"], 2)

        return jsonify({
            "kpis": kpis,
            "items": items,
            "categories": categories,
            "matrix_thresholds": {
                "avg_qty": round(avg_qty, 1),
                "avg_price": round(avg_price, 2),
            },
        })

    except Exception as e:
        logging.exception("menu-engineering API error")
        return jsonify({"error": str(e)}), 500


# ─── Guest Intelligence API ──────────────────────────────────────────────────
@bp.route("/api/customer-loyalty", methods=["POST"])
def api_customer_loyalty():
    """
    Guest intelligence: card-based RFM segmentation, visit patterns,
    revenue concentration, and behavioral analytics.

    Request body:
    {
        "start_date": "2025-06-01",
        "end_date": "2026-02-28"
    }
    """
    try:
        body = request.get_json(silent=True) or {}
        start_date = body.get("start_date", "")
        end_date = body.get("end_date", "")
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400

        bq = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start_date),
                bigquery.ScalarQueryParameter("end", "DATE", end_date),
            ]
        )

        # ── Q1: Card-level RFM aggregates ───────────────────────────────
        q_cards = f"""
        WITH deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY payment_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE status IN ('CAPTURED','AUTHORIZED','CAPTURE_IN_PROGRESS')
              AND last_4_card_digits IS NOT NULL
              AND last_4_card_digits != ''
              AND DATE(CAST(paid_date AS DATETIME))
                  BETWEEN @start AND @end
          ) WHERE _rn = 1
        )
        SELECT
          last_4_card_digits AS card,
          card_type,
          COUNT(*) AS txn_count,
          COUNT(DISTINCT DATE(CAST(paid_date AS DATETIME))) AS visit_days,
          ROUND(SUM(amount), 2) AS total_spend,
          ROUND(AVG(amount), 2) AS avg_per_visit,
          ROUND(SAFE_DIVIDE(SUM(tip), NULLIF(SUM(amount), 0)) * 100,
                1) AS tip_pct,
          MIN(DATE(CAST(paid_date AS DATETIME))) AS first_seen,
          MAX(DATE(CAST(paid_date AS DATETIME))) AS last_seen,
          DATE_DIFF(@end,
                    MAX(DATE(CAST(paid_date AS DATETIME))), DAY
          ) AS recency_days
        FROM deduped
        GROUP BY last_4_card_digits, card_type
        ORDER BY total_spend DESC
        """

        # ── Q2: Monthly guest flow ──────────────────────────────────────
        q_monthly = f"""
        WITH deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY payment_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE status IN ('CAPTURED','AUTHORIZED','CAPTURE_IN_PROGRESS')
              AND last_4_card_digits IS NOT NULL
              AND last_4_card_digits != ''
              AND DATE(CAST(paid_date AS DATETIME))
                  BETWEEN @start AND @end
          ) WHERE _rn = 1
        ),
        card_key AS (
          SELECT *,
            CONCAT(last_4_card_digits, '-',
                   COALESCE(card_type, '')) AS ckey,
            FORMAT_DATE('%Y-%m',
              DATE(CAST(paid_date AS DATETIME))) AS month
          FROM deduped
        ),
        card_first AS (
          SELECT ckey, MIN(month) AS first_month
          FROM card_key GROUP BY ckey
        )
        SELECT
          ck.month,
          COUNT(DISTINCT ck.ckey) AS active_guests,
          COUNT(DISTINCT CASE WHEN ck.month = cf.first_month
                              THEN ck.ckey END) AS new_guests,
          COUNT(DISTINCT CASE WHEN ck.month != cf.first_month
                              THEN ck.ckey END) AS returning_guests,
          ROUND(SUM(ck.amount), 2) AS total_revenue,
          ROUND(SUM(CASE WHEN ck.month != cf.first_month
                         THEN ck.amount ELSE 0 END), 2) AS repeat_revenue
        FROM card_key ck
        JOIN card_first cf ON ck.ckey = cf.ckey
        GROUP BY ck.month
        ORDER BY ck.month
        """

        # ── Q3: DOW + hourly patterns by frequency tier ─────────────────
        q_patterns = f"""
        WITH deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY payment_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE status IN ('CAPTURED','AUTHORIZED','CAPTURE_IN_PROGRESS')
              AND last_4_card_digits IS NOT NULL
              AND last_4_card_digits != ''
              AND DATE(CAST(paid_date AS DATETIME))
                  BETWEEN @start AND @end
          ) WHERE _rn = 1
        ),
        card_freq AS (
          SELECT
            CONCAT(last_4_card_digits, '-',
                   COALESCE(card_type, '')) AS ckey,
            COUNT(DISTINCT DATE(CAST(paid_date AS DATETIME))) AS vd
          FROM deduped GROUP BY ckey
        ),
        card_tier AS (
          SELECT ckey,
            CASE WHEN vd >= 10 THEN 'Champions'
                 WHEN vd >= 5  THEN 'Regulars'
                 WHEN vd >= 2  THEN 'Returning'
                 ELSE 'New' END AS tier
          FROM card_freq
        )
        SELECT
          ct.tier,
          FORMAT_DATE('%A',
            DATE(CAST(dp.paid_date AS DATETIME))) AS day_name,
          EXTRACT(DAYOFWEEK FROM
            DATE(CAST(dp.paid_date AS DATETIME))) AS dow_num,
          EXTRACT(HOUR FROM
            CAST(dp.paid_date AS DATETIME)) AS hour,
          COUNT(*) AS txn_count,
          ROUND(SUM(dp.amount), 2) AS revenue,
          ROUND(AVG(dp.amount), 2) AS avg_txn
        FROM deduped dp
        JOIN card_tier ct
          ON CONCAT(dp.last_4_card_digits, '-',
                    COALESCE(dp.card_type, '')) = ct.ckey
        GROUP BY ct.tier, day_name, dow_num, hour
        ORDER BY ct.tier, dow_num, hour
        """

        # ── Q4: Contact enrichment (CheckDetails + PaymentDetails) ──────
        q_contacts = f"""
        WITH check_deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY check_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
            WHERE customer_id IS NOT NULL AND customer_id != ''
              AND PARSE_DATE('%m/%d/%y', opened_date)
                  BETWEEN @start AND @end
          ) WHERE _rn = 1
        ),
        pay_link AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY payment_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE status IN ('CAPTURED','AUTHORIZED','CAPTURE_IN_PROGRESS')
              AND DATE(CAST(paid_date AS DATETIME))
                  BETWEEN @start AND @end
          ) WHERE _rn = 1
        )
        SELECT
          cd.customer_id,
          MAX(cd.customer) AS name,
          MAX(cd.customer_phone) AS phone,
          MAX(cd.customer_email) AS email,
          COUNT(DISTINCT cd.check_id) AS visits,
          ROUND(SUM(CAST(cd.total AS FLOAT64)), 2) AS total_spend,
          ROUND(AVG(CAST(cd.total AS FLOAT64)), 2) AS avg_check,
          MIN(PARSE_DATE('%m/%d/%y', cd.opened_date)) AS first_visit,
          MAX(PARSE_DATE('%m/%d/%y', cd.opened_date)) AS last_visit,
          DATE_DIFF(@end,
            MAX(PARSE_DATE('%m/%d/%y', cd.opened_date)), DAY
          ) AS recency_days,
          MAX(pl.last_4_card_digits) AS linked_card,
          MAX(pl.card_type) AS linked_card_type
        FROM check_deduped cd
        LEFT JOIN pay_link pl ON cd.check_id = pl.check_id
        GROUP BY cd.customer_id
        ORDER BY total_spend DESC
        """

        # ── Execute queries ─────────────────────────────────────────────
        rows_cards = list(
            bq.query(q_cards, job_config=job_config).result())
        rows_monthly = list(
            bq.query(q_monthly, job_config=job_config).result())
        rows_patterns = list(
            bq.query(q_patterns, job_config=job_config).result())
        rows_contacts = list(
            bq.query(q_contacts, job_config=job_config).result())

        # ── Segment each card ───────────────────────────────────────────
        SEG_ORDER = ["champions", "loyal", "regulars", "returning",
                     "new", "at_risk", "dormant"]

        def classify(vd, rec):
            if rec > 90:
                return "dormant"
            if vd >= 10 and rec <= 30:
                return "champions"
            if vd >= 10:
                return "loyal"        # high freq, not super recent
            if vd >= 5 and rec <= 45:
                return "regulars"
            if vd >= 3 and rec > 45:
                return "at_risk"
            if vd >= 2:
                return "returning"
            if rec <= 30:
                return "new"
            return "dormant"

        all_cards = []
        seg_data = {s: {"count": 0, "revenue": 0.0, "spends": []}
                    for s in SEG_ORDER}
        total_revenue = 0.0
        total_txns = 0

        for r in rows_cards:
            vd = r.visit_days or 0
            rec = r.recency_days or 0
            spend = float(r.total_spend or 0)
            seg = classify(vd, rec)

            seg_data[seg]["count"] += 1
            seg_data[seg]["revenue"] += spend
            seg_data[seg]["spends"].append(spend)
            total_revenue += spend
            total_txns += (r.txn_count or 0)

            all_cards.append({
                "card": r.card,
                "card_type": r.card_type or "",
                "txn_count": r.txn_count or 0,
                "visit_days": vd,
                "total_spend": spend,
                "avg_per_visit": float(r.avg_per_visit or 0),
                "tip_pct": float(r.tip_pct or 0),
                "first_seen": str(r.first_seen) if r.first_seen else "",
                "last_seen": str(r.last_seen) if r.last_seen else "",
                "recency_days": rec,
                "segment": seg,
            })

        total_guests = len(all_cards)
        repeat_guests = sum(1 for c in all_cards if c["visit_days"] > 1)
        repeat_pct = (round(repeat_guests / total_guests * 100, 1)
                      if total_guests else 0)
        repeat_rev = sum(c["total_spend"] for c in all_cards
                         if c["visit_days"] > 1)
        repeat_rev_pct = (round(repeat_rev / total_revenue * 100, 1)
                          if total_revenue else 0)
        avg_visits_repeat = (
            round(sum(c["visit_days"] for c in all_cards
                      if c["visit_days"] > 1) / repeat_guests, 1)
            if repeat_guests else 0)
        avg_spend = (round(total_revenue / total_txns, 2)
                     if total_txns else 0)
        rev_per_guest = (round(total_revenue / total_guests, 2)
                         if total_guests else 0)
        at_risk_ct = seg_data["at_risk"]["count"]
        at_risk_rev = round(seg_data["at_risk"]["revenue"], 2)

        # Build segments response
        segments = {}
        for seg in SEG_ORDER:
            d = seg_data[seg]
            cnt = d["count"]
            rev = d["revenue"]
            sp = d["spends"]
            segments[seg] = {
                "count": cnt,
                "pct_of_guests": round(cnt / total_guests * 100, 1)
                                 if total_guests else 0,
                "revenue": round(rev, 2),
                "revenue_pct": round(rev / total_revenue * 100, 1)
                               if total_revenue else 0,
                "avg_spend": round(sum(sp) / len(sp), 2) if sp else 0,
            }

        # ── Revenue concentration (power law) ───────────────────────────
        sorted_by_spend = sorted(all_cards,
                                 key=lambda x: -x["total_spend"])
        concentration = []
        for pct_label, pct_val in [("Top 5%", 0.05), ("Top 10%", 0.10),
                                    ("Top 20%", 0.20), ("Top 50%", 0.50)]:
            n = max(1, int(total_guests * pct_val))
            rev_slice = sum(c["total_spend"]
                            for c in sorted_by_spend[:n])
            concentration.append({
                "label": pct_label,
                "guests": n,
                "revenue": round(rev_slice, 2),
                "revenue_pct": round(rev_slice / total_revenue * 100, 1)
                               if total_revenue else 0,
            })

        # ── Frequency distribution ──────────────────────────────────────
        freq_bands = [
            ("1 visit", 1, 1), ("2-3 visits", 2, 3),
            ("4-6 visits", 4, 6), ("7-12 visits", 7, 12),
            ("13-25 visits", 13, 25), ("26+ visits", 26, 9999),
        ]
        freq_dist = []
        for label, lo, hi in freq_bands:
            cards_in = [c for c in all_cards
                        if lo <= c["visit_days"] <= hi]
            cnt = len(cards_in)
            rev = sum(c["total_spend"] for c in cards_in)
            avg = round(rev / cnt, 2) if cnt else 0
            freq_dist.append({
                "band": label,
                "guests": cnt,
                "pct_guests": round(cnt / total_guests * 100, 1)
                              if total_guests else 0,
                "revenue": round(rev, 2),
                "pct_revenue": round(rev / total_revenue * 100, 1)
                               if total_revenue else 0,
                "avg_spend": avg,
            })

        # ── Monthly trend ───────────────────────────────────────────────
        monthly = []
        for r in rows_monthly:
            tot_rev = float(r.total_revenue or 0)
            rep_rev = float(r.repeat_revenue or 0)
            act = r.active_guests or 0
            ret = r.returning_guests or 0
            monthly.append({
                "month": r.month,
                "active": act,
                "new": r.new_guests or 0,
                "returning": ret,
                "return_pct": round(ret / act * 100, 1) if act else 0,
                "revenue": round(tot_rev, 2),
                "repeat_revenue": round(rep_rev, 2),
                "repeat_rev_pct": round(rep_rev / tot_rev * 100, 1)
                                  if tot_rev else 0,
            })

        # ── DOW patterns (aggregate across tiers) ───────────────────────
        dow_agg: dict = {}
        hour_agg: dict = {}
        tier_dow: dict = {}
        for r in rows_patterns:
            tier = r.tier or "New"
            dn = r.day_name or ""
            dow_n = r.dow_num or 0
            hr = r.hour if r.hour is not None else 0
            txn = r.txn_count or 0
            rev = float(r.revenue or 0)

            # DOW totals
            if dow_n not in dow_agg:
                dow_agg[dow_n] = {"day": dn, "txns": 0, "revenue": 0.0}
            dow_agg[dow_n]["txns"] += txn
            dow_agg[dow_n]["revenue"] += rev

            # Hour totals
            if hr not in hour_agg:
                hour_agg[hr] = {"txns": 0, "revenue": 0.0}
            hour_agg[hr]["txns"] += txn
            hour_agg[hr]["revenue"] += rev

            # Tier x DOW
            if tier not in tier_dow:
                tier_dow[tier] = {}
            if dow_n not in tier_dow[tier]:
                tier_dow[tier][dow_n] = {"txns": 0, "revenue": 0.0}
            tier_dow[tier][dow_n]["txns"] += txn
            tier_dow[tier][dow_n]["revenue"] += rev

        dow_list = []
        for dow_n in sorted(dow_agg.keys()):
            d = dow_agg[dow_n]
            entry = {"day": d["day"], "txns": d["txns"],
                     "revenue": round(d["revenue"], 2)}
            for t in ["Champions", "Regulars", "Returning", "New"]:
                td = tier_dow.get(t, {}).get(dow_n, {})
                entry[t.lower() + "_txns"] = td.get("txns", 0)
            dow_list.append(entry)

        hour_list = []
        for hr in sorted(hour_agg.keys()):
            d = hour_agg[hr]
            label = (f"{hr % 12 or 12}{'AM' if hr < 12 else 'PM'}")
            hour_list.append({
                "hour": hr, "label": label,
                "txns": d["txns"],
                "revenue": round(d["revenue"], 2),
            })

        # ── Top 50 repeat guests ────────────────────────────────────────
        top_guests = sorted(
            [c for c in all_cards if c["visit_days"] > 1],
            key=lambda x: (-x["visit_days"], -x["total_spend"])
        )[:50]

        # ── Contact enrichment ─────────────────────────────────────────
        def clean_phone(raw):
            if not raw:
                return ""
            p = str(raw).replace(".0", "").strip()
            if p in ("", "5555555555", "15555555555"):
                return ""
            # Strip leading 1 if 11 digits
            if len(p) == 11 and p.startswith("1"):
                p = p[1:]
            if len(p) == 10:
                return f"+1{p}"
            return p

        contacts_list = []
        ct_with_email = 0
        ct_with_phone = 0
        for r in rows_contacts:
            vd = r.visits or 0
            rec = r.recency_days or 0
            seg = classify(vd, rec)
            email = (r.email or "").strip()
            phone = clean_phone(r.phone)
            if email:
                ct_with_email += 1
            if phone:
                ct_with_phone += 1
            contacts_list.append({
                "customer_id": r.customer_id,
                "name": r.name or "",
                "email": email,
                "phone": phone,
                "visits": vd,
                "total_spend": float(r.total_spend or 0),
                "avg_check": float(r.avg_check or 0),
                "first_visit": str(r.first_visit) if r.first_visit else "",
                "last_visit": str(r.last_visit) if r.last_visit else "",
                "recency_days": rec,
                "segment": seg,
                "linked_card": r.linked_card or "",
                "linked_card_type": r.linked_card_type or "",
            })

        return jsonify({
            "kpis": {
                "total_guests": total_guests,
                "repeat_pct": repeat_pct,
                "repeat_rev_pct": repeat_rev_pct,
                "avg_visits_repeat": avg_visits_repeat,
                "avg_spend_per_visit": avg_spend,
                "rev_per_guest": rev_per_guest,
                "at_risk_count": at_risk_ct,
                "at_risk_revenue": at_risk_rev,
                "total_revenue": round(total_revenue, 2),
                "total_txns": total_txns,
            },
            "segments": segments,
            "concentration": concentration,
            "freq_distribution": freq_dist,
            "monthly": monthly,
            "patterns": {
                "day_of_week": dow_list,
                "hourly": hour_list,
            },
            "top_guests": top_guests,
            "contacts": {
                "total": len(contacts_list),
                "with_email": ct_with_email,
                "with_phone": ct_with_phone,
                "guests": contacts_list,
            },
        })

    except Exception as e:
        logging.exception("customer-loyalty API error")
        return jsonify({"error": str(e)}), 500


# ─── Guest Export CSV ────────────────────────────────────────────────────────
@bp.route("/api/guest-export", methods=["GET"])
def api_guest_export():
    """Export enriched guest contacts as CSV for SevenRooms import."""
    try:
        start_date = request.args.get("start_date", "")
        end_date = request.args.get("end_date", "")
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400

        bq = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start_date),
                bigquery.ScalarQueryParameter("end", "DATE", end_date),
            ]
        )

        q = f"""
        WITH check_deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY check_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
            WHERE customer_id IS NOT NULL AND customer_id != ''
              AND PARSE_DATE('%m/%d/%y', opened_date)
                  BETWEEN @start AND @end
          ) WHERE _rn = 1
        ),
        pay_link AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY payment_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE status IN ('CAPTURED','AUTHORIZED','CAPTURE_IN_PROGRESS')
              AND DATE(CAST(paid_date AS DATETIME))
                  BETWEEN @start AND @end
          ) WHERE _rn = 1
        )
        SELECT
          cd.customer_id,
          MAX(cd.customer) AS name,
          MAX(cd.customer_phone) AS phone,
          MAX(cd.customer_email) AS email,
          COUNT(DISTINCT cd.check_id) AS visits,
          ROUND(SUM(CAST(cd.total AS FLOAT64)), 2) AS total_spend,
          ROUND(AVG(CAST(cd.total AS FLOAT64)), 2) AS avg_check,
          MIN(PARSE_DATE('%m/%d/%y', cd.opened_date)) AS first_visit,
          MAX(PARSE_DATE('%m/%d/%y', cd.opened_date)) AS last_visit,
          DATE_DIFF(@end,
            MAX(PARSE_DATE('%m/%d/%y', cd.opened_date)), DAY
          ) AS recency_days,
          MAX(pl.last_4_card_digits) AS linked_card,
          MAX(pl.card_type) AS linked_card_type
        FROM check_deduped cd
        LEFT JOIN pay_link pl ON cd.check_id = pl.check_id
        GROUP BY cd.customer_id
        ORDER BY total_spend DESC
        """

        rows = list(bq.query(q, job_config=job_config).result())

        def clean_phone(raw):
            if not raw:
                return ""
            p = str(raw).replace(".0", "").strip()
            if p in ("", "5555555555", "15555555555"):
                return ""
            if len(p) == 11 and p.startswith("1"):
                p = p[1:]
            if len(p) == 10:
                return f"+1{p}"
            return p

        def classify_seg(vd, rec):
            if rec > 90:
                return "Dormant"
            if vd >= 10 and rec <= 30:
                return "Champions"
            if vd >= 10:
                return "Loyal"
            if vd >= 5 and rec <= 45:
                return "Regulars"
            if vd >= 3 and rec > 45:
                return "At Risk"
            if vd >= 2:
                return "Returning"
            if rec <= 30:
                return "New"
            return "Dormant"

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "first_name", "last_name", "email", "phone",
            "visits", "total_spend", "avg_check",
            "first_visit", "last_visit", "segment", "tags",
        ])

        for r in rows:
            name = (r.name or "").strip()
            parts = name.split(None, 1)
            first = parts[0] if parts else ""
            last = parts[1] if len(parts) > 1 else ""
            email = (r.email or "").strip()
            phone = clean_phone(r.phone)
            if not email and not phone:
                continue
            vd = r.visits or 0
            rec = r.recency_days or 0
            seg = classify_seg(vd, rec)
            spend = float(r.total_spend or 0)

            # Build tags
            tags = [seg]
            if spend >= 500:
                tags.append("High Spender")
            elif spend >= 100:
                tags.append("Medium Spender")
            if vd >= 5:
                tags.append("Frequent Visitor")
            elif vd >= 2:
                tags.append("Repeat Visitor")
            tags.append("LOV3 Guest")

            writer.writerow([
                first, last, email, phone,
                vd, f"{spend:.2f}", f"{float(r.avg_check or 0):.2f}",
                str(r.first_visit) if r.first_visit else "",
                str(r.last_visit) if r.last_visit else "",
                seg, "; ".join(tags),
            ])

        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition":
                    f"attachment; filename=lov3_guests_{end_date}.csv"
            },
        )

    except Exception as e:
        logging.exception("guest-export error")
        return jsonify({"error": str(e)}), 500


# ─── KPI Benchmarking API ────────────────────────────────────────────────────
@bp.route("/api/kpi-benchmarks", methods=["POST"])
def api_kpi_benchmarks():
    """
    KPI scorecard: actual metrics vs industry benchmarks for nightlife venues.

    Request body:
    {
        "start_date": "2026-03-01",
        "end_date": "2026-03-31"
    }

    Returns financial health, operational, and guest intelligence metrics with
    green/yellow/red status, prior-period deltas, and 6-month trend data.
    """
    data = request.get_json() or {}
    ck = _cache_key("kpi_benchmarks", data)
    cached = _cache_get(ck)
    if cached is not None:
        return jsonify(cached)

    # ── Benchmark thresholds (tunable) ────────────────────────────────────
    KPI_BENCHMARKS = {
        "cogs_pct": {
            "label": "COGS %", "format": "pct",
            "description": "Cost of goods sold as % of adjusted revenue",
            "good_max": 30.0, "watch_max": 35.0, "direction": "lower_is_better",
            "source": "Industry 25-35% full-service; lounge target 25-30%",
        },
        "labor_pct": {
            "label": "True Labor %", "format": "pct",
            "description": "Labor cost (less pass-through) as % of adjusted revenue",
            "good_max": 28.0, "watch_max": 33.0, "direction": "lower_is_better",
            "source": "Full-service 25-35%; nightlife 22-30%",
        },
        "prime_cost_pct": {
            "label": "Prime Cost %", "format": "pct",
            "description": "COGS + True Labor as % of adjusted revenue",
            "good_max": 55.0, "watch_max": 58.0, "direction": "lower_is_better",
            "source": "Industry target <60%; nightlife <55% excellent",
        },
        "net_margin_pct": {
            "label": "Net Profit Margin", "format": "pct",
            "description": "Net profit as % of adjusted revenue",
            "good_min": 12.0, "watch_min": 5.0, "direction": "higher_is_better",
            "source": "Bar/lounge 10-25%; restaurant avg 3-5%",
        },
        "marketing_pct": {
            "label": "Marketing %", "format": "pct",
            "description": "Marketing & entertainment as % of revenue",
            "good_max": 5.0, "watch_max": 8.0, "direction": "lower_is_better",
            "source": "Restaurant standard 3-6%",
        },
        "opex_pct": {
            "label": "OPEX %", "format": "pct",
            "description": "Operating expenses as % of revenue",
            "good_max": 15.0, "watch_max": 20.0, "direction": "lower_is_better",
            "source": "Target <15% for lean operations",
        },
        "avg_check": {
            "label": "Avg Check", "format": "dollar",
            "description": "Average order amount",
            "good_min": 50.0, "watch_min": 35.0, "direction": "higher_is_better",
            "source": "Upscale lounge $50+; nightlife $35-75",
        },
        "orders_per_day": {
            "label": "Orders / Day", "format": "number",
            "description": "Average daily order count",
            "good_min": 60.0, "watch_min": 40.0, "direction": "higher_is_better",
            "source": "LOV3 internal target",
        },
        "void_rate_pct": {
            "label": "Void Rate", "format": "pct",
            "description": "Voided amount as % of gross revenue",
            "good_max": 1.0, "watch_max": 3.0, "direction": "lower_is_better",
            "source": "Industry: <1% good, >3% investigate",
        },
        "discount_rate_pct": {
            "label": "Discount Rate", "format": "pct",
            "description": "Discounts as % of (gross + discounts)",
            "good_max": 5.0, "watch_max": 10.0, "direction": "lower_is_better",
            "source": "Target <5%; 8-10% max comfort",
        },
        "rev_per_labor_hour": {
            "label": "Rev / Labor Hr", "format": "dollar",
            "description": "Revenue per estimated labor hour ($18/hr proxy)",
            "good_min": 60.0, "watch_min": 40.0, "direction": "higher_is_better",
            "source": "Full-service $40-80; bar/lounge $60+",
        },
        "repeat_guest_pct": {
            "label": "Repeat Guest %", "format": "pct",
            "description": "Guests with 2+ visits (card-based)",
            "good_min": 30.0, "watch_min": 20.0, "direction": "higher_is_better",
            "source": "Restaurant avg 30-40%",
        },
        "repeat_rev_pct": {
            "label": "Repeat Revenue %", "format": "pct",
            "description": "Revenue from returning guests",
            "good_min": 50.0, "watch_min": 35.0, "direction": "higher_is_better",
            "source": "Industry: 60-80% from regulars",
        },
        "at_risk_pct": {
            "label": "At-Risk Guests %", "format": "pct",
            "description": "Guests in at-risk churn segment",
            "good_max": 5.0, "watch_max": 10.0, "direction": "lower_is_better",
            "source": "LOV3 internal: minimize guest churn",
        },
    }

    def _classify(value: float, bench: dict) -> str:
        d = bench.get("direction", "lower_is_better")
        if d == "lower_is_better":
            if value <= bench.get("good_max", float("inf")):
                return "good"
            if value <= bench.get("watch_max", float("inf")):
                return "watch"
            return "critical"
        else:
            if value >= bench.get("good_min", 0):
                return "good"
            if value >= bench.get("watch_min", 0):
                return "watch"
            return "critical"

    def _prior_period(start_s: str, end_s: str):
        s = datetime.strptime(start_s, "%Y-%m-%d")
        e = datetime.strptime(end_s, "%Y-%m-%d")
        if s.year == e.year and s.month == e.month:
            pm = s.month - 1 if s.month > 1 else 12
            py = s.year if s.month > 1 else s.year - 1
            max_d = calendar.monthrange(py, pm)[1]
            ps = s.replace(year=py, month=pm, day=min(s.day, max_d))
            pe = e.replace(year=py, month=pm, day=min(e.day, max_d))
        else:
            ps = s.replace(year=s.year - 1)
            pe = e.replace(year=e.year - 1)
        return ps.strftime("%Y-%m-%d"), pe.strftime("%Y-%m-%d")

    def _run_period(bq_client, sd: str, ed: str):
        """Run all queries for a given date range and return computed metrics."""
        period_params = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("sd", "STRING", sd),
            bigquery.ScalarQueryParameter("ed", "STRING", ed),
        ])
        # Q1: Revenue + Orders + Discounts from OrderDetails
        rev_q = f"""
        SELECT
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS total_tips,
            COALESCE(SUM(gratuity), 0) AS total_gratuity,
            COUNT(DISTINCT order_id) AS order_count,
            COUNT(DISTINCT processing_date) AS operating_days,
            COALESCE(SUM(discount_amount), 0) AS total_discounts
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @sd) AND PARSE_DATE('%Y-%m-%d', @ed)
          AND (voided IS NULL OR voided = 'false')
        """
        rev_row = list(bq_client.query(rev_q, job_config=period_params).result())[0]
        net_sales = float(rev_row.net_sales or 0)
        total_tips = float(rev_row.total_tips or 0)
        total_grat = float(rev_row.total_gratuity or 0)
        order_count = int(rev_row.order_count or 0)
        operating_days = int(rev_row.operating_days or 0)
        total_discounts = abs(float(rev_row.total_discounts or 0))

        grat_retained = round(total_grat * GRAT_RETAIN_PCT, 2)
        total_pass_through = round(total_tips + total_grat * GRAT_PASSTHROUGH_PCT, 2)
        adjusted_revenue = round(net_sales + grat_retained, 2)
        rev_denom = adjusted_revenue if adjusted_revenue > 0 else 1

        avg_check = round(net_sales / order_count, 2) if order_count > 0 else 0
        orders_per_day = round(order_count / operating_days, 1) if operating_days > 0 else 0
        gross_plus_disc = net_sales + total_discounts
        discount_rate = round(total_discounts / gross_plus_disc * 100, 1) if gross_plus_disc > 0 else 0

        # Void amount from OrderDetails (voided orders)
        void_q = f"""
        SELECT COALESCE(SUM(ABS(amount)), 0) AS voided_amount
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @sd) AND PARSE_DATE('%Y-%m-%d', @ed)
          AND voided = 'true'
        """
        void_row = list(bq_client.query(void_q, job_config=period_params).result())[0]
        voided_amount = float(void_row.voided_amount or 0)
        gross_for_void = net_sales + voided_amount
        void_rate = round(voided_amount / gross_for_void * 100, 1) if gross_for_void > 0 else 0

        # Q2: Expenses by category from BankTransactions
        bank_table = f"{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw"
        has_bank = True
        try:
            bq_client.get_table(bank_table)
        except NotFound:
            has_bank = False

        expenses = {}
        total_cogs = 0.0
        labor_gross = 0.0
        labor_true = 0.0
        marketing_total = 0.0
        opex_total = 0.0
        net_profit = 0.0
        total_expenses_adj = 0.0

        if has_bank:
            exp_q = f"""
            SELECT category, ROUND(SUM(abs_amount), 2) AS total
            FROM `{bank_table}`
            WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @sd) AND PARSE_DATE('%Y-%m-%d', @ed)
              AND transaction_type = 'debit'
            GROUP BY category ORDER BY total DESC
            """
            for row in bq_client.query(exp_q, job_config=period_params).result():
                expenses[row.category] = float(row.total or 0)

            def sum_matching(exps, keywords):
                t = 0.0
                for cat, amt in exps.items():
                    cl = cat.lower()
                    if any(kw.lower() in cl for kw in keywords):
                        t += amt
                return t

            total_cogs = sum_matching(expenses, ["cost of goods", "cogs"])
            labor_gross = sum_matching(expenses, ["3. labor", "labor cost", "payroll"])
            marketing_total = sum_matching(expenses, ["marketing", "promotions", "entertainment", "event"])
            opex_total = sum_matching(expenses, ["operating expenses", "opex"])

            labor_true = round(labor_gross - total_pass_through, 2)
            if labor_true < 0:
                labor_true = 0.0

            total_raw = sum(v for k, v in expenses.items() if "revenue" not in k.lower())
            total_expenses_adj = round(total_raw - total_pass_through, 2)
            if total_expenses_adj < 0:
                total_expenses_adj = total_raw
            net_profit = round(adjusted_revenue - total_expenses_adj, 2)

        cogs_pct = round(total_cogs / rev_denom * 100, 1)
        labor_pct = round(labor_true / rev_denom * 100, 1)
        prime_cost_pct = round((total_cogs + labor_true) / rev_denom * 100, 1)
        net_margin_pct = round(net_profit / rev_denom * 100, 1)
        marketing_pct = round(marketing_total / rev_denom * 100, 1)
        opex_pct = round(opex_total / rev_denom * 100, 1)

        # Rev per labor hour (estimated at $18/hr Houston avg)
        est_labor_hours = labor_true / 18.0 if labor_true > 0 else 1
        rev_per_labor_hour = round(adjusted_revenue / est_labor_hours, 2) if labor_true > 0 else 0

        # Q3: Guest intelligence (simplified card-based)
        guest_q = f"""
        WITH deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY payment_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE status IN ('CAPTURED','AUTHORIZED','CAPTURE_IN_PROGRESS')
              AND last_4_card_digits IS NOT NULL
              AND last_4_card_digits != ''
              AND DATE(CAST(paid_date AS DATETIME)) BETWEEN @sd AND @ed
          ) WHERE _rn = 1
        ),
        cards AS (
          SELECT
            CONCAT(last_4_card_digits, '-', COALESCE(card_type, '')) AS ckey,
            COUNT(DISTINCT DATE(CAST(paid_date AS DATETIME))) AS visit_days,
            ROUND(SUM(amount), 2) AS total_spend,
            DATE_DIFF(PARSE_DATE('%Y-%m-%d', @ed), MAX(DATE(CAST(paid_date AS DATETIME))), DAY) AS recency
          FROM deduped
          GROUP BY ckey
        )
        SELECT
          COUNT(*) AS total_guests,
          COUNTIF(visit_days >= 2) AS repeat_guests,
          SUM(total_spend) AS total_revenue,
          SUM(CASE WHEN visit_days >= 2 THEN total_spend ELSE 0 END) AS repeat_revenue,
          COUNTIF(visit_days >= 3 AND recency > 45 AND recency <= 90) AS at_risk_count
        FROM cards
        """
        guest_row = list(bq_client.query(guest_q, job_config=period_params).result())[0]
        total_guests = int(guest_row.total_guests or 0)
        repeat_guests = int(guest_row.repeat_guests or 0)
        total_guest_rev = float(guest_row.total_revenue or 0)
        repeat_revenue = float(guest_row.repeat_revenue or 0)
        at_risk_count = int(guest_row.at_risk_count or 0)

        repeat_guest_pct = round(repeat_guests / total_guests * 100, 1) if total_guests > 0 else 0
        repeat_rev_pct = round(repeat_revenue / total_guest_rev * 100, 1) if total_guest_rev > 0 else 0
        at_risk_pct = round(at_risk_count / total_guests * 100, 1) if total_guests > 0 else 0

        return {
            "adjusted_revenue": adjusted_revenue,
            "net_profit": net_profit,
            "order_count": order_count,
            "operating_days": operating_days,
            "has_bank_data": has_bank,
            "metrics": {
                "cogs_pct": cogs_pct,
                "labor_pct": labor_pct,
                "prime_cost_pct": prime_cost_pct,
                "net_margin_pct": net_margin_pct,
                "marketing_pct": marketing_pct,
                "opex_pct": opex_pct,
                "avg_check": avg_check,
                "orders_per_day": orders_per_day,
                "void_rate_pct": void_rate,
                "discount_rate_pct": discount_rate,
                "rev_per_labor_hour": rev_per_labor_hour,
                "repeat_guest_pct": repeat_guest_pct,
                "repeat_rev_pct": repeat_rev_pct,
                "at_risk_pct": at_risk_pct,
            },
            "guest_detail": {
                "total_guests": total_guests,
                "repeat_guests": repeat_guests,
                "at_risk_count": at_risk_count,
                "repeat_revenue": round(repeat_revenue, 2),
            },
        }

    # ── Main handler ──────────────────────────────────────────────────────
    try:
        body = request.get_json(silent=True) or {}
        start_date = body.get("start_date", "")
        end_date = body.get("end_date", "")
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date required"}), 400

        bq = bigquery.Client(project=PROJECT_ID)

        # Current period
        current = _run_period(bq, start_date, end_date)

        # Prior period
        prior_start, prior_end = _prior_period(start_date, end_date)
        prior = _run_period(bq, prior_start, prior_end)

        # Build scorecard with deltas and status
        scorecard = []
        for key, bench in KPI_BENCHMARKS.items():
            val = current["metrics"].get(key, 0)
            pval = prior["metrics"].get(key, 0)
            delta = round(val - pval, 2)
            status = _classify(val, bench)
            scorecard.append({
                "key": key,
                "label": bench["label"],
                "format": bench["format"],
                "value": val,
                "prior": pval,
                "delta": delta,
                "status": status,
                "direction": bench["direction"],
                "description": bench["description"],
            })

        good_count = sum(1 for s in scorecard if s["status"] == "good")
        watch_count = sum(1 for s in scorecard if s["status"] == "watch")
        crit_count = sum(1 for s in scorecard if s["status"] == "critical")
        health = "good" if good_count >= len(scorecard) / 2 else ("watch" if crit_count < len(scorecard) / 3 else "critical")

        # Monthly trends (last 6 months from end_date)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        trend_months = []
        trend_data = {}
        for i in range(5, -1, -1):
            m = end_dt.month - i
            y = end_dt.year
            while m < 1:
                m += 12
                y -= 1
            ms = f"{y}-{m:02d}-01"
            last_day = calendar.monthrange(y, m)[1]
            me = f"{y}-{m:02d}-{last_day}"
            label = f"{y}-{m:02d}"
            trend_months.append(label)
            try:
                td = _run_period(bq, ms, me)
                trend_data[label] = td["metrics"]
                trend_data[label]["adjusted_revenue"] = td["adjusted_revenue"]
            except Exception:
                trend_data[label] = {}

        trends = {"months": trend_months}
        for key in ["cogs_pct", "labor_pct", "prime_cost_pct", "net_margin_pct", "avg_check", "adjusted_revenue"]:
            trends[key] = [round(trend_data.get(m, {}).get(key, 0), 1) for m in trend_months]

        # ── Generate key insights per metric ─────────────────────────────
        insights = []
        for s in scorecard:
            k, v, p, d, st = s["key"], s["value"], s["prior"], s["delta"], s["status"]
            trend_arr = trends.get(k, [])
            trend_dir = ""
            if len(trend_arr) >= 3:
                recent = trend_arr[-3:]
                if all(recent[i] <= recent[i+1] for i in range(len(recent)-1)):
                    trend_dir = "rising"
                elif all(recent[i] >= recent[i+1] for i in range(len(recent)-1)):
                    trend_dir = "falling"

            # Direction-aware delta description
            improving = (s["direction"] == "lower_is_better" and d < 0) or \
                        (s["direction"] == "higher_is_better" and d > 0)
            delta_word = "improved" if improving else "worsened" if d != 0 else "unchanged"

            insight_parts = []

            if k == "cogs_pct":
                if st == "good":
                    insight_parts.append(f"COGS is well-controlled at {v}%. Product purchasing and waste management are effective.")
                elif st == "watch":
                    insight_parts.append(f"COGS at {v}% is above the {KPI_BENCHMARKS[k]['good_max']}% target. Review supplier pricing, portion sizes, and waste logs.")
                else:
                    insight_parts.append(f"COGS at {v}% is critically high. Audit top-cost menu items, negotiate vendor contracts, and check for theft/waste.")
                if d != 0:
                    insight_parts.append(f"{delta_word.capitalize()} {abs(d):.1f}pp vs prior period.")
                if trend_dir == "rising":
                    insight_parts.append("Trending upward over 3 months — investigate rising ingredient costs.")

            elif k == "labor_pct":
                if st == "good":
                    insight_parts.append(f"True labor at {v}% shows efficient staffing relative to revenue.")
                elif st == "watch":
                    insight_parts.append(f"Labor at {v}% exceeds the {KPI_BENCHMARKS[k]['good_max']}% target. Evaluate shift scheduling and overtime hours.")
                else:
                    insight_parts.append(f"Labor at {v}% is critically high. Consider reducing slow-night staffing, cross-training staff, or reviewing management layers.")
                if trend_dir == "rising":
                    insight_parts.append("Labor cost trending up — may indicate overstaffing on slower nights.")
                elif trend_dir == "falling":
                    insight_parts.append("Labor cost trending down — scheduling optimizations are working.")

            elif k == "prime_cost_pct":
                if st == "good":
                    insight_parts.append(f"Prime cost at {v}% is below {KPI_BENCHMARKS[k]['good_max']}% — the two largest controllable costs are well-managed.")
                elif st == "watch":
                    insight_parts.append(f"Prime cost at {v}% is elevated. This combines COGS ({current['metrics']['cogs_pct']}%) + labor ({current['metrics']['labor_pct']}%). Focus on whichever is further from target.")
                else:
                    insight_parts.append(f"Prime cost at {v}% exceeds safe threshold. Both COGS and labor need immediate attention to protect margins.")

            elif k == "net_margin_pct":
                if st == "good":
                    insight_parts.append(f"Net margin at {v}% exceeds the {KPI_BENCHMARKS[k]['good_min']}% bar/lounge target. Strong overall profitability.")
                elif st == "watch":
                    insight_parts.append(f"Net margin at {v}% is below the {KPI_BENCHMARKS[k]['good_min']}% target but still positive. Look for expense reduction opportunities.")
                else:
                    if v < 0:
                        insight_parts.append(f"Operating at a {abs(v):.1f}% loss. Urgent: cut non-essential expenses and boost revenue through events/promotions.")
                    else:
                        insight_parts.append(f"Margin at {v}% is razor-thin. One bad month could push into losses. Prioritize cost control.")
                if d > 0:
                    insight_parts.append(f"Margin improved {d:.1f}pp vs prior period — positive momentum.")

            elif k == "marketing_pct":
                if st == "good":
                    insight_parts.append(f"Marketing spend at {v}% is efficient. Ensure ROI tracking on entertainment/DJ/promoter costs.")
                elif st == "watch":
                    insight_parts.append(f"Marketing at {v}% is above average. Evaluate which promotions drive the best return per dollar spent.")
                else:
                    insight_parts.append(f"Marketing/entertainment at {v}% is excessive. Audit DJ fees, promoter deals, and event costs for ROI.")

            elif k == "opex_pct":
                if st == "good":
                    insight_parts.append(f"Operating expenses at {v}% are lean. Overhead is well-controlled.")
                elif st == "watch":
                    insight_parts.append(f"OPEX at {v}% is above target. Review utilities, insurance, repairs, and subscriptions for savings.")
                else:
                    insight_parts.append(f"OPEX at {v}% is too high. Conduct a line-by-line expense audit — look for redundant services or inflated vendor costs.")

            elif k == "avg_check":
                if st == "good":
                    insight_parts.append(f"Average check at ${v:.2f} reflects strong upselling and premium positioning.")
                elif st == "watch":
                    insight_parts.append(f"Average check at ${v:.2f} is below the ${KPI_BENCHMARKS[k]['good_min']:.0f} target. Train staff on upselling cocktails, bottle service, and premium menu items.")
                else:
                    insight_parts.append(f"Average check at ${v:.2f} is low. Re-evaluate pricing strategy, menu engineering, and server incentives for upsells.")
                if d != 0:
                    insight_parts.append(f"{'Up' if d>0 else 'Down'} ${abs(d):.2f} from prior period.")

            elif k == "orders_per_day":
                if st == "good":
                    insight_parts.append(f"Averaging {v:.0f} orders/day shows healthy traffic and demand.")
                elif st == "watch":
                    insight_parts.append(f"At {v:.0f} orders/day, traffic is below target. Consider promotions, events, or happy hour specials to drive volume.")
                else:
                    insight_parts.append(f"Only {v:.0f} orders/day — significantly below the {KPI_BENCHMARKS[k]['good_min']:.0f} target. Need aggressive marketing or event programming.")

            elif k == "void_rate_pct":
                if st == "good":
                    insight_parts.append(f"Void rate at {v}% is within normal range. Order accuracy is strong.")
                elif st == "watch":
                    insight_parts.append(f"Void rate at {v}% warrants monitoring. Check for training gaps, POS entry errors, or customer order changes.")
                else:
                    insight_parts.append(f"Void rate at {v}% is abnormally high. Investigate: POS misuse, server errors, or potential fraud. Require manager approval for all voids.")

            elif k == "discount_rate_pct":
                if st == "good":
                    insight_parts.append(f"Discounts at {v}% are well-controlled. Comps and promos are within acceptable range.")
                elif st == "watch":
                    insight_parts.append(f"Discount rate at {v}% is elevated. Review manager comp patterns and promotional effectiveness.")
                else:
                    insight_parts.append(f"Discounts at {v}% are excessive. Set discount authorization limits and track comp reasons. Every 1% = real margin erosion.")

            elif k == "rev_per_labor_hour":
                if st == "good":
                    insight_parts.append(f"Revenue per labor hour at ${v:.0f} shows strong staffing efficiency.")
                elif st == "watch":
                    insight_parts.append(f"At ${v:.0f}/labor hr, consider optimizing schedules — reduce overlap during slow dayparts.")
                else:
                    insight_parts.append(f"At ${v:.0f}/labor hr, staff productivity is low. Cross-train team, stagger shifts, and cut slow-period labor.")

            elif k == "repeat_guest_pct":
                if st == "good":
                    insight_parts.append(f"Repeat rate at {v}% shows strong guest loyalty. Your regulars are engaged.")
                elif st == "watch":
                    insight_parts.append(f"Repeat rate at {v}% is below the {KPI_BENCHMARKS[k]['good_min']}% target. Invest in loyalty programs, email campaigns, and personalized outreach.")
                else:
                    insight_parts.append(f"Only {v}% of guests return — critical retention problem. Focus on first-visit experience, follow-up marketing, and VIP incentives.")

            elif k == "repeat_rev_pct":
                if st == "good":
                    insight_parts.append(f"Repeat guests generate {v}% of revenue — strong dependency on loyal base. Protect these relationships.")
                elif st == "watch":
                    insight_parts.append(f"Repeat revenue at {v}% — regulars contribute less than expected. Increase visit frequency with targeted offers and events.")
                else:
                    insight_parts.append(f"Only {v}% of revenue from repeat guests. Over-reliance on one-time visitors is risky — build retention programs.")

            elif k == "at_risk_pct":
                ar_count = current["guest_detail"]["at_risk_count"]
                if st == "good":
                    insight_parts.append(f"Only {ar_count} at-risk guests ({v}%) — churn risk is low.")
                elif st == "watch":
                    insight_parts.append(f"{ar_count} guests ({v}%) are at risk of churning (3+ visits but absent 45-90 days). Launch a win-back campaign.")
                else:
                    insight_parts.append(f"{ar_count} guests ({v}%) are at risk — this is a significant churn problem. Prioritize personalized outreach to high-value at-risk guests.")

            insights.append({
                "key": k,
                "label": s["label"],
                "status": st,
                "insight": " ".join(insight_parts),
            })

        # Serialize benchmarks for frontend legend
        bench_info = {}
        for key, b in KPI_BENCHMARKS.items():
            bench_info[key] = {
                "label": b["label"],
                "description": b["description"],
                "direction": b["direction"],
                "source": b["source"],
            }
            if b["direction"] == "lower_is_better":
                bench_info[key]["good_max"] = b["good_max"]
                bench_info[key]["watch_max"] = b["watch_max"]
            else:
                bench_info[key]["good_min"] = b["good_min"]
                bench_info[key]["watch_min"] = b["watch_min"]

        result = {
            "period": {"start": start_date, "end": end_date},
            "prior_period": {"start": prior_start, "end": prior_end},
            "adjusted_revenue": current["adjusted_revenue"],
            "net_profit": current["net_profit"],
            "order_count": current["order_count"],
            "operating_days": current["operating_days"],
            "has_bank_data": current["has_bank_data"],
            "scorecard": scorecard,
            "summary": {
                "total": len(scorecard),
                "good": good_count,
                "watch": watch_count,
                "critical": crit_count,
                "health": health,
            },
            "guest": current["guest_detail"],
            "trends": trends,
            "insights": insights,
            "benchmarks": bench_info,
        }
        _cache_set(ck, result)
        return jsonify(result)

    except Exception as e:
        logging.exception("kpi-benchmarks API error")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/budget", methods=["POST"])
def api_budget():
    """
    Budget tracker API — actual vs target spending for 15% margin goal.

    Request body:
    {"month": "2026-03"}   (defaults to current month if omitted)

    Returns budget category actuals vs targets, variance analysis,
    12-month trend, top vendors, insights, and path-to-15%.
    """
    data = request.get_json() or {}
    month_str = data.get("month")
    if not month_str:
        today = datetime.now()
        month_str = today.strftime("%Y-%m")

    try:
        year, mon = int(month_str[:4]), int(month_str[5:7])
        _, last_day = calendar.monthrange(year, mon)
        start_date = f"{month_str}-01"
        end_date = f"{month_str}-{last_day:02d}"
    except (ValueError, IndexError):
        return jsonify({"error": "Invalid month format. Use YYYY-MM."}), 400

    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        date_params = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ])

        # --- Q1: Revenue for selected month ---
        rev_q = f"""
        SELECT
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS total_tips,
            COALESCE(SUM(gratuity), 0) AS total_gratuity,
            COUNT(DISTINCT order_id) AS order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND (voided IS NULL OR voided = 'false')
        """
        rev_row = list(bq_client.query(rev_q, job_config=date_params).result())[0]
        net_sales = float(rev_row.net_sales or 0)
        total_tips = float(rev_row.total_tips or 0)
        total_gratuity = float(rev_row.total_gratuity or 0)
        grat_retained = round(total_gratuity * GRAT_RETAIN_PCT, 2)
        total_pass_through = round(total_tips + total_gratuity * GRAT_PASSTHROUGH_PCT, 2)
        adjusted_revenue = round(net_sales + grat_retained, 2)
        gross_revenue = round(net_sales + total_tips + total_gratuity, 2)

        # --- Q1b: Revenue breakdown by sales category (Toast POS) ---
        rev_cat_q = f"""
        SELECT
            COALESCE(SUM(CASE WHEN sales_category = 'Food' THEN CAST(net_price AS FLOAT64) ELSE 0 END), 0) AS food_rev,
            COALESCE(SUM(CASE WHEN sales_category = 'Liquor' THEN CAST(net_price AS FLOAT64) ELSE 0 END), 0) AS liquor_rev,
            COALESCE(SUM(CAST(net_price AS FLOAT64)), 0) AS item_total
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND (voided IS NULL OR voided = 'false')
        """
        rev_cat_row = list(bq_client.query(rev_cat_q, job_config=date_params).result())[0]
        food_rev = round(float(rev_cat_row.food_rev or 0), 2)
        liquor_rev = round(float(rev_cat_row.liquor_rev or 0), 2)

        # --- Q1c: Hookah revenue from bank deposits (not Toast POS) ---
        hookah_bank_q = f"""
        SELECT COALESCE(SUM(amount), 0) AS hookah_rev
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND LOWER(category) LIKE '%hookah sales%'
            AND amount > 0
        """
        hookah_row = list(bq_client.query(hookah_bank_q, job_config=date_params).result())[0]
        hookah_rev = round(float(hookah_row.hookah_rev or 0), 2)
        other_rev = round(max(net_sales - food_rev - liquor_rev, 0), 2)
        # Hookah is additive (bank deposits, not in Toast net_sales)
        gross_revenue = round(gross_revenue + hookah_rev, 2)

        # --- Q2: Expenses by category for selected month ---
        exp_q = f"""
        SELECT
            category,
            ROUND(SUM(abs_amount), 2) AS total
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
        GROUP BY category
        ORDER BY total DESC
        """
        exp_rows = list(bq_client.query(exp_q, job_config=date_params).result())
        expenses_by_cat: Dict[str, float] = {}
        for row in exp_rows:
            expenses_by_cat[row.category] = float(row.total or 0)
        total_expenses = sum(
            v for k, v in expenses_by_cat.items()
            if "revenue" not in k.lower()
        )

        def sum_matching(exps: Dict[str, float], keywords: List[str]) -> float:
            total = 0.0
            for cat, amt in exps.items():
                cat_lower = cat.lower()
                if any(kw.lower() in cat_lower for kw in keywords):
                    total += amt
            return total

        # --- Q3: Top vendors for selected month ---
        vendor_q = f"""
        SELECT
            vendor_normalized,
            category,
            ROUND(SUM(abs_amount), 2) AS total,
            COUNT(*) AS txns
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_type = 'debit'
            AND transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
        GROUP BY vendor_normalized, category
        ORDER BY total DESC
        LIMIT 50
        """
        vendor_rows = list(bq_client.query(vendor_q, job_config=date_params).result())
        top_vendors_raw = [
            {
                "vendor": row.vendor_normalized or "Unknown",
                "category": row.category or "Uncategorized",
                "amount": float(row.total or 0),
                "txns": int(row.txns or 0),
            }
            for row in vendor_rows
        ]

        # --- Q3b: Individual transactions for subcategory drill-in ---
        txn_q = f"""
        SELECT
            transaction_date,
            description,
            abs_amount,
            category,
            vendor_normalized
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_type = 'debit'
            AND transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
        ORDER BY abs_amount DESC
        LIMIT 500
        """
        txn_rows = list(bq_client.query(txn_q, job_config=date_params).result())
        all_txns = [
            {
                "date": str(row.transaction_date),
                "description": row.description or "",
                "amount": float(row.abs_amount or 0),
                "vendor": row.vendor_normalized or "",
                "category": row.category or "Uncategorized",
            }
            for row in txn_rows
        ]

        # --- Q4: Monthly history (last 12 months) ---
        hist_end = end_date
        hist_start_year = year - 1
        hist_start_month = mon + 1
        if hist_start_month > 12:
            hist_start_month = 1
            hist_start_year += 1
        hist_start = f"{hist_start_year}-{hist_start_month:02d}-01"

        rev_hist_q = f"""
        SELECT
            LEFT(CAST(processing_date AS STRING), 7) AS month,
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS tips,
            COALESCE(SUM(gratuity), 0) AS grat
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @hist_start) AND PARSE_DATE('%Y-%m-%d', @hist_end)
            AND (voided IS NULL OR voided = 'false')
        GROUP BY month ORDER BY month
        """
        exp_hist_q = f"""
        SELECT
            LEFT(CAST(transaction_date AS STRING), 7) AS month,
            category,
            ROUND(SUM(abs_amount), 2) AS total
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @hist_start) AND PARSE_DATE('%Y-%m-%d', @hist_end)
            AND transaction_type = 'debit'
        GROUP BY month, category
        ORDER BY month
        """
        hist_params = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("hist_start", "STRING", hist_start),
            bigquery.ScalarQueryParameter("hist_end", "STRING", hist_end),
        ])
        rev_hist_rows = list(bq_client.query(rev_hist_q, job_config=hist_params).result())
        exp_hist_rows = list(bq_client.query(exp_hist_q, job_config=hist_params).result())

        # Build history by month
        rev_by_month: Dict[str, Dict] = {}
        for row in rev_hist_rows:
            m = row.month
            ns = float(row.net_sales or 0)
            tips = float(row.tips or 0)
            grat = float(row.grat or 0)
            gr = round(grat * GRAT_RETAIN_PCT, 2)
            pt = round(tips + grat * GRAT_PASSTHROUGH_PCT, 2)
            rev_by_month[m] = {"adjusted_revenue": round(ns + gr, 2), "pass_through": pt}

        exp_by_month: Dict[str, Dict[str, float]] = {}
        for row in exp_hist_rows:
            m = row.month
            if m not in exp_by_month:
                exp_by_month[m] = {}
            exp_by_month[m][row.category] = float(row.total or 0)

        # Assemble sorted month list
        all_months = sorted(set(list(rev_by_month.keys()) + list(exp_by_month.keys())))
        history = {
            "months": all_months,
            "revenue": [],
            "cogs_pct": [],
            "labor_pct": [],
            "marketing_pct": [],
            "opex_pct": [],
            "margin_pct": [],
        }
        for m in all_months:
            rv = rev_by_month.get(m, {"adjusted_revenue": 0, "pass_through": 0})
            ex = exp_by_month.get(m, {})
            adj_rev = rv["adjusted_revenue"]
            pt = rv["pass_through"]
            history["revenue"].append(round(adj_rev))
            rev_denom = adj_rev if adj_rev > 0 else 1
            m_cogs = sum_matching(ex, BUDGET_TARGETS["cogs"]["keywords"])
            m_labor_gross = sum_matching(ex, BUDGET_TARGETS["labor"]["keywords"])
            m_labor = max(round(m_labor_gross - pt, 2), 0)
            m_mktg = sum_matching(ex, BUDGET_TARGETS["marketing"]["keywords"])
            m_opex = sum_matching(ex, BUDGET_TARGETS["opex"]["keywords"])
            m_total_exp = sum(v for k, v in ex.items() if "revenue" not in k.lower())
            m_adj_exp = max(round(m_total_exp - pt, 2), m_total_exp * 0.5)
            m_profit = round(adj_rev - m_adj_exp, 2)
            history["cogs_pct"].append(round(m_cogs / rev_denom * 100, 1))
            history["labor_pct"].append(round(m_labor / rev_denom * 100, 1))
            history["marketing_pct"].append(round(m_mktg / rev_denom * 100, 1))
            history["opex_pct"].append(round(m_opex / rev_denom * 100, 1))
            history["margin_pct"].append(round(m_profit / rev_denom * 100, 1))

        # --- Build budget response for selected month ---
        rev_denom = adjusted_revenue if adjusted_revenue > 0 else 1
        budget_resp = {}
        total_target_pct = 0.0
        total_actual_pct = 0.0

        for key, cfg in BUDGET_TARGETS.items():
            actual_raw = sum_matching(expenses_by_cat, cfg["keywords"])
            # For labor, subtract pass-through
            if key == "labor":
                actual_raw = max(round(actual_raw - total_pass_through, 2), 0)
            actual_pct = round(actual_raw / rev_denom * 100, 1)
            target_amt = round(adjusted_revenue * cfg["target_pct"] / 100, 2)
            variance = round(actual_raw - target_amt, 2)
            variance_pct = round(actual_pct - cfg["target_pct"], 1)

            if actual_pct <= cfg["target_pct"]:
                status = "under_budget"
            elif actual_pct <= cfg["target_pct"] + 2.0:
                status = "on_track"
            elif actual_pct <= cfg["max_pct"]:
                status = "watch"
            else:
                status = "over_budget"

            # Top vendors for this category
            cat_vendors = [
                v for v in top_vendors_raw
                if any(kw.lower() in v["category"].lower() for kw in cfg["keywords"])
            ][:5]

            budget_resp[key] = {
                "label": cfg["label"],
                "target_pct": cfg["target_pct"],
                "max_pct": cfg["max_pct"],
                "actual": round(actual_raw, 2),
                "actual_pct": actual_pct,
                "target_amount": target_amt,
                "variance": variance,
                "variance_pct": variance_pct,
                "status": status,
                "top_vendors": cat_vendors,
            }
            total_target_pct += cfg["target_pct"]
            total_actual_pct += actual_pct

        # --- Subcategory actuals ---
        subcategories: Dict[str, Any] = {}
        for sub_key, sub_cfg in BUDGET_SUBCATEGORIES.items():
            parent_key = sub_cfg["parent"]
            parent_target_pct = BUDGET_TARGETS[parent_key]["target_pct"]
            is_informational = sub_cfg.get("informational", False)

            sub_actual = sum_matching(expenses_by_cat, sub_cfg["keywords"])
            sub_actual_pct = round(sub_actual / rev_denom * 100, 2)

            if is_informational:
                sub_target_pct = 0.0
                sub_target_amt = 0.0
                sub_status = "informational"
                sub_variance = 0.0
                sub_variance_pct = 0.0
            elif "fixed_target" in sub_cfg:
                sub_target_amt = sub_cfg["fixed_target"]
                sub_target_pct = round(sub_target_amt / rev_denom * 100, 2)
            else:
                sub_target_pct = round(parent_target_pct * sub_cfg["share_pct"] / 100, 2)
                sub_target_amt = round(adjusted_revenue * sub_target_pct / 100, 2)
                sub_variance = round(sub_actual - sub_target_amt, 2)
                sub_variance_pct = round(sub_actual_pct - sub_target_pct, 2)
                if sub_target_amt > 0:
                    ratio = sub_actual / sub_target_amt
                else:
                    ratio = 0.0 if sub_actual == 0 else 2.0
                if ratio <= 1.0:
                    sub_status = "under_budget"
                elif ratio <= 1.15:
                    sub_status = "on_track"
                elif ratio <= 1.30:
                    sub_status = "watch"
                else:
                    sub_status = "over_budget"

            # Top 5 vendors for this subcategory
            sub_vendors = [
                v for v in top_vendors_raw
                if any(kw.lower() in v["category"].lower() for kw in sub_cfg["keywords"])
            ][:5]

            # Top 5 individual transactions for this subcategory
            sub_txns = [
                t for t in all_txns
                if any(kw.lower() in t["category"].lower() for kw in sub_cfg["keywords"])
            ]
            sub_txn_count = len(sub_txns)

            subcategories[sub_key] = {
                "label": sub_cfg["label"],
                "parent": parent_key,
                "parent_label": BUDGET_TARGETS[parent_key]["label"],
                "share_pct": sub_cfg["share_pct"],
                "target_pct": sub_target_pct,
                "target_amount": sub_target_amt,
                "actual": round(sub_actual, 2),
                "actual_pct": sub_actual_pct,
                "variance": sub_variance,
                "variance_pct": sub_variance_pct,
                "status": sub_status,
                "informational": is_informational,
                "insight": sub_cfg["insight"],
                "top_vendors": sub_vendors,
                "top_transactions": sub_txns[:5],
                "transaction_count": sub_txn_count,
            }

        # --- Unbudgeted sections (G&A, Facility) ---
        unbudgeted: Dict[str, Any] = {}
        for ub_key, ub_cfg in UNBUDGETED_SECTIONS.items():
            ub_actual = sum_matching(expenses_by_cat, ub_cfg["keywords"])
            ub_vendors = [
                v for v in top_vendors_raw
                if any(kw.lower() in v["category"].lower() for kw in ub_cfg["keywords"])
            ][:5]
            unbudgeted[ub_key] = {
                "label": ub_cfg["label"],
                "actual": round(ub_actual, 2),
                "actual_pct": round(ub_actual / rev_denom * 100, 2),
                "note": ub_cfg["note"],
                "top_vendors": ub_vendors,
            }

        # --- Tag each vendor with its parent category + subcategory budget status ---
        top_vendors = []
        for v in top_vendors_raw:
            v_cat_lower = v["category"].lower()
            matched_key = None
            for key, cfg in BUDGET_TARGETS.items():
                if any(kw.lower() in v_cat_lower for kw in cfg["keywords"]):
                    matched_key = key
                    break
            # Find matching subcategory
            matched_sub = None
            for sk, sc in subcategories.items():
                sub_kws = BUDGET_SUBCATEGORIES[sk]["keywords"]
                if any(kw.lower() in v_cat_lower for kw in sub_kws):
                    matched_sub = sk
                    break
            if matched_key:
                b = budget_resp[matched_key]
                v["budget_group"] = b["label"]
                v["budget_status"] = b["status"]
                v["budget_variance"] = b["variance"]
                v["budget_variance_pct"] = b["variance_pct"]
                v["actionable"] = b["status"] in ("over_budget", "watch")
            else:
                v["budget_group"] = "Other"
                v["budget_status"] = "unknown"
                v["budget_variance"] = 0
                v["budget_variance_pct"] = 0
                v["actionable"] = False
            if matched_sub:
                v["subcategory"] = subcategories[matched_sub]["label"]
                v["subcategory_status"] = subcategories[matched_sub]["status"]
            else:
                v["subcategory"] = ""
                v["subcategory_status"] = ""
            top_vendors.append(v)

        # Sort vendors: over_budget first, then watch, on_track, under_budget, by amount desc
        status_priority = {"over_budget": 0, "watch": 1, "on_track": 2, "under_budget": 3, "unknown": 4, "": 5}
        top_vendors.sort(key=lambda x: (
            status_priority.get(x.get("subcategory_status") or x.get("budget_status", "unknown"), 5),
            -x["amount"],
        ))

        # Totals
        adjusted_expenses = max(round(total_expenses - total_pass_through, 2), 0)
        net_profit = round(adjusted_revenue - adjusted_expenses, 2)
        margin_pct = round(net_profit / rev_denom * 100, 1)
        target_margin = 15.0
        if margin_pct >= target_margin:
            margin_status = "good"
        elif margin_pct >= 5.0:
            margin_status = "watch"
        else:
            margin_status = "critical"

        totals = {
            "budget_total_pct": total_target_pct,
            "actual_total_pct": total_actual_pct,
            "target_expenses": round(adjusted_revenue * total_target_pct / 100, 2),
            "actual_expenses": round(adjusted_expenses, 2),
            "net_profit": net_profit,
            "margin_pct": margin_pct,
            "target_margin": target_margin,
            "margin_status": margin_status,
        }

        # --- Insights ---
        insights = []
        for key, cfg in BUDGET_TARGETS.items():
            b = budget_resp[key]
            if b["status"] == "over_budget":
                severity = "critical"
            elif b["status"] == "watch":
                severity = "warning"
            elif b["status"] == "on_track":
                severity = "info"
            else:
                severity = "good"

            # Build vendor detail with amounts
            vendor_details = []
            for v in b["top_vendors"][:3]:
                vendor_details.append(f"{v['vendor']} (${v['amount']:,.0f})")
            vendor_str = ", ".join(vendor_details)
            delta_str = f"+{b['variance_pct']}pp" if b["variance_pct"] > 0 else f"{b['variance_pct']}pp"

            if b["status"] in ("over_budget", "watch"):
                text = (
                    f"<strong>{cfg['label']}</strong> at {b['actual_pct']}% is {delta_str} vs "
                    f"{cfg['target_pct']}% target — <strong>${abs(b['variance']):,.0f} over budget.</strong> "
                    f"Top spend: {vendor_str}. "
                    f"<em>Action: {cfg['insight']}.</em>"
                )
            elif b["status"] == "on_track":
                headroom = round(b["target_amount"] - b["actual"], 2)
                text = (
                    f"<strong>{cfg['label']}</strong> at {b['actual_pct']}% — within "
                    f"{abs(b['variance_pct'])}pp of {cfg['target_pct']}% target. "
                    f"Only ${headroom:,.0f} of headroom left. "
                    f"Top spend: {vendor_str}. <em>Monitor closely — one large invoice could push over.</em>"
                )
            else:
                savings = round(b["target_amount"] - b["actual"], 2)
                text = (
                    f"<strong>{cfg['label']}</strong> at {b['actual_pct']}% — "
                    f"${savings:,.0f} under the {cfg['target_pct']}% target. "
                    f"Well managed this month. No action needed."
                )

            insights.append({
                "category": key,
                "severity": severity,
                "text": text,
            })

        # Overall margin insight
        if margin_pct < 0:
            insights.insert(0, {
                "category": "overall",
                "severity": "critical",
                "text": (
                    f"Operating at {margin_pct}% margin — a ${abs(net_profit):,.0f}/mo loss. "
                    f"Need to cut ${abs(net_profit) + round(adjusted_revenue * target_margin / 100):,.0f}/mo "
                    f"in expenses to reach {target_margin}% margin."
                ),
            })
        elif margin_pct < target_margin:
            gap = round(adjusted_revenue * target_margin / 100 - net_profit, 2)
            insights.insert(0, {
                "category": "overall",
                "severity": "warning",
                "text": (
                    f"Margin at {margin_pct}% — ${gap:,.0f}/mo short of "
                    f"{target_margin}% target. Focus on the highest-variance categories."
                ),
            })
        else:
            insights.insert(0, {
                "category": "overall",
                "severity": "good",
                "text": f"Margin at {margin_pct}% — above the {target_margin}% target. Keep it up!",
            })

        # Subcategory-level insights for over_budget / watch items
        for sub_key, sub in subcategories.items():
            if sub["informational"] or sub["status"] not in ("over_budget", "watch"):
                continue
            sev = "critical" if sub["status"] == "over_budget" else "warning"
            sub_vendor_strs = [f"{v['vendor']} (${v['amount']:,.0f})" for v in sub["top_vendors"][:3]]
            sub_vendor_str = ", ".join(sub_vendor_strs) if sub_vendor_strs else "no vendor detail"
            insights.append({
                "category": f"sub_{sub_key}",
                "severity": sev,
                "text": (
                    f"<strong>{sub['label']}</strong> ({sub['parent_label']}) at "
                    f"{sub['actual_pct']:.1f}% of revenue — "
                    f"${abs(sub['variance']):,.0f} {'over' if sub['variance'] > 0 else 'under'} "
                    f"the {sub['target_pct']:.1f}% sub-target. "
                    f"Top spend: {sub_vendor_str}. "
                    f"<em>Action: {sub['insight']}.</em>"
                ),
            })

        # Sort: critical first
        sev_order = {"critical": 0, "warning": 1, "info": 2, "good": 3}
        insights.sort(key=lambda x: sev_order.get(x["severity"], 9))

        # --- Path to 15% ---
        gap_pct = round(target_margin - margin_pct, 1)
        gap_dollars = round(adjusted_revenue * gap_pct / 100, 2) if gap_pct > 0 else 0
        recommendations = []
        for key, cfg in BUDGET_TARGETS.items():
            b = budget_resp[key]
            if b["actual_pct"] > cfg["target_pct"]:
                savings = round(b["actual"] - b["target_amount"], 2)
                recommendations.append({
                    "category": key,
                    "label": cfg["label"],
                    "current_pct": b["actual_pct"],
                    "target_pct": cfg["target_pct"],
                    "savings": savings,
                    "insight": cfg["insight"],
                })
        recommendations.sort(key=lambda x: x["savings"], reverse=True)
        for i, r in enumerate(recommendations):
            r["priority"] = i + 1

        path_to_target = {
            "current_margin": margin_pct,
            "target_margin": target_margin,
            "gap_pct": gap_pct,
            "gap_dollars": gap_dollars,
            "total_potential_savings": sum(r["savings"] for r in recommendations),
            "recommendations": recommendations,
        }

        return jsonify({
            "month": month_str,
            "revenue": {
                "gross_revenue": gross_revenue,
                "net_sales": round(net_sales, 2),
                "adjusted_revenue": adjusted_revenue,
                "food": food_rev,
                "liquor": liquor_rev,
                "hookah": hookah_rev,
                "other": other_rev,
                "total_tips": round(total_tips, 2),
                "total_gratuity": round(total_gratuity, 2),
                "gratuity_retained": grat_retained,
                "pass_through": total_pass_through,
                "order_count": int(rev_row.order_count or 0),
            },
            "budget": budget_resp,
            "subcategories": subcategories,
            "unbudgeted": unbudgeted,
            "totals": totals,
            "monthly_history": history,
            "top_vendors": top_vendors,
            "insights": insights,
            "path_to_target": path_to_target,
        })

    except Exception as e:
        logging.exception("budget API error")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/budget-drilldown", methods=["POST"])
def api_budget_drilldown():
    """
    Budget drilldown API — all individual transactions for a subcategory in a month.

    Request body:
    {"month": "2026-03", "subcategory": "food_cogs"}

    Returns all debit transactions matching the subcategory's category keywords.
    """
    data = request.get_json() or {}
    month_str = data.get("month")
    sub_key = data.get("subcategory", "")

    if not month_str:
        return jsonify({"error": "month is required (YYYY-MM)"}), 400

    # Look up subcategory config
    sub_cfg = BUDGET_SUBCATEGORIES.get(sub_key)
    if not sub_cfg:
        sub_cfg = UNBUDGETED_SECTIONS.get(sub_key)
    if not sub_cfg:
        return jsonify({"error": f"Unknown subcategory: {sub_key}"}), 400

    try:
        year, mon = int(month_str[:4]), int(month_str[5:7])
        _, last_day = calendar.monthrange(year, mon)
        start_date = f"{month_str}-01"
        end_date = f"{month_str}-{last_day:02d}"
    except (ValueError, IndexError):
        return jsonify({"error": "Invalid month format. Use YYYY-MM."}), 400

    keywords = sub_cfg["keywords"]
    kw_clauses = [f"LOWER(category) LIKE '%{kw.lower()}%'" for kw in keywords]
    kw_where = "(" + " OR ".join(kw_clauses) + ")"

    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        q = f"""
        SELECT
            transaction_date,
            description,
            abs_amount,
            category,
            vendor_normalized,
            category_source
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_type = 'debit'
            AND transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND {kw_where}
        ORDER BY abs_amount DESC
        """
        date_params = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ])
        rows = list(bq_client.query(q, job_config=date_params).result())
        transactions = [
            {
                "date": str(r.transaction_date),
                "description": r.description or "",
                "amount": float(r.abs_amount or 0),
                "vendor": r.vendor_normalized or "",
                "category": r.category or "Uncategorized",
            }
            for r in rows
        ]

        return jsonify({
            "month": month_str,
            "subcategory": sub_key,
            "label": sub_cfg["label"],
            "total": round(sum(t["amount"] for t in transactions), 2),
            "count": len(transactions),
            "transactions": transactions,
        })

    except Exception as e:
        logging.exception("budget-drilldown error")
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Event ROI API
# ---------------------------------------------------------------------------
@bp.route("/api/event-roi", methods=["POST"])
def api_event_roi():
    """
    Event ROI analysis — recurring weekly events at LOV3.

    Request body:
    {"start_date": "2025-09-01", "end_date": "2026-02-28"}

    Returns per-event revenue (by DOW), direct costs (vendor-mapped),
    shared costs (revenue-proportional), ROI/margin metrics,
    monthly trend, unattributed vendors, and insights.
    """
    data = request.get_json()
    if not data or "start_date" not in data or "end_date" not in data:
        return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400

    start_date = data["start_date"]
    end_date = data["end_date"]

    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        # Build business-day SQL for PaymentDetails (paid_date is STRING)
        bd = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")

        # --- Q1: Revenue by DOW by month ---
        q_revenue = f"""
        SELECT
            LEFT(CAST({bd} AS STRING), 7) AS month,
            EXTRACT(DAYOFWEEK FROM {bd}) AS dow_num,
            FORMAT_DATE('%A', {bd}) AS dow_name,
            COUNT(*) AS txn_count,
            COUNT(DISTINCT {bd}) AS num_nights,
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS tips,
            COALESCE(SUM(gratuity), 0) AS gratuity
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE {bd} BETWEEN @start_date AND @end_date
            AND status IN ('CAPTURED', 'AUTHORIZED', 'CAPTURE_IN_PROGRESS')
            AND paid_date IS NOT NULL AND paid_date != ''
        GROUP BY month, dow_num, dow_name
        ORDER BY month, dow_num
        """

        # Build category LIKE clauses
        direct_likes = " OR ".join(
            f"LOWER(category) LIKE '%{cat}%'" for cat in DIRECT_EVENT_CATEGORIES
        )
        shared_likes = " OR ".join(
            f"LOWER(category) LIKE '%{cat}%'" for cat in SHARED_EVENT_CATEGORIES
        )

        # --- Q2: Direct event expenses by vendor by month ---
        q_direct = f"""
        SELECT
            LEFT(CAST(transaction_date AS STRING), 7) AS month,
            COALESCE(vendor_normalized, description) AS vendor,
            category,
            ROUND(SUM(abs_amount), 2) AS total_amount,
            COUNT(*) AS txn_count
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
            AND ({direct_likes})
        GROUP BY month, vendor, category
        ORDER BY month, total_amount DESC
        """

        # --- Q3: Shared event expenses by month ---
        q_shared = f"""
        SELECT
            LEFT(CAST(transaction_date AS STRING), 7) AS month,
            category,
            ROUND(SUM(abs_amount), 2) AS total_amount,
            COUNT(*) AS txn_count
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
            AND ({shared_likes})
        GROUP BY month, category
        ORDER BY month
        """

        # --- Q4: All direct-category vendors (for unattributed list) ---
        q_all_vendors = f"""
        SELECT
            COALESCE(vendor_normalized, description) AS vendor,
            category,
            ROUND(SUM(abs_amount), 2) AS total_amount,
            COUNT(*) AS txn_count
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
            AND ({direct_likes})
        GROUP BY vendor, category
        ORDER BY total_amount DESC
        """

        # --- Q5: Payroll labor by month (includes tip/grat pass-through) ---
        # Excludes security & contract labor (handled separately in Q6)
        ops_likes = " AND ".join(
            f"LOWER(category) NOT LIKE '%{cat}%'" for cat in OPERATIONAL_LABOR_CATEGORIES
        )
        q_labor = f"""
        SELECT
            LEFT(CAST(transaction_date AS STRING), 7) AS month,
            ROUND(SUM(abs_amount), 2) AS total_labor
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
            AND (LOWER(category) LIKE '%labor%' OR LOWER(category) LIKE '%payroll%')
            AND {ops_likes}
        GROUP BY month
        ORDER BY month
        """

        # --- Q6: Operational labor by month (security + contract staffing) ---
        # No tip/grat pass-through — allocated directly by DOW%
        ops_cat_likes = " OR ".join(
            f"LOWER(category) LIKE '%{cat}%'" for cat in OPERATIONAL_LABOR_CATEGORIES
        )
        q_ops_labor = f"""
        SELECT
            LEFT(CAST(transaction_date AS STRING), 7) AS month,
            ROUND(SUM(abs_amount), 2) AS total_amount,
            category
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND transaction_type = 'debit'
            AND ({ops_cat_likes})
        GROUP BY month, category
        ORDER BY month
        """

        date_params = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ])
        rev_rows = list(bq_client.query(q_revenue, job_config=date_params).result())
        direct_rows = list(bq_client.query(q_direct, job_config=date_params).result())
        shared_rows = list(bq_client.query(q_shared, job_config=date_params).result())
        all_vendor_rows = list(bq_client.query(q_all_vendors, job_config=date_params).result())
        labor_rows = list(bq_client.query(q_labor, job_config=date_params).result())
        ops_labor_rows = list(bq_client.query(q_ops_labor, job_config=date_params).result())

        # --- Step 1: Build revenue by event by month ---
        dow_to_event = {cfg["dow_num"]: key for key, cfg in RECURRING_EVENTS.items()}

        rev_by_event_month: Dict[str, Dict[str, Dict]] = {}
        for row in rev_rows:
            dow = int(row.dow_num)
            event_key = dow_to_event.get(dow)
            if not event_key:
                continue
            month = row.month
            ns = float(row.net_sales or 0)
            grat = float(row.gratuity or 0)
            tips = float(row.tips or 0)
            gr = round(grat * GRAT_RETAIN_PCT, 2)
            if event_key not in rev_by_event_month:
                rev_by_event_month[event_key] = {}
            rev_by_event_month[event_key][month] = {
                "net_sales": round(ns, 2),
                "tips": round(tips, 2),
                "gratuity": round(grat, 2),
                "grat_retained": gr,
                "adjusted_revenue": round(ns + gr, 2),
                "num_nights": int(row.num_nights or 0),
                "txn_count": int(row.txn_count or 0),
            }

        # --- Step 2: Attribute direct costs by vendor ---
        def _match_vendor(vendor: str) -> List[str]:
            v_lower = (vendor or "").lower()
            for keyword, ev_key in EVENT_VENDOR_MAP.items():
                if keyword.lower() in v_lower:
                    if isinstance(ev_key, list):
                        return ev_key
                    return [ev_key]
            return []

        direct_by_event_month: Dict[str, Dict[str, float]] = {}
        direct_vendors_by_event: Dict[str, Dict[str, Dict]] = {}
        unattributed_accum: Dict[str, Dict] = {}

        for row in direct_rows:
            vendor = row.vendor or "Unknown"
            month = row.month
            amount = float(row.total_amount or 0)
            txns = int(row.txn_count or 0)
            matched_events = _match_vendor(vendor)

            if matched_events:
                split_amt = round(amount / len(matched_events), 2)
                for ek in matched_events:
                    if ek not in direct_by_event_month:
                        direct_by_event_month[ek] = {}
                    direct_by_event_month[ek][month] = (
                        direct_by_event_month[ek].get(month, 0) + split_amt
                    )
                    if ek not in direct_vendors_by_event:
                        direct_vendors_by_event[ek] = {}
                    if vendor not in direct_vendors_by_event[ek]:
                        direct_vendors_by_event[ek][vendor] = {"amount": 0, "txns": 0, "category": row.category}
                    direct_vendors_by_event[ek][vendor]["amount"] += split_amt
                    direct_vendors_by_event[ek][vendor]["txns"] += txns
            else:
                if vendor not in unattributed_accum:
                    unattributed_accum[vendor] = {"amount": 0, "txns": 0, "category": row.category or ""}
                unattributed_accum[vendor]["amount"] += amount
                unattributed_accum[vendor]["txns"] += txns

        unattributed_vendors = []
        for row in all_vendor_rows:
            vendor = row.vendor or "Unknown"
            if not _match_vendor(vendor):
                unattributed_vendors.append({
                    "vendor": vendor,
                    "category": row.category or "",
                    "amount": round(float(row.total_amount or 0), 2),
                    "txns": int(row.txn_count or 0),
                })

        total_unattributed = sum(v["amount"] for v in unattributed_vendors)

        # --- Step 3: Compute shared costs allocation by month ---
        shared_by_month: Dict[str, float] = {}
        for row in shared_rows:
            m = row.month
            shared_by_month[m] = shared_by_month.get(m, 0) + float(row.total_amount or 0)

        all_months = sorted(set(
            m for ev in rev_by_event_month.values() for m in ev
        ) | set(shared_by_month.keys()) | set(
            m for ev in direct_by_event_month.values() for m in ev
        ))

        shared_by_event_month: Dict[str, Dict[str, float]] = {}
        for month in all_months:
            month_total_rev = sum(
                rev_by_event_month.get(ek, {}).get(month, {}).get("adjusted_revenue", 0)
                for ek in RECURRING_EVENTS
            )
            month_shared = shared_by_month.get(month, 0)
            for ek in RECURRING_EVENTS:
                ev_rev = rev_by_event_month.get(ek, {}).get(month, {}).get("adjusted_revenue", 0)
                share_pct = ev_rev / month_total_rev if month_total_rev > 0 else 0
                allocation = round(month_shared * share_pct, 2)
                if ek not in shared_by_event_month:
                    shared_by_event_month[ek] = {}
                shared_by_event_month[ek][month] = allocation

        # --- Step 3b: Allocate TRUE labor by DOW ---
        labor_by_month: Dict[str, float] = {}
        for row in labor_rows:
            labor_by_month[row.month] = float(row.total_labor or 0)

        passthrough_by_month: Dict[str, float] = {}
        for ek in RECURRING_EVENTS:
            for month, mdata in rev_by_event_month.get(ek, {}).items():
                tips = mdata.get("tips", 0)
                grat = mdata.get("gratuity", 0)
                grat_passthrough = round(grat * (1 - GRAT_RETAIN_PCT), 2)
                passthrough_by_month[month] = passthrough_by_month.get(month, 0) + tips + grat_passthrough

        all_months = sorted(set(all_months) | set(labor_by_month.keys()))

        labor_by_event_month: Dict[str, Dict[str, float]] = {}
        total_gross_labor = 0.0
        total_passthrough = 0.0
        total_true_labor = 0.0
        total_fixed_labor = 0.0
        total_variable_labor = 0.0

        for month in all_months:
            gross = labor_by_month.get(month, 0)
            total_gross_labor += gross
            pt = passthrough_by_month.get(month, 0)
            total_passthrough += pt
            true_labor = max(gross - pt, 0)
            total_true_labor += true_labor
            fixed = FIXED_LABOR_MONTHLY
            total_fixed_labor += fixed
            variable = max(true_labor - fixed, 0)
            total_variable_labor += variable
            for ek, pct in LABOR_DOW_PCT.items():
                alloc = round(variable * pct, 2)
                if ek not in labor_by_event_month:
                    labor_by_event_month[ek] = {}
                labor_by_event_month[ek][month] = alloc

        # --- Step 3c: Allocate operational labor (security + contract staffing) by DOW ---
        ops_labor_by_month: Dict[str, float] = {}
        for row in ops_labor_rows:
            m = row.month
            ops_labor_by_month[m] = ops_labor_by_month.get(m, 0) + float(row.total_amount or 0)

        all_months = sorted(set(all_months) | set(ops_labor_by_month.keys()))

        ops_by_event_month: Dict[str, Dict[str, float]] = {}
        total_ops_labor = 0.0

        for month in all_months:
            ops_total = ops_labor_by_month.get(month, 0)
            total_ops_labor += ops_total
            for ek, pct in LABOR_DOW_PCT.items():
                alloc = round(ops_total * pct, 2)
                if ek not in ops_by_event_month:
                    ops_by_event_month[ek] = {}
                ops_by_event_month[ek][month] = ops_by_event_month[ek].get(month, 0) + alloc

        # --- Step 4: Assemble per-event summary ---
        total_event_revenue = 0.0
        total_direct_costs = 0.0
        total_shared_costs = 0.0
        total_labor_costs = 0.0
        total_ops_costs = 0.0

        events_list = []
        for ek, cfg in RECURRING_EVENTS.items():
            ev_rev_months = rev_by_event_month.get(ek, {})
            ev_direct_months = direct_by_event_month.get(ek, {})
            ev_shared_months = shared_by_event_month.get(ek, {})
            ev_labor_months = labor_by_event_month.get(ek, {})
            ev_ops_months = ops_by_event_month.get(ek, {})

            total_rev = sum(m.get("adjusted_revenue", 0) for m in ev_rev_months.values())
            total_ns = sum(m.get("net_sales", 0) for m in ev_rev_months.values())
            total_grat = sum(m.get("gratuity", 0) for m in ev_rev_months.values())
            total_gr = sum(m.get("grat_retained", 0) for m in ev_rev_months.values())
            total_nights = sum(m.get("num_nights", 0) for m in ev_rev_months.values())
            total_txns = sum(m.get("txn_count", 0) for m in ev_rev_months.values())

            ev_direct = sum(ev_direct_months.values())
            ev_shared = sum(ev_shared_months.values())
            ev_labor = sum(ev_labor_months.values())
            ev_ops = sum(ev_ops_months.values())
            ev_total_costs = round(ev_direct + ev_shared + ev_labor + ev_ops, 2)
            ev_net = round(total_rev - ev_total_costs, 2)
            ev_roi = round(ev_net / ev_total_costs * 100, 1) if ev_total_costs > 0 else 0
            ev_margin = round(ev_net / total_rev * 100, 1) if total_rev > 0 else 0
            avg_nightly = round(total_rev / total_nights, 2) if total_nights > 0 else 0

            total_event_revenue += total_rev
            total_direct_costs += ev_direct
            total_shared_costs += ev_shared
            total_labor_costs += ev_labor
            total_ops_costs += ev_ops

            vendor_detail = []
            for v, info in sorted(
                direct_vendors_by_event.get(ek, {}).items(),
                key=lambda x: x[1]["amount"], reverse=True
            ):
                vendor_detail.append({
                    "vendor": v,
                    "category": info["category"],
                    "amount": round(info["amount"], 2),
                    "txns": info["txns"],
                })

            events_list.append({
                "key": ek,
                "label": cfg["label"],
                "dow_name": cfg["dow_name"],
                "dow_num": cfg["dow_num"],
                "num_nights": total_nights,
                "revenue": {
                    "net_sales": round(total_ns, 2),
                    "gratuity": round(total_grat, 2),
                    "grat_retained": round(total_gr, 2),
                    "adjusted_revenue": round(total_rev, 2),
                    "avg_nightly": avg_nightly,
                    "txn_count": total_txns,
                },
                "costs": {
                    "direct_costs": round(ev_direct, 2),
                    "shared_costs": round(ev_shared, 2),
                    "labor_costs": round(ev_labor, 2),
                    "ops_labor_costs": round(ev_ops, 2),
                    "total_costs": ev_total_costs,
                    "direct_vendors": vendor_detail,
                    "labor_pct": round(LABOR_DOW_PCT.get(ek, 0) * 100, 1),
                },
                "roi": {
                    "net_contribution": ev_net,
                    "roi_pct": ev_roi,
                    "margin_pct": ev_margin,
                    "cost_per_night": round(ev_total_costs / total_nights, 2) if total_nights > 0 else 0,
                },
            })

        events_list.sort(key=lambda e: e["revenue"]["adjusted_revenue"], reverse=True)

        for ev in events_list:
            ev["revenue"]["revenue_share_pct"] = round(
                ev["revenue"]["adjusted_revenue"] / total_event_revenue * 100, 1
            ) if total_event_revenue > 0 else 0

        total_costs = round(total_direct_costs + total_shared_costs + total_labor_costs + total_ops_costs, 2)
        total_net = round(total_event_revenue - total_costs, 2)
        summary = {
            "total_event_revenue": round(total_event_revenue, 2),
            "total_direct_costs": round(total_direct_costs, 2),
            "total_shared_costs": round(total_shared_costs, 2),
            "total_labor_costs": round(total_labor_costs, 2),
            "total_ops_labor_costs": round(total_ops_labor, 2),
            "total_event_costs": total_costs,
            "total_net_contribution": total_net,
            "overall_roi_pct": round(total_net / total_costs * 100, 1) if total_costs > 0 else 0,
            "overall_margin_pct": round(total_net / total_event_revenue * 100, 1) if total_event_revenue > 0 else 0,
            "unattributed_direct_costs": round(total_unattributed, 2),
            "labor_detail": {
                "gross_payroll": round(total_gross_labor, 2),
                "tip_grat_passthrough": round(total_passthrough, 2),
                "true_labor": round(total_true_labor, 2),
                "fixed_monthly": FIXED_LABOR_MONTHLY,
                "total_fixed": round(total_fixed_labor, 2),
                "variable_payroll": round(total_variable_labor, 2),
                "ops_labor": round(total_ops_labor, 2),
            },
        }

        # --- Step 5: Monthly trend ---
        monthly_trend = []
        for month in all_months:
            month_events = {}
            for ek in RECURRING_EVENTS:
                m_rev = rev_by_event_month.get(ek, {}).get(month, {}).get("adjusted_revenue", 0)
                m_direct = direct_by_event_month.get(ek, {}).get(month, 0)
                m_shared = shared_by_event_month.get(ek, {}).get(month, 0)
                m_labor = labor_by_event_month.get(ek, {}).get(month, 0)
                m_ops = ops_by_event_month.get(ek, {}).get(month, 0)
                m_costs = round(m_direct + m_shared + m_labor + m_ops, 2)
                m_net = round(m_rev - m_costs, 2)
                m_margin = round(m_net / m_rev * 100, 1) if m_rev > 0 else 0
                m_nights = rev_by_event_month.get(ek, {}).get(month, {}).get("num_nights", 0)
                month_events[ek] = {
                    "revenue": round(m_rev, 2),
                    "direct_costs": round(m_direct, 2),
                    "shared_costs": round(m_shared, 2),
                    "labor_costs": round(m_labor, 2),
                    "ops_labor_costs": round(m_ops, 2),
                    "total_costs": m_costs,
                    "net_contribution": m_net,
                    "margin_pct": m_margin,
                    "nights": m_nights,
                }
            monthly_trend.append({"month": month, "events": month_events})

        # --- Step 6: Insights ---
        insights = []

        if total_unattributed > 0:
            pct_unattr = round(total_unattributed / (total_unattributed + total_direct_costs) * 100, 1) if (total_unattributed + total_direct_costs) > 0 else 0
            insights.append({
                "severity": "warning",
                "text": (
                    f"${total_unattributed:,.0f} in direct event costs ({pct_unattr}%) "
                    f"are not mapped to any event. {len(unattributed_vendors)} vendors need "
                    f"attribution in EVENT_VENDOR_MAP. See the Unattributed Vendors table below."
                ),
            })

        if total_variable_labor > 0:
            labor_pct_of_rev = round(total_labor_costs / total_event_revenue * 100, 1) if total_event_revenue > 0 else 0
            insights.append({
                "severity": "info",
                "text": (
                    f"True Labor: ${total_true_labor:,.0f} (gross ${total_gross_labor:,.0f} "
                    f"minus ${total_passthrough:,.0f} tip/grat pass-through). "
                    f"After removing ${total_fixed_labor:,.0f} fixed overhead "
                    f"(mgmt + 1099), ${total_variable_labor:,.0f} variable labor "
                    f"allocated by DOW staffing % ({labor_pct_of_rev}% of event revenue)."
                ),
            })

        events_with_costs = [e for e in events_list if e["costs"]["total_costs"] > 0]
        if events_with_costs:
            best = max(events_with_costs, key=lambda e: e["roi"]["roi_pct"])
            worst = min(events_with_costs, key=lambda e: e["roi"]["roi_pct"])
            insights.append({
                "severity": "good",
                "text": (
                    f"{best['label']} ({best['dow_name']}) has the highest ROI at "
                    f"{best['roi']['roi_pct']}% — generating ${best['roi']['net_contribution']:,.0f} "
                    f"net contribution on ${best['costs']['total_costs']:,.0f} in costs."
                ),
            })
            if worst["key"] != best["key"]:
                sev = "critical" if worst["roi"]["margin_pct"] < 25 else "warning"
                insights.append({
                    "severity": sev,
                    "text": (
                        f"{worst['label']} ({worst['dow_name']}) has the lowest ROI at "
                        f"{worst['roi']['roi_pct']}% with {worst['roi']['margin_pct']}% margin. "
                        f"Review direct costs (${worst['costs']['direct_costs']:,.0f}) and "
                        f"consider renegotiating vendor contracts."
                    ),
                })

        if events_list:
            top_rev = events_list[0]
            insights.append({
                "severity": "info",
                "text": (
                    f"{top_rev['label']} drives the most revenue at "
                    f"${top_rev['revenue']['adjusted_revenue']:,.0f} "
                    f"({top_rev['revenue']['revenue_share_pct']}% of total), "
                    f"averaging ${top_rev['revenue']['avg_nightly']:,.0f}/night "
                    f"over {top_rev['num_nights']} nights."
                ),
            })

        events_w_rev = [e for e in events_list if e["revenue"]["adjusted_revenue"] > 0 and e["costs"]["total_costs"] > 0]
        if events_w_rev:
            most_efficient = min(events_w_rev, key=lambda e: e["costs"]["total_costs"] / e["revenue"]["adjusted_revenue"])
            cost_ratio = round(most_efficient["costs"]["total_costs"] / most_efficient["revenue"]["adjusted_revenue"] * 100, 1)
            insights.append({
                "severity": "info",
                "text": (
                    f"{most_efficient['label']} is the most cost-efficient event — "
                    f"only {cost_ratio}% of revenue goes to event costs, "
                    f"yielding {most_efficient['roi']['margin_pct']}% margin."
                ),
            })

        sev_order = {"critical": 0, "warning": 1, "info": 2, "good": 3}
        insights.sort(key=lambda x: sev_order.get(x["severity"], 9))

        return jsonify({
            "period": {"start_date": start_date, "end_date": end_date},
            "summary": summary,
            "events": events_list,
            "monthly_trend": monthly_trend,
            "unattributed_vendors": unattributed_vendors,
            "insights": insights,
        })

    except Exception as e:
        logging.exception("event-roi API error")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/menu-mix", methods=["POST"])
def api_menu_mix():
    """
    Menu mix / item analysis from ItemSelectionDetails.

    Request body:
    {
        "start_date": "2025-12-01",
        "end_date": "2026-02-27"
    }

    Returns top items, category breakdown, service period performance,
    day-of-week and hourly revenue profiles.
    """
    data = request.get_json()
    if not data or "start_date" not in data or "end_date" not in data:
        return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400

    start_date = data["start_date"]
    end_date = data["end_date"]

    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        bd = BUSINESS_DAY_SQL.format(dt_col="CAST(order_date AS DATETIME)")

        base_filter = (
            f"{bd} BETWEEN @start_date AND @end_date "
            "AND (voided = 'false' OR voided IS NULL) "
            "AND (deferred = 'false' OR deferred IS NULL) "
            "AND order_date IS NOT NULL AND order_date != ''"
        )

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )

        dedup_cte = f"""deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY item_selection_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
            WHERE order_date IS NOT NULL AND order_date != ''
          ) WHERE _rn = 1
        )"""

        q_kpis = f"""
        WITH {dedup_cte},
        base AS (
          SELECT
            CAST(net_price AS FLOAT64) AS net_price,
            CAST(qty AS INT64) AS qty,
            check_id
          FROM deduped
          WHERE {base_filter}
        ),
        voids AS (
          SELECT
            COALESCE(SUM(CAST(net_price AS FLOAT64)), 0) AS void_rev,
            COALESCE(SUM(CAST(qty AS INT64)), 0) AS void_qty
          FROM deduped
          WHERE {bd} BETWEEN @start_date AND @end_date
            AND voided = 'true'
        )
        SELECT
          COALESCE(SUM(b.net_price), 0) AS total_revenue,
          COALESCE(SUM(b.qty), 0) AS total_items,
          COUNT(DISTINCT b.check_id) AS unique_checks,
          SAFE_DIVIDE(SUM(b.net_price), NULLIF(COUNT(DISTINCT b.check_id), 0)) AS avg_check,
          v.void_rev,
          v.void_qty
        FROM base b
        CROSS JOIN voids v
        GROUP BY v.void_rev, v.void_qty
        """

        q_top_items = f"""
        WITH {dedup_cte}
        SELECT
          COALESCE(menu_item, '(unknown)') AS menu_item,
          COALESCE(menu_group, '(none)') AS menu_group,
          COALESCE(menu, '(none)') AS menu,
          SUM(CAST(qty AS INT64)) AS qty_sold,
          SUM(CAST(net_price AS FLOAT64)) AS net_revenue,
          SAFE_DIVIDE(SUM(CAST(net_price AS FLOAT64)), NULLIF(SUM(CAST(qty AS INT64)), 0)) AS avg_price
        FROM deduped
        WHERE {base_filter}
        GROUP BY menu_item, menu_group, menu
        ORDER BY net_revenue DESC
        LIMIT 20
        """

        q_cat_svc = f"""
        WITH {dedup_cte}
        SELECT
          COALESCE(sales_category, '(uncategorized)') AS sales_category,
          COALESCE(service, '(none)') AS service,
          SUM(CAST(qty AS INT64)) AS item_qty,
          SUM(CAST(net_price AS FLOAT64)) AS revenue,
          COUNT(DISTINCT check_id) AS checks
        FROM deduped
        WHERE {base_filter}
        GROUP BY sales_category, service
        ORDER BY revenue DESC
        """

        q_dow_hour = f"""
        WITH {dedup_cte}
        SELECT
          FORMAT_DATE('%A', {bd}) AS dow_name,
          EXTRACT(DAYOFWEEK FROM {bd}) AS dow_num,
          EXTRACT(HOUR FROM CAST(order_date AS DATETIME)) AS hour_of_day,
          {bd} AS business_date,
          SUM(CAST(qty AS INT64)) AS item_qty,
          SUM(CAST(net_price AS FLOAT64)) AS revenue,
          COUNT(DISTINCT check_id) AS checks
        FROM deduped
        WHERE {base_filter}
        GROUP BY dow_name, dow_num, hour_of_day, business_date
        """

        kpi_rows = list(bq_client.query(q_kpis, job_config=job_config).result())
        top_items_rows = list(bq_client.query(q_top_items, job_config=job_config).result())
        cat_svc_rows = list(bq_client.query(q_cat_svc, job_config=job_config).result())
        dow_hour_rows = list(bq_client.query(q_dow_hour, job_config=job_config).result())

        if kpi_rows:
            kr = kpi_rows[0]
            total_revenue = float(kr.total_revenue or 0)
            total_items = int(kr.total_items or 0)
            unique_checks = int(kr.unique_checks or 0)
            avg_check = float(kr.avg_check or 0)
            void_rev = float(kr.void_rev or 0)
            void_qty = int(kr.void_qty or 0)
        else:
            total_revenue = total_items = unique_checks = 0
            avg_check = void_rev = 0.0
            void_qty = 0

        total_with_voids = total_revenue + void_rev
        void_rate = (void_rev / total_with_voids * 100) if total_with_voids > 0 else 0.0

        kpis = {
            "total_revenue": round(total_revenue, 2),
            "total_items_sold": total_items,
            "unique_checks": unique_checks,
            "avg_check_size": round(avg_check, 2),
            "void_revenue": round(void_rev, 2),
            "void_qty": void_qty,
            "void_rate_pct": round(void_rate, 1),
        }

        top_items = []
        for r in top_items_rows:
            rev = float(r.net_revenue or 0)
            top_items.append({
                "menu_item": r.menu_item,
                "menu_group": r.menu_group,
                "menu": r.menu,
                "qty_sold": int(r.qty_sold or 0),
                "net_revenue": round(rev, 2),
                "avg_price": round(float(r.avg_price or 0), 2),
                "pct_of_total": round(rev / total_revenue * 100, 1) if total_revenue > 0 else 0.0,
            })

        cat_map: dict = {}
        svc_map: dict = {}
        for r in cat_svc_rows:
            cat = r.sales_category
            svc = r.service
            rev = float(r.revenue or 0)
            items = int(r.item_qty or 0)
            checks = int(r.checks or 0)

            if cat not in cat_map:
                cat_map[cat] = {"items": 0, "revenue": 0.0, "checks": 0}
            cat_map[cat]["items"] += items
            cat_map[cat]["revenue"] += rev
            cat_map[cat]["checks"] += checks

            if svc not in svc_map:
                svc_map[svc] = {"items": 0, "revenue": 0.0, "checks": 0}
            svc_map[svc]["items"] += items
            svc_map[svc]["revenue"] += rev
            svc_map[svc]["checks"] += checks

        categories = []
        for cat, vals in sorted(cat_map.items(), key=lambda x: x[1]["revenue"], reverse=True):
            categories.append({
                "category": cat,
                "items": vals["items"],
                "revenue": round(vals["revenue"], 2),
                "checks": vals["checks"],
                "pct_of_total": round(vals["revenue"] / total_revenue * 100, 1) if total_revenue > 0 else 0.0,
            })

        service_periods = []
        for svc, vals in sorted(svc_map.items(), key=lambda x: x[1]["revenue"], reverse=True):
            avg_chk = vals["revenue"] / vals["checks"] if vals["checks"] > 0 else 0.0
            service_periods.append({
                "service": svc,
                "items": vals["items"],
                "revenue": round(vals["revenue"], 2),
                "checks": vals["checks"],
                "avg_check": round(avg_chk, 2),
                "pct_of_total": round(vals["revenue"] / total_revenue * 100, 1) if total_revenue > 0 else 0.0,
            })

        dow_map: dict = {}
        hour_map: dict = {}
        for r in dow_hour_rows:
            dn = r.dow_name
            dnum = int(r.dow_num or 0)
            hour = int(r.hour_of_day or 0)
            bd_val = str(r.business_date) if r.business_date else None
            rev = float(r.revenue or 0)
            items = int(r.item_qty or 0)
            checks = int(r.checks or 0)

            if dn not in dow_map:
                dow_map[dn] = {"items": 0, "revenue": 0.0, "checks": 0, "dow_num": dnum, "dates": set()}
            dow_map[dn]["items"] += items
            dow_map[dn]["revenue"] += rev
            dow_map[dn]["checks"] += checks
            if bd_val:
                dow_map[dn]["dates"].add(bd_val)

            if hour not in hour_map:
                hour_map[hour] = {"items": 0, "revenue": 0.0, "checks": 0, "dates": set()}
            hour_map[hour]["items"] += items
            hour_map[hour]["revenue"] += rev
            hour_map[hour]["checks"] += checks
            if bd_val:
                hour_map[hour]["dates"].add(bd_val)

        day_of_week = []
        for dn, vals in sorted(dow_map.items(), key=lambda x: x[1]["dow_num"]):
            num_days = len(vals["dates"])
            avg_daily = vals["revenue"] / num_days if num_days > 0 else 0.0
            avg_chk = vals["revenue"] / vals["checks"] if vals["checks"] > 0 else 0.0
            day_of_week.append({
                "day": dn,
                "dow_num": vals["dow_num"],
                "items": vals["items"],
                "revenue": round(vals["revenue"], 2),
                "checks": vals["checks"],
                "num_days": num_days,
                "avg_check": round(avg_chk, 2),
                "avg_daily_revenue": round(avg_daily, 2),
                "pct_of_total": round(vals["revenue"] / total_revenue * 100, 1) if total_revenue > 0 else 0.0,
            })

        hourly_profile = []
        for hour in sorted(hour_map.keys()):
            vals = hour_map[hour]
            num_days = len(vals["dates"])
            avg_daily = vals["revenue"] / num_days if num_days > 0 else 0.0
            hourly_profile.append({
                "hour": hour,
                "items": vals["items"],
                "revenue": round(vals["revenue"], 2),
                "checks": vals["checks"],
                "num_days": num_days,
                "avg_daily_revenue": round(avg_daily, 2),
            })

        return jsonify({
            "period": {"start_date": start_date, "end_date": end_date},
            "kpis": kpis,
            "top_items": top_items,
            "categories": categories,
            "service_periods": service_periods,
            "day_of_week": day_of_week,
            "hourly_profile": hourly_profile,
        })

    except Exception as e:
        logger.error(f"Menu mix analysis failed: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/cash-recon", methods=["POST"])
def api_cash_recon():
    """
    Cash reconciliation: POS collections vs bank deposits.

    Request body:
    {
        "start_date": "2025-09-01",
        "end_date": "2026-02-28"
    }

    Returns monthly POS vs bank breakdown for credit cards and cash,
    with cumulative diffs, status badges, and alerts.
    """
    data = request.get_json()
    if not data or "start_date" not in data or "end_date" not in data:
        return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400

    start_date = data["start_date"]
    end_date = data["end_date"]

    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        pos_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', DATE(CAST(paid_date AS DATETIME))) AS month,
            status,
            CASE WHEN UPPER(payment_type) IN ('CREDIT','VISA','MASTERCARD','AMEX','DISCOVER') THEN 'Credit'
                 WHEN UPPER(payment_type) = 'CASH' THEN 'Cash' ELSE 'Other' END AS pay_type,
            COUNT(*) AS txn_count,
            COALESCE(SUM(CAST(total AS FLOAT64)), 0) AS gross_total,
            COALESCE(SUM(CAST(v_mc_d_fees AS FLOAT64)), 0) AS card_fees
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE DATE(CAST(paid_date AS DATETIME)) BETWEEN @start_date AND @end_date
            AND status IN ('CAPTURED', 'AUTHORIZED', 'CAPTURE_IN_PROGRESS')
        GROUP BY month, status, pay_type
        ORDER BY month, pay_type, status
        """
        date_params = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ])
        pos_rows = list(bq_client.query(pos_query, job_config=date_params).result())

        bank_table = f"{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw"
        has_bank = True
        try:
            bq_client.get_table(bank_table)
        except NotFound:
            has_bank = False

        bank_rows = []
        if has_bank:
            bank_query = f"""
            SELECT
                FORMAT_DATE('%Y-%m', CAST(transaction_date AS DATE)) AS month,
                CASE
                    WHEN description LIKE '%Citizens%NET SETLMT%' AND amount > 0 THEN 'citizens_settlement'
                    WHEN description LIKE '%TOAST DES:DEP%' AND amount > 0 THEN 'toast_dep'
                    WHEN description LIKE '%TOAST DES:EOM%' THEN 'toast_eom'
                    WHEN description LIKE '%Counter Credit%' AND amount > 0 THEN 'counter_credit'
                    WHEN description LIKE '%Toast, Inc DES:Toast%' AND amount < 0 THEN 'platform_fee'
                    WHEN description LIKE 'Online Banking transfer from CHK 9121%' AND amount > 0 THEN 'interaccount_in'
                END AS deposit_type,
                COALESCE(SUM(amount), 0) AS net_amount,
                COUNT(*) AS txn_count
            FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
            WHERE CAST(transaction_date AS DATE) BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
                AND (description LIKE '%Citizens%' OR description LIKE '%TOAST%'
                     OR description LIKE '%Toast%' OR description LIKE '%Counter Credit%'
                     OR (description LIKE 'Online Banking transfer from CHK 9121%' AND amount > 0))
            GROUP BY month, deposit_type
            HAVING deposit_type IS NOT NULL
            ORDER BY month, deposit_type
            """
            bank_rows = list(bq_client.query(bank_query, job_config=date_params).result())

        all_months: set[str] = set()
        pos_data: dict[str, dict] = {}
        for row in pos_rows:
            m = row.month
            all_months.add(m)
            if m not in pos_data:
                pos_data[m] = {
                    "credit_gross": 0.0, "credit_fees": 0.0,
                    "cash_collected": 0.0, "other_collected": 0.0,
                    "status_breakdown": {},
                }
            d = pos_data[m]
            amount = float(row.gross_total or 0)
            fees = float(row.card_fees or 0)
            count = int(row.txn_count or 0)
            status = row.status
            pay_type = row.pay_type

            if pay_type == "Credit":
                d["credit_gross"] += amount
                d["credit_fees"] += fees
            elif pay_type == "Cash":
                d["cash_collected"] += amount
            else:
                d["other_collected"] += amount

            if pay_type == "Credit":
                if status not in d["status_breakdown"]:
                    d["status_breakdown"][status] = {"count": 0, "amount": 0.0}
                d["status_breakdown"][status]["count"] += count
                d["status_breakdown"][status]["amount"] += amount

        bank_data: dict[str, dict] = {}
        for row in bank_rows:
            m = row.month
            all_months.add(m)
            if m not in bank_data:
                bank_data[m] = {
                    "citizens_settlement": 0.0, "toast_dep": 0.0,
                    "toast_eom": 0.0, "counter_credit": 0.0,
                    "platform_fee": 0.0, "interaccount_in": 0.0,
                    "citizens_settlement_count": 0, "toast_dep_count": 0,
                    "toast_eom_count": 0, "counter_credit_count": 0,
                    "platform_fee_count": 0, "interaccount_in_count": 0,
                }
            bd = bank_data[m]
            dtype = row.deposit_type
            amt = float(row.net_amount or 0)
            cnt = int(row.txn_count or 0)
            if dtype in bd:
                bd[dtype] += amt
                bd[f"{dtype}_count"] += cnt

        sorted_months = sorted(all_months)

        months_result = []
        cum_card_diff = 0.0
        cum_cash_gap = 0.0
        total_pos_credit_net = 0.0
        total_pos_cash = 0.0
        total_bank_card_net = 0.0
        total_bank_cash = 0.0
        alerts = []

        for m in sorted_months:
            pd_m = pos_data.get(m, {
                "credit_gross": 0.0, "credit_fees": 0.0,
                "cash_collected": 0.0, "other_collected": 0.0,
                "status_breakdown": {},
            })
            bd_m = bank_data.get(m, {
                "citizens_settlement": 0.0, "toast_dep": 0.0,
                "toast_eom": 0.0, "counter_credit": 0.0,
                "platform_fee": 0.0, "interaccount_in": 0.0,
                "citizens_settlement_count": 0, "toast_dep_count": 0,
                "toast_eom_count": 0, "counter_credit_count": 0,
                "platform_fee_count": 0, "interaccount_in_count": 0,
            })

            credit_net = round(pd_m["credit_gross"] - pd_m["credit_fees"], 2)
            total_card_deposits = round(
                bd_m["citizens_settlement"] + bd_m["toast_dep"] + bd_m["toast_eom"], 2
            )
            bank_card_net = round(total_card_deposits + bd_m["platform_fee"], 2)
            card_diff = round(bank_card_net - credit_net, 2)
            cum_card_diff = round(cum_card_diff + card_diff, 2)
            card_diff_pct = round(card_diff / credit_net * 100, 1) if credit_net else 0.0

            total_cash_in = round(bd_m["counter_credit"] + bd_m["interaccount_in"], 2)
            cash_gap = round(pd_m["cash_collected"] - total_cash_in, 2)
            cum_cash_gap = round(cum_cash_gap + cash_gap, 2)

            abs_pct = abs(card_diff_pct)
            card_status = "OK" if abs_pct <= 5 else ("WATCH" if abs_pct <= 10 else "HIGH")

            total_pos_credit_net += credit_net
            total_pos_cash += pd_m["cash_collected"]
            total_bank_card_net += bank_card_net
            total_bank_cash += total_cash_in

            if total_cash_in == 0 and pd_m["cash_collected"] > 0:
                alerts.append({
                    "month": m, "type": "zero_cash_deposit",
                    "message": f"$0 cash deposited/transferred but POS collected ${pd_m['cash_collected']:,.2f}",
                })
            if abs_pct > 10 and credit_net > 0:
                alerts.append({
                    "month": m, "type": "high_card_gap",
                    "message": f"Card gap is {card_diff_pct:+.1f}% (${card_diff:+,.2f})",
                })

            months_result.append({
                "month": m,
                "pos": {
                    "credit_gross": round(pd_m["credit_gross"], 2),
                    "credit_fees": round(pd_m["credit_fees"], 2),
                    "credit_net": credit_net,
                    "cash_collected": round(pd_m["cash_collected"], 2),
                    "other_collected": round(pd_m["other_collected"], 2),
                    "status_breakdown": {
                        k: {"count": v["count"], "amount": round(v["amount"], 2)}
                        for k, v in pd_m["status_breakdown"].items()
                    },
                },
                "bank": {
                    "citizens_settlement": round(bd_m["citizens_settlement"], 2),
                    "toast_dep": round(bd_m["toast_dep"], 2),
                    "toast_eom": round(bd_m["toast_eom"], 2),
                    "total_card_deposits": total_card_deposits,
                    "platform_fee": round(bd_m["platform_fee"], 2),
                    "net_card": bank_card_net,
                    "counter_credit": round(bd_m["counter_credit"], 2),
                    "interaccount_in": round(bd_m["interaccount_in"], 2),
                    "total_cash_in": total_cash_in,
                    "citizens_count": bd_m.get("citizens_settlement_count", 0),
                    "toast_dep_count": bd_m.get("toast_dep_count", 0),
                    "toast_eom_count": bd_m.get("toast_eom_count", 0),
                    "counter_credit_count": bd_m.get("counter_credit_count", 0),
                    "platform_fee_count": bd_m.get("platform_fee_count", 0),
                    "interaccount_in_count": bd_m.get("interaccount_in_count", 0),
                },
                "recon": {
                    "card_diff": card_diff,
                    "card_diff_pct": card_diff_pct,
                    "card_cum_diff": cum_card_diff,
                    "cash_gap": cash_gap,
                    "cash_cum_gap": cum_cash_gap,
                    "card_status": card_status,
                },
            })

        total_pos_credit_net = round(total_pos_credit_net, 2)
        total_pos_cash = round(total_pos_cash, 2)
        total_bank_card_net = round(total_bank_card_net, 2)
        total_bank_cash = round(total_bank_cash, 2)
        total_card_diff = round(total_bank_card_net - total_pos_credit_net, 2)
        card_recon_pct = round(
            total_bank_card_net / total_pos_credit_net * 100, 1
        ) if total_pos_credit_net else 0.0
        cash_deposited_pct = round(
            total_bank_cash / total_pos_cash * 100, 1
        ) if total_pos_cash else 0.0

        return jsonify({
            "period": {"start_date": start_date, "end_date": end_date},
            "months": months_result,
            "totals": {
                "pos_credit_net": total_pos_credit_net,
                "pos_cash": total_pos_cash,
                "bank_card_net": total_bank_card_net,
                "bank_cash": total_bank_cash,
                "card_recon_pct": card_recon_pct,
                "cash_deposited_pct": cash_deposited_pct,
                "undeposited_cash": round(total_pos_cash - total_bank_cash, 2),
                "total_card_diff": total_card_diff,
            },
            "alerts": alerts,
        })

    except Exception as e:
        logger.error(f"Cash reconciliation failed: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/profit-summary", methods=["POST"])
def profit_summary():
    """
    P&L report combining Toast revenue with bank expense data.

    Request body:
    {
        "start_date": "2025-01-01",
        "end_date": "2025-01-31"
    }

    Returns revenue (from Toast OrderDetails), expenses by category
    (from BankTransactions), COGS %, labor %, and net profit margin.
    """
    data = request.get_json()
    result = _validate_date_range(data)
    if not isinstance(result, tuple) or len(result) != 2 or not isinstance(result[0], str):
        return result  # error response
    start_date, end_date = result

    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        revenue_query = f"""
        SELECT
            COALESCE(SUM(amount), 0) as net_sales,
            COALESCE(SUM(tax), 0) as total_tax,
            COALESCE(SUM(tip), 0) as total_tips,
            COALESCE(SUM(gratuity), 0) as total_gratuity,
            COALESCE(SUM(total), 0) as gross_revenue,
            COUNT(DISTINCT order_id) as order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
            AND (voided IS NULL OR voided = 'false')
        """
        date_params = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ])
        rev_row = list(bq_client.query(revenue_query, job_config=date_params).result())[0]
        net_sales = float(rev_row.net_sales or 0)
        gross_revenue = float(rev_row.gross_revenue or 0)
        total_tips = float(rev_row.total_tips or 0)
        total_gratuity = float(rev_row.total_gratuity or 0)

        grat_retained = round(total_gratuity * GRAT_RETAIN_PCT, 2)
        grat_to_staff = round(total_gratuity * GRAT_PASSTHROUGH_PCT, 2)
        total_pass_through = round(total_tips + grat_to_staff, 2)
        adjusted_net_revenue = round(net_sales + grat_retained, 2)

        cash_query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN payment_type = 'Cash' OR payment_type LIKE '%CASH%'
                         THEN total ELSE 0 END), 0) as cash_collected,
            COUNTIF(payment_type = 'Cash' OR payment_type LIKE '%CASH%') as cash_txn_count
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
        """
        cash_row = list(bq_client.query(cash_query, job_config=date_params).result())[0]
        cash_collected = float(cash_row.cash_collected or 0)
        cash_txn_count = int(cash_row.cash_txn_count or 0)

        drawer_query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN action = 'CASH_COLLECTED' THEN amount ELSE 0 END), 0) as drawer_collected,
            COALESCE(SUM(CASE WHEN action = 'PAY_OUT' THEN ABS(amount) ELSE 0 END), 0) as payouts,
            COALESCE(SUM(CASE WHEN action = 'CLOSE_OUT_OVERAGE' THEN amount ELSE 0 END), 0) as overages,
            COALESCE(SUM(CASE WHEN action = 'CLOSE_OUT_SHORTAGE' THEN ABS(amount) ELSE 0 END), 0) as shortages,
            COUNTIF(action = 'NO_SALE') as no_sale_count,
            COUNTIF(action = 'CLOSE_OUT_EXACT') as exact_closeouts
        FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
        WHERE processing_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
        """
        drawer_row = list(bq_client.query(drawer_query, job_config=date_params).result())[0]
        drawer_collected = float(drawer_row.drawer_collected or 0)
        drawer_payouts = float(drawer_row.payouts or 0)
        drawer_overages = float(drawer_row.overages or 0)
        drawer_shortages = float(drawer_row.shortages or 0)
        no_sale_count = int(drawer_row.no_sale_count or 0)
        exact_closeouts = int(drawer_row.exact_closeouts or 0)

        bank_table = f"{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw"
        try:
            bq_client.get_table(bank_table)
            has_bank_data = True
        except NotFound:
            has_bank_data = False

        expenses_by_category = {}
        total_expenses = 0.0
        cash_deposited = 0.0

        if has_bank_data:
            expense_query = f"""
            SELECT
                category,
                ROUND(SUM(abs_amount), 2) as total
            FROM `{bank_table}`
            WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
                AND transaction_type = 'debit'
            GROUP BY category
            ORDER BY total DESC
            """
            expense_rows = list(bq_client.query(expense_query, job_config=date_params).result())
            for row in expense_rows:
                expenses_by_category[row.category] = float(row.total or 0)
            total_expenses = sum(
                v for k, v in expenses_by_category.items()
                if "revenue" not in k.lower()
            )

            deposit_query = f"""
            SELECT
                COALESCE(SUM(abs_amount), 0) as total_deposits
            FROM `{bank_table}`
            WHERE transaction_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
                AND transaction_type = 'credit'
                AND (LOWER(category) LIKE '%cash deposit%'
                     OR LOWER(category) LIKE '%cash account transfer%'
                     OR LOWER(description) LIKE '%counter credit%')
            """
            dep_row = list(bq_client.query(deposit_query, job_config=date_params).result())[0]
            cash_deposited = float(dep_row.total_deposits or 0)

        def sum_matching(expenses: Dict[str, float], keywords: List[str]) -> float:
            total = 0.0
            for cat, amt in expenses.items():
                cat_lower = cat.lower()
                if any(kw.lower() in cat_lower for kw in keywords):
                    total += amt
            return total

        total_cogs = sum_matching(expenses_by_category, ["cost of goods", "cogs"])
        labor_gross = sum_matching(expenses_by_category, ["3. labor", "labor cost", "payroll"])
        marketing = sum_matching(expenses_by_category, ["marketing", "promotions", "entertainment", "event"])
        opex = sum_matching(expenses_by_category, ["operating expenses", "opex"])

        labor_true = round(labor_gross - total_pass_through, 2)
        if labor_true < 0:
            labor_true = 0.0

        adjusted_expenses = round(total_expenses - total_pass_through, 2)
        if adjusted_expenses < 0:
            adjusted_expenses = total_expenses

        rev = adjusted_net_revenue if adjusted_net_revenue > 0 else 1
        prime_cost = round(total_cogs + labor_true, 2)
        net_profit = round(adjusted_net_revenue - adjusted_expenses, 2)

        undeposited_cash = round(cash_collected - cash_deposited, 2)
        cash_adjusted_net_profit = round(net_profit + undeposited_cash, 2)
        cash_adjusted_margin = round((cash_adjusted_net_profit / rev * 100), 1)

        return jsonify({
            "period": {"start_date": start_date, "end_date": end_date},
            "revenue": {
                "net_sales": round(net_sales, 2),
                "tax": round(float(rev_row.total_tax or 0), 2),
                "tips": round(total_tips, 2),
                "gratuity": round(total_gratuity, 2),
                "gratuity_retained_by_lov3": grat_retained,
                "gratuity_paid_to_staff": grat_to_staff,
                "total_pass_through_to_staff": total_pass_through,
                "adjusted_net_revenue": adjusted_net_revenue,
                "gross_revenue": round(gross_revenue, 2),
                "order_count": int(rev_row.order_count or 0),
            },
            "expenses": {
                "by_category": expenses_by_category,
                "total_expenses_gross": round(total_expenses, 2),
                "less_pass_through": total_pass_through,
                "total_expenses_adjusted": adjusted_expenses,
            },
            "cash_control": {
                "toast_cash_collected": round(cash_collected, 2),
                "toast_cash_txn_count": cash_txn_count,
                "bank_cash_deposited": round(cash_deposited, 2),
                "undeposited_cash": undeposited_cash,
                "drawer_activity": {
                    "drawer_collected": round(drawer_collected, 2),
                    "payouts": round(drawer_payouts, 2),
                    "overages": round(drawer_overages, 2),
                    "shortages": round(drawer_shortages, 2),
                    "no_sale_count": no_sale_count,
                    "exact_closeouts": exact_closeouts,
                },
            },
            "profitability": {
                "net_profit_bank_only": net_profit,
                "margin_pct_bank_only": round((net_profit / rev * 100), 1),
                "net_profit_cash_adjusted": cash_adjusted_net_profit,
                "margin_pct_cash_adjusted": cash_adjusted_margin,
                "cogs_total": round(total_cogs, 2),
                "cogs_pct": round((total_cogs / rev * 100), 1),
                "labor_gross": round(labor_gross, 2),
                "labor_pass_through": total_pass_through,
                "labor_true": labor_true,
                "labor_pct": round((labor_true / rev * 100), 1),
                "prime_cost": prime_cost,
                "prime_cost_pct": round((prime_cost / rev * 100), 1),
                "marketing_total": round(marketing, 2),
                "marketing_pct": round((marketing / rev * 100), 1),
                "opex_total": round(opex, 2),
                "opex_pct": round((opex / rev * 100), 1),
            },
            "has_bank_data": has_bank_data,
        })

    except Exception as e:
        logger.error(f"Profit summary error: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/comprehensive-analysis", methods=["POST"])
def comprehensive_analysis():
    """
    Full financial analysis combining Toast POS + BofA bank data.
    """
    data = request.get_json()
    result = _validate_date_range(data)
    if not isinstance(result, tuple) or len(result) != 2 or not isinstance(result[0], str):
        return result  # error response
    start_date, end_date = result

    try:
        report = WeeklyReportGenerator()

        revenue_by_dow = report.query_revenue_by_business_day(start_date, end_date)
        monthly_pnl = report.query_monthly_pnl(start_date, end_date)
        hourly_profile = report.query_hourly_revenue_profile(start_date, end_date)

        total_adj_revenue = sum(m["adjusted_revenue"] for m in monthly_pnl)
        total_adj_expenses = sum(m["total_expenses_adjusted"] for m in monthly_pnl)
        total_cogs = sum(m["cogs"] for m in monthly_pnl)
        total_labor_true = sum(m["labor_true"] for m in monthly_pnl)
        total_marketing = sum(m["marketing"] for m in monthly_pnl)
        total_opex = sum(m["opex"] for m in monthly_pnl)
        total_net_profit = sum(m["net_profit"] for m in monthly_pnl)
        total_unreconciled = sum(m["unreconciled_cash"] for m in monthly_pnl)
        total_pass_through = sum(m["pass_through_to_staff"] for m in monthly_pnl)

        rev_denom = total_adj_revenue if total_adj_revenue > 0 else 1
        prime_cost = total_cogs + total_labor_true

        return jsonify({
            "period": {"start_date": start_date, "end_date": end_date},
            "assumptions": {
                "business_day_cutoff_hour": BUSINESS_DAY_CUTOFF_HOUR,
                "gratuity_retain_pct": GRAT_RETAIN_PCT,
                "gratuity_passthrough_pct": GRAT_PASSTHROUGH_PCT,
                "notes": [
                    "Business day runs 4:00AM to 3:59AM (nightlife venue)",
                    "Revenue at 1AM Saturday is attributed to Friday",
                    "True labor = gross labor - tip/gratuity pass-through",
                    "Unreconciled cash = Toast cash collected - bank deposits",
                    "Categories reflect post-audit hierarchy (Feb 2026)",
                ],
            },
            "summary_pnl": {
                "adjusted_revenue": round(total_adj_revenue, 2),
                "total_expenses_adjusted": round(total_adj_expenses, 2),
                "net_profit": round(total_net_profit, 2),
                "margin_pct": round(total_net_profit / rev_denom * 100, 1),
                "cogs": round(total_cogs, 2),
                "cogs_pct": round(total_cogs / rev_denom * 100, 1),
                "labor_true": round(total_labor_true, 2),
                "labor_pct": round(total_labor_true / rev_denom * 100, 1),
                "prime_cost": round(prime_cost, 2),
                "prime_cost_pct": round(prime_cost / rev_denom * 100, 1),
                "marketing": round(total_marketing, 2),
                "marketing_pct": round(total_marketing / rev_denom * 100, 1),
                "opex": round(total_opex, 2),
                "opex_pct": round(total_opex / rev_denom * 100, 1),
                "pass_through_to_staff": round(total_pass_through, 2),
                "unreconciled_cash": round(total_unreconciled, 2),
                "net_profit_cash_adjusted": round(
                    total_net_profit + total_unreconciled, 2
                ),
                "margin_pct_cash_adjusted": round(
                    (total_net_profit + total_unreconciled) / rev_denom * 100, 1
                ),
            },
            "monthly_pnl": monthly_pnl,
            "revenue_by_business_day": revenue_by_dow,
            "hourly_revenue_profile": hourly_profile,
            "num_months": len(monthly_pnl),
        })

    except Exception as e:
        logger.error(f"Comprehensive analysis error: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/sync-check-register", methods=["POST"])
def sync_check_register():
    """Manually sync the Google Sheet check register into BigQuery."""
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        register = CheckRegisterSync(bq_client, DATASET_ID)
        count = register.sync_from_sheet()
        return jsonify({
            "status": "success",
            "rows_synced": count,
            "sheet_id": CHECK_REGISTER_SHEET_ID,
        })
    except GoogleHttpError as e:
        logger.error(f"Google Sheets API error: {e}")
        return jsonify({"error": f"Google Sheets API error: {e}"}), 502
    except Exception as e:
        logger.error(f"Check register sync failed: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/upload-check-register", methods=["POST"])
def upload_check_register():
    """Upload a check register CSV (fallback if Google Sheet sync fails)."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided. Use multipart form with key 'file'."}), 400

    uploaded = request.files["file"]
    if uploaded.filename == "":
        return jsonify({"error": "Empty filename"}), 400

    try:
        file_content = uploaded.read()
        bq_client = bigquery.Client(project=PROJECT_ID)
        register = CheckRegisterSync(bq_client, DATASET_ID)
        count = register.load_from_csv(file_content)
        return jsonify({
            "status": "success",
            "rows_loaded": count,
            "source": uploaded.filename,
        })
    except ValueError as e:
        logger.error(f"Check register CSV parse error: {e}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Check register upload failed: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/reconcile-checks", methods=["POST"])
def reconcile_checks():
    """Re-categorize uncategorized Check transactions using the current register."""
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        register_sync = CheckRegisterSync(bq_client, DATASET_ID)
        synced = register_sync.sync_from_sheet()
        check_lookup = register_sync.get_lookup()

        cat_manager = BankCategoryManager(bq_client, DATASET_ID)
        rules = cat_manager.list_rules()

        parser = BofACSVParser(rules, check_register=check_lookup)

        query = f"""
            SELECT transaction_date, description, amount
            FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
            WHERE category = 'Uncategorized'
              AND REGEXP_CONTAINS(description, r'(?i)^check\\s+\\d+$')
        """
        rows = list(bq_client.query(query).result())
        if not rows:
            return jsonify({
                "status": "success",
                "register_synced": synced,
                "reconciled": 0,
                "message": "No uncategorized checks found.",
            })

        updates: list = []
        for row in rows:
            cat, source, vendor = parser._categorize(row.description)
            if cat != "Uncategorized":
                updates.append({
                    "transaction_date": str(row.transaction_date),
                    "description": row.description,
                    "amount": float(row.amount),
                    "category": cat,
                    "category_source": source,
                    "vendor_normalized": vendor,
                })

        if not updates:
            return jsonify({
                "status": "success",
                "register_synced": synced,
                "reconciled": 0,
                "still_uncategorized": len(rows),
                "message": "All uncategorized checks lack register entries or matching rules.",
            })

        reconciled = 0
        for u in updates:
            uq = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
                SET category = @category,
                    category_source = @source,
                    vendor_normalized = @vendor
                WHERE transaction_date = PARSE_DATE('%Y-%m-%d', @txn_date)
                  AND description = @desc
                  AND amount = @amt
            """
            params = [
                bigquery.ScalarQueryParameter("category", "STRING", u["category"]),
                bigquery.ScalarQueryParameter("source", "STRING", u["category_source"]),
                bigquery.ScalarQueryParameter("vendor", "STRING", u["vendor_normalized"]),
                bigquery.ScalarQueryParameter("txn_date", "STRING", u["transaction_date"]),
                bigquery.ScalarQueryParameter("desc", "STRING", u["description"]),
                bigquery.ScalarQueryParameter("amt", "FLOAT64", u["amount"]),
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            bq_client.query(uq, job_config=job_config).result()
            reconciled += 1

        return jsonify({
            "status": "success",
            "register_synced": synced,
            "reconciled": reconciled,
            "still_uncategorized": len(rows) - reconciled,
            "details": [
                {"check": u["description"], "payee": u["vendor_normalized"], "category": u["category"]}
                for u in updates
            ],
        })
    except Exception as e:
        logger.error(f"Check reconciliation failed: {e}")
        return jsonify({"error": str(e)}), 500


# ─── Daily Flash Report API ──────────────────────────────────────────────────

@bp.route("/api/flash-report", methods=["POST"])
def api_flash_report():
    """
    Daily flash report — yesterday's key metrics at a glance.

    Request body (optional):
        {"date": "2026-03-22"}  // defaults to yesterday

    Returns revenue, orders, guests, avg check, top servers, expenses,
    margins, cash gap, and comparison to same day last week.

    Also sends Slack + email if ?send=true query param is set.
    """
    import re
    data = request.get_json(silent=True) or {}
    report_date = data.get("date")

    # Validate date format if provided
    if report_date:
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", report_date):
            return jsonify({"error": f"Invalid date format: {report_date}. Use YYYY-MM-DD"}), 400

    try:
        from flash_report import FlashReport
        fr = FlashReport()
        report_data = fr.collect(report_date)
        result = fr.format_json(report_data)

        # Send notifications if requested
        if request.args.get("send") == "true":
            fr.send_slack(report_data)
            fr.send_email(report_data)
            result["notifications"] = {"slack": True, "email": True}

        return jsonify(result)

    except Exception as e:
        logger.error(f"Flash report error: {e}")
        return jsonify({"error": str(e)}), 500


# ─── Vendor Spend Tracker API ────────────────────────────────────────────────

@bp.route("/api/vendor-tracker", methods=["POST"])
def api_vendor_tracker():
    """
    Vendor spend analysis — top vendors, trends, concentration, anomalies.

    Request body:
        {"start_date": "2025-09-01", "end_date": "2026-02-28", "limit": 30}
    """
    import re
    data = request.get_json() or {}

    start_date = data.get("start_date", "")
    end_date = data.get("end_date", "")
    limit = min(int(data.get("limit", 30)), 100)

    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", start_date) or not re.match(r"^\d{4}-\d{2}-\d{2}$", end_date):
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

    # Check cache
    ck = _cache_key("vendor_tracker", {"start_date": start_date, "end_date": end_date, "limit": limit})
    cached = _cache_get(ck)
    if cached is not None:
        return jsonify(cached)

    try:
        from vendor_tracker import VendorTracker
        vt = VendorTracker()
        result = vt.collect(start_date, end_date, limit)
        _cache_set(ck, result)
        return jsonify(result)

    except Exception as e:
        logger.error(f"Vendor tracker error: {e}")
        return jsonify({"error": str(e)}), 500
