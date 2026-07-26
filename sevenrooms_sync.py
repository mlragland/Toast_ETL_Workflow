"""SevenRooms Reservations Sync — incremental pull to BigQuery.

Pulls reservations via GET /reservations using the `updated_since` filter
plus cursor pagination, then MERGEs into SevenRooms_Reservations_raw.

Auth: POST /auth with client_id + client_secret (form-urlencoded) →
       {data: {token, token_expiration_datetime}}. Token lasts 24h.
Header on subsequent requests: `Authorization: <token>` (no "Bearer" prefix).

Pattern intentionally mirrors teller_sync.py so Cloud Run + Cloud Scheduler +
Slack alert wiring is consistent across sync services.
"""

import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery

from config import ALERT_WEBHOOK_URL, DATASET_ID, PROJECT_ID
from services import AlertManager, SecretManager

logger = logging.getLogger(__name__)

SR_API_BASE = "https://www.sevenrooms.com/api-ext/2_4"
SR_TABLE = f"{PROJECT_ID}.{DATASET_ID}.SevenRooms_Reservations_raw"

# API max is 400; 200 keeps memory/latency reasonable
SR_PAGE_LIMIT = 200

# Alert when latest reservation.updated is more than this old on a successful sync
STALE_HOURS = 6

# Overlap window on updated_since to tolerate clock skew and late writes
LOOKBACK_MINUTES = 5

# First-run backfill window if the target table is empty
DEFAULT_INITIAL_LOOKBACK_DAYS = 30


class TokenCache:
    """In-process auth-token cache with a 5-minute pre-expiry buffer.

    Scoped to a single SevenRoomsSync instance; if the Cloud Run container stays
    warm across requests we also avoid re-auth across syncs, but the correctness
    guarantee is only per-instance.
    """

    def __init__(self) -> None:
        self._token: Optional[str] = None
        self._expires_at: float = 0.0

    def get(self) -> Optional[str]:
        if self._token and time.time() < self._expires_at - 300:
            return self._token
        return None

    def set(self, token: str, expires_at_iso: str) -> None:
        self._token = token
        try:
            dt = datetime.fromisoformat(expires_at_iso.replace("Z", "+00:00"))
            self._expires_at = dt.timestamp()
        except (ValueError, TypeError, AttributeError):
            # Fall back to a conservative 23-hour cache if the expiry string
            # is malformed — one hour shy of SR's stated 24h lifetime.
            self._expires_at = time.time() + 23 * 3600

    def clear(self) -> None:
        self._token = None
        self._expires_at = 0.0


@dataclass
class SyncSummary:
    status: str
    reservations_pulled: int = 0
    rows_merged: int = 0
    updated_since: str = ""
    duration_seconds: float = 0.0
    latest_updated_in_bq: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "status": self.status,
            "reservations_pulled": self.reservations_pulled,
            "rows_merged": self.rows_merged,
            "updated_since": self.updated_since,
            "duration_seconds": round(self.duration_seconds, 2),
            "latest_updated_in_bq": self.latest_updated_in_bq,
        }
        if self.error:
            d["error"] = self.error
        return d


def _reservation_schema() -> List[bigquery.SchemaField]:
    """Typed columns for the fields we plan to query analytically.

    Every raw reservation is also stored in `raw_json` so we can extract
    additional fields later without a re-sync.
    """
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("venue_id", "STRING"),
        bigquery.SchemaField("venue_group_id", "STRING"),
        bigquery.SchemaField("client_id", "STRING"),
        bigquery.SchemaField("reference_code", "STRING"),
        bigquery.SchemaField("external_id", "STRING"),
        bigquery.SchemaField("external_reference_code", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("arrival_time", "STRING"),
        bigquery.SchemaField("seated_time", "STRING"),
        bigquery.SchemaField("left_time", "STRING"),
        bigquery.SchemaField("duration", "INTEGER"),
        bigquery.SchemaField("time_slot_iso", "STRING"),
        bigquery.SchemaField("real_datetime_of_slot", "TIMESTAMP"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("phone_number", "STRING"),
        bigquery.SchemaField("is_vip", "BOOLEAN"),
        bigquery.SchemaField("loyalty_tier", "STRING"),
        bigquery.SchemaField("booked_by", "STRING"),
        bigquery.SchemaField("served_by", "STRING"),
        bigquery.SchemaField("paid_by", "STRING"),
        bigquery.SchemaField("max_guests", "INTEGER"),
        bigquery.SchemaField("arrived_guests", "INTEGER"),
        bigquery.SchemaField("mf_ratio_male", "INTEGER"),
        bigquery.SchemaField("mf_ratio_female", "INTEGER"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("status_simple", "STRING"),
        bigquery.SchemaField("status_display", "STRING"),
        bigquery.SchemaField("deleted", "BOOLEAN"),
        bigquery.SchemaField("check_numbers", "STRING"),
        bigquery.SchemaField("shift_persistent_id", "STRING"),
        bigquery.SchemaField("shift_category", "STRING"),
        bigquery.SchemaField("venue_seating_area_id", "STRING"),
        bigquery.SchemaField("venue_seating_area_name", "STRING"),
        bigquery.SchemaField("reservation_type", "STRING"),
        bigquery.SchemaField("total_gross_payment", "FLOAT"),
        bigquery.SchemaField("total_net_payment", "FLOAT"),
        bigquery.SchemaField("total_payment", "FLOAT"),
        bigquery.SchemaField("onsite_payment_total", "FLOAT"),
        bigquery.SchemaField("prepayment_total", "FLOAT"),
        bigquery.SchemaField("cancellation_fee", "FLOAT"),
        bigquery.SchemaField("comps", "FLOAT"),
        bigquery.SchemaField("rating", "FLOAT"),
        bigquery.SchemaField("notes", "STRING"),
        bigquery.SchemaField("client_requests", "STRING"),
        bigquery.SchemaField("created", "TIMESTAMP"),
        bigquery.SchemaField("updated", "TIMESTAMP"),
        # Nested API structures — stored as JSON strings so we can query with JSON_EXTRACT
        bigquery.SchemaField("tags_json", "STRING"),
        bigquery.SchemaField("pos_tickets_json", "STRING"),
        bigquery.SchemaField("custom_fields_json", "STRING"),
        bigquery.SchemaField("upgrades_json", "STRING"),
        bigquery.SchemaField("table_numbers_json", "STRING"),
        bigquery.SchemaField("raw_json", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("synced_at", "TIMESTAMP", mode="REQUIRED"),
    ]


def _reservation_to_row(r: Dict, synced_at: str) -> Optional[Dict]:
    """Convert a raw SR reservation dict to a BQ row. Returns None if `id` missing."""
    rid = r.get("id")
    if not rid:
        return None

    def _iso_ts(v):
        if not v:
            return None
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00")).isoformat()
        except (ValueError, TypeError):
            return None

    def _iso_date(v):
        if not v:
            return None
        try:
            return datetime.strptime(str(v)[:10], "%Y-%m-%d").date().isoformat()
        except (ValueError, TypeError):
            return None

    def _json_or_none(v):
        if v in (None, "", [], {}):
            return None
        return json.dumps(v)

    def _f(v):
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    def _i(v):
        if v in (None, ""):
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None

    def _s(v):
        return None if v is None else str(v)

    def _b(v):
        return None if v is None else bool(v)

    return {
        "id": _s(rid),
        "venue_id": _s(r.get("venue_id")),
        "venue_group_id": _s(r.get("venue_group_id")),
        "client_id": _s(r.get("client_id")),
        "reference_code": _s(r.get("reference_code")),
        "external_id": _s(r.get("external_id")),
        "external_reference_code": _s(r.get("external_reference_code")),
        "date": _iso_date(r.get("date")),
        "arrival_time": _s(r.get("arrival_time")),
        "seated_time": _s(r.get("seated_time")),
        "left_time": _s(r.get("left_time")),
        "duration": _i(r.get("duration")),
        "time_slot_iso": _s(r.get("time_slot_iso")),
        "real_datetime_of_slot": _iso_ts(r.get("real_datetime_of_slot")),
        "first_name": _s(r.get("first_name")),
        "last_name": _s(r.get("last_name")),
        "email": _s(r.get("email")),
        "phone_number": _s(r.get("phone_number")),
        "is_vip": _b(r.get("is_vip")),
        "loyalty_tier": _s(r.get("loyalty_tier")),
        "booked_by": _s(r.get("booked_by")),
        "served_by": _s(r.get("served_by")),
        "paid_by": _s(r.get("paid_by")),
        "max_guests": _i(r.get("max_guests")),
        "arrived_guests": _i(r.get("arrived_guests")),
        "mf_ratio_male": _i(r.get("mf_ratio_male")),
        "mf_ratio_female": _i(r.get("mf_ratio_female")),
        "status": _s(r.get("status")),
        "status_simple": _s(r.get("status_simple")),
        "status_display": _s(r.get("status_display")),
        "deleted": _b(r.get("deleted")),
        "check_numbers": _s(r.get("check_numbers")),
        "shift_persistent_id": _s(r.get("shift_persistent_id")),
        "shift_category": _s(r.get("shift_category")),
        "venue_seating_area_id": _s(r.get("venue_seating_area_id")),
        "venue_seating_area_name": _s(r.get("venue_seating_area_name")),
        "reservation_type": _s(r.get("reservation_type")),
        "total_gross_payment": _f(r.get("total_gross_payment")),
        "total_net_payment": _f(r.get("total_net_payment")),
        "total_payment": _f(r.get("total_payment")),
        "onsite_payment_total": _f(r.get("onsite_payment_total")),
        "prepayment_total": _f(r.get("prepayment_total")),
        "cancellation_fee": _f(r.get("cancellation_fee")),
        "comps": _f(r.get("comps")),
        "rating": _f(r.get("rating")),
        "notes": _s(r.get("notes")),
        "client_requests": _s(r.get("client_requests")),
        "created": _iso_ts(r.get("created")),
        "updated": _iso_ts(r.get("updated")),
        "tags_json": _json_or_none(r.get("tags")),
        "pos_tickets_json": _json_or_none(r.get("pos_tickets")),
        "custom_fields_json": _json_or_none(r.get("custom_fields")),
        "upgrades_json": _json_or_none(r.get("upgrades")),
        "table_numbers_json": _json_or_none(r.get("table_numbers")),
        "raw_json": json.dumps(r),
        "synced_at": synced_at,
    }


class SevenRoomsSync:
    """Sync SR reservations into BigQuery incrementally."""

    def __init__(self, bq_client: Optional[bigquery.Client] = None,
                 secret_manager: Optional[SecretManager] = None) -> None:
        self.bq = bq_client or bigquery.Client(project=PROJECT_ID)
        self.sm = secret_manager or SecretManager(PROJECT_ID)
        self._token_cache = TokenCache()

    # ── Credentials ──────────────────────────────────────────────────────

    def _get_credentials(self) -> Tuple[str, str, str]:
        client_id = self.sm.get_secret("sevenrooms-client-id").strip()
        client_secret = self.sm.get_secret("sevenrooms-client-secret").strip()
        venue_group_id = self.sm.get_secret("sevenrooms-venue-group-id").strip()
        return client_id, client_secret, venue_group_id

    def _get_token(self) -> str:
        cached = self._token_cache.get()
        if cached:
            return cached
        client_id, client_secret, _ = self._get_credentials()
        resp = requests.post(
            f"{SR_API_BASE}/auth",
            data={"client_id": client_id, "client_secret": client_secret},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") != 200 or "data" not in body:
            raise RuntimeError(f"SR /auth returned status={body.get('status')} msg={body.get('msg')}")
        data = body["data"]
        token = data.get("token")
        if not token:
            raise RuntimeError("SR /auth response missing token")
        self._token_cache.set(token, data.get("token_expiration_datetime", ""))
        return token

    # ── HTTP ─────────────────────────────────────────────────────────────

    def _api_get(self, path: str, params: Dict,
                 _retried_401: bool = False,
                 _retries_429: int = 0) -> Dict:
        MAX_429_RETRIES = 5
        token = self._get_token()
        resp = requests.get(
            f"{SR_API_BASE}{path}",
            params=params,
            headers={"Authorization": token, "Accept": "application/json"},
            timeout=60,
        )
        # 401: token expired mid-sync. Clear cache and retry ONCE.
        if resp.status_code == 401 and not _retried_401:
            logger.warning("SR 401 — token likely expired mid-sync, refreshing")
            self._token_cache.clear()
            return self._api_get(path, params, _retried_401=True,
                                 _retries_429=_retries_429)
        # 429: back off using Retry-After (bounded). Cap total attempts.
        if resp.status_code == 429:
            if _retries_429 >= MAX_429_RETRIES:
                raise RuntimeError(f"SR /{path} — 429 rate-limit exceeded after {_retries_429} retries")
            wait = min(int(resp.headers.get("Retry-After", "5") or 5), 60)
            logger.warning("SR 429 (attempt %d/%d) — sleeping %ds",
                           _retries_429 + 1, MAX_429_RETRIES, wait)
            time.sleep(wait)
            return self._api_get(path, params,
                                 _retried_401=_retried_401,
                                 _retries_429=_retries_429 + 1)
        resp.raise_for_status()
        return resp.json()

    # ── Fetch ────────────────────────────────────────────────────────────

    def _fetch_reservations(self, updated_since: str, venue_group_id: str) -> List[Dict]:
        """Iterate all pages of /reservations updated since `updated_since`."""
        all_rows: List[Dict] = []
        cursor: Optional[str] = None
        pages = 0
        MAX_PAGES = 500  # safety cap: 500 pages × 200 = 100k reservations
        while True:
            params: Dict[str, Any] = {
                "venue_group_id": venue_group_id,
                "updated_since": updated_since,
                "limit": SR_PAGE_LIMIT,
                "sort_order": "asc",
            }
            if cursor:
                params["cursor"] = cursor
            body = self._api_get("/reservations", params)
            if body.get("status") != 200:
                raise RuntimeError(
                    f"SR /reservations status={body.get('status')} msg={body.get('msg')}"
                )
            data = body.get("data") or {}
            results = data.get("results") or []
            all_rows.extend(results)
            pages += 1
            cursor = data.get("cursor")
            if not cursor or not results:
                break
            if pages >= MAX_PAGES:
                logger.warning("SR sync hit %d-page safety cap; stopping early", MAX_PAGES)
                break
        logger.info("SR fetched %d reservations across %d pages", len(all_rows), pages)
        return all_rows

    # ── BigQuery ─────────────────────────────────────────────────────────

    def _table_exists(self) -> bool:
        try:
            self.bq.get_table(SR_TABLE)
            return True
        except Exception:
            return False

    def _ensure_table(self) -> None:
        if self._table_exists():
            return
        table = bigquery.Table(SR_TABLE, schema=_reservation_schema())
        self.bq.create_table(table)
        logger.info("Created BQ table %s", SR_TABLE)

    def _latest_updated_in_bq(self) -> Optional[str]:
        if not self._table_exists():
            return None
        try:
            row = next(self.bq.query(
                f"SELECT MAX(updated) AS latest FROM `{SR_TABLE}`"
            ).result(), None)
            if row and row.latest:
                return row.latest.isoformat()
        except Exception as e:
            logger.warning("Could not read MAX(updated) from BQ: %s", e)
        return None

    def _load_to_bq(self, rows: List[Dict]) -> int:
        if not rows:
            return 0
        self._ensure_table()
        staging = f"{SR_TABLE}_staging_{int(time.time())}"
        schema = _reservation_schema()
        try:
            load_job = self.bq.load_table_from_json(
                rows, staging,
                job_config=bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                ),
            )
            load_job.result()

            cols = [f.name for f in schema]
            insert_cols = ", ".join(cols)
            insert_vals = ", ".join(f"S.{c}" for c in cols)
            update_set = ", ".join(f"T.{c} = S.{c}" for c in cols if c != "id")

            # Dedup staging by id (keep newest updated) — same pattern that
            # unblocked teller_sync (see .claude/rules/stack-gotchas.md).
            # Guard MATCHED with an updated freshness check so stale re-pulls
            # can't overwrite newer data.
            merge_sql = f"""
            MERGE `{SR_TABLE}` T
            USING (
                SELECT * EXCEPT(rn) FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY id
                        ORDER BY updated DESC NULLS LAST, synced_at DESC
                    ) AS rn
                    FROM `{staging}`
                ) WHERE rn = 1
            ) S
            ON T.id = S.id
            WHEN MATCHED AND (T.updated IS NULL OR (S.updated IS NOT NULL AND S.updated >= T.updated)) THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            merge_job = self.bq.query(merge_sql)
            merge_job.result()
            return merge_job.num_dml_affected_rows or 0
        finally:
            # Always drop the staging table, even if load/merge raised —
            # otherwise failed syncs leave orphaned `_staging_*` tables.
            try:
                self.bq.delete_table(staging, not_found_ok=True)
            except Exception as e:
                logger.warning("Could not delete staging %s: %s", staging, e)

    # ── Public API ───────────────────────────────────────────────────────

    def sync(self, force_updated_since: Optional[str] = None) -> Dict[str, Any]:
        """Run the incremental sync. Returns a summary dict."""
        start = datetime.now(timezone.utc)
        summary = SyncSummary(status="success")

        try:
            _, _, venue_group_id = self._get_credentials()

            if force_updated_since:
                updated_since = force_updated_since
            else:
                latest = self._latest_updated_in_bq()
                if latest:
                    dt = datetime.fromisoformat(latest.replace("Z", "+00:00"))
                    dt -= timedelta(minutes=LOOKBACK_MINUTES)
                    updated_since = dt.isoformat()
                else:
                    dt = start - timedelta(days=DEFAULT_INITIAL_LOOKBACK_DAYS)
                    updated_since = dt.isoformat()
            summary.updated_since = updated_since

            logger.info("SR sync starting from updated_since=%s", updated_since)
            reservations = self._fetch_reservations(updated_since, venue_group_id)
            synced_at = start.isoformat()
            rows = [
                r for r in
                (_reservation_to_row(res, synced_at) for res in reservations)
                if r is not None
            ]
            summary.reservations_pulled = len(reservations)
            summary.rows_merged = self._load_to_bq(rows)
            summary.latest_updated_in_bq = self._latest_updated_in_bq()
        except Exception as e:
            logger.exception("SR sync failed")
            summary = SyncSummary(status="error", error=str(e),
                                  updated_since=summary.updated_since)

        summary.duration_seconds = (datetime.now(timezone.utc) - start).total_seconds()
        self._send_slack_report(summary)
        return summary.to_dict()

    # ── Alerts ───────────────────────────────────────────────────────────

    def _send_slack_report(self, summary: SyncSummary) -> None:
        """Post a report to Slack. Escalates a successful sync to red when
        the latest reservation.updated is stale.
        """
        alert = AlertManager(slack_webhook=ALERT_WEBHOOK_URL)
        is_error = summary.status != "success"

        stale_hours: Optional[float] = None
        if not is_error and summary.latest_updated_in_bq:
            try:
                dt = datetime.fromisoformat(summary.latest_updated_in_bq.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                stale_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
                if stale_hours > STALE_HOURS:
                    is_error = True
            except (ValueError, TypeError):
                pass

        icon = "❌" if is_error else "✅"
        msg = f"{icon} *SevenRooms Sync — {date.today().strftime('%b %-d, %Y')}*\n\n"

        if stale_hours is not None and stale_hours > STALE_HOURS:
            msg += (
                f"⚠️ *STALE DATA* — latest reservation.updated is *{stale_hours:.1f} hrs old*. "
                f"Sync ran cleanly; SevenRooms is not delivering fresh updates.\n\n"
            )

        if summary.status == "success":
            msg += (
                f"*Status:* SUCCESS in {summary.duration_seconds:.1f}s\n"
                f"• Reservations pulled: *{summary.reservations_pulled}*\n"
                f"• Rows merged into BQ: *{summary.rows_merged}*\n"
                f"• updated_since window: {summary.updated_since}\n"
                f"• Latest updated in BQ: {summary.latest_updated_in_bq or '?'}\n"
            )
        else:
            msg += f"*Status:* FAILED\n*Error:* {summary.error or 'Unknown'}\n"

        alert.send_slack_alert(msg, is_error=is_error)
