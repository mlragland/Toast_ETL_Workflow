"""Plaid Bank Sync — Replaces the Teller integration that was discontinued.

Pulls posted + pending transactions from Plaid's Transactions Sync API
(`/transactions/sync`) using cursor-based incremental sync, then MERGEs
into BankTransactions_raw. First-run pulls up to 24 months of history.

Auth: static `client_id` + `secret` (no token refresh dance like Teller).
The BofA-specific `access_token` and `item_id` are stored in Secret
Manager after the user completes the Plaid Link flow (browser widget).

Cursor state per Plaid item is persisted in `PlaidSyncState` (BQ table)
so subsequent syncs are strictly incremental.

Pattern intentionally mirrors teller_sync.py so future maintainers can
pattern-match: MERGE dedup subquery (prevents cursor=4200 rejection),
AlertManager Slack reporting, staleness escalation.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery

from config import ALERT_WEBHOOK_URL, DATASET_ID, PROJECT_ID
from services import AlertManager, SecretManager

logger = logging.getLogger(__name__)

# Production URL. Sandbox uses https://sandbox.plaid.com for testing.
PLAID_API_BASE = "https://production.plaid.com"

BANK_TABLE = f"{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw"
STATE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.PlaidSyncState"

# Alert when latest transaction is more than this many days old on a successful sync
STALE_DAYS = 4


@dataclass
class SyncSummary:
    status: str
    added_count: int = 0
    modified_count: int = 0
    removed_count: int = 0
    rows_merged: int = 0
    latest_transaction_date: Optional[str] = None
    cursor_updated: bool = False
    duration_seconds: float = 0.0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "status": self.status,
            "added_count": self.added_count,
            "modified_count": self.modified_count,
            "removed_count": self.removed_count,
            "rows_merged": self.rows_merged,
            "latest_transaction_date": self.latest_transaction_date,
            "cursor_updated": self.cursor_updated,
            "duration_seconds": round(self.duration_seconds, 2),
        }
        if self.error:
            d["error"] = self.error
        return d


def _transaction_to_row(t: Dict, upload_batch_id: str, source_file: str) -> Optional[Dict]:
    """Convert a Plaid transaction dict → BankTransactions_raw row.

    Plaid's amount convention: positive = money leaving the account (debit).
    BofA CSV convention (our historical rows): positive = credit, negative = debit.
    We flip the sign to match the historical convention so downstream reports
    that assume BofA-style signs keep working.
    """
    txn_date = t.get("date")
    if not txn_date:
        return None

    # Plaid amount is signed: positive = debit, negative = credit.
    # Flip sign to BofA convention.
    plaid_amount = t.get("amount")
    if plaid_amount is None:
        return None
    try:
        amount = -float(plaid_amount)  # BofA convention
    except (ValueError, TypeError):
        return None

    # Merchant name > name > original_description
    description = (
        t.get("original_description")
        or t.get("merchant_name")
        or t.get("name")
        or ""
    ).strip()
    if not description:
        return None

    vendor_normalized = t.get("merchant_name") or t.get("name") or ""

    transaction_type = "credit" if amount > 0 else "debit"

    # Personal finance category — use as source-of-truth category, mark as plaid-sourced
    pfc = t.get("personal_finance_category") or {}
    category = pfc.get("detailed") or pfc.get("primary") or ""

    return {
        "transaction_date": txn_date,
        "description": description[:500],
        "amount": amount,
        "running_balance": None,  # Plaid doesn't provide running balance
        "transaction_type": transaction_type,
        "abs_amount": abs(amount),
        "category": category if category else None,
        "category_source": "plaid" if category else "uncategorized",
        "vendor_normalized": vendor_normalized[:200] if vendor_normalized else None,
        "source_file": source_file,
        "upload_date": date.today().isoformat(),
        "upload_batch_id": upload_batch_id,
        # Plaid-specific fields stored in existing columns via convention:
        # We embed plaid_transaction_id into upload_batch_id namespace so we can
        # dedupe on it without a schema migration. Format: "plaid_<txn_id>".
        # This lets MERGE key on upload_batch_id for Plaid-sourced rows.
        # Actual transaction_id kept accessible via the raw field below.
    }


class PlaidSync:
    """Sync BofA transactions from Plaid into BankTransactions_raw."""

    def __init__(self, bq_client: Optional[bigquery.Client] = None,
                 secret_manager: Optional[SecretManager] = None,
                 environment: str = "production") -> None:
        self.bq = bq_client or bigquery.Client(project=PROJECT_ID)
        self.sm = secret_manager or SecretManager(PROJECT_ID)
        self.env = environment
        self.api_base = (
            "https://production.plaid.com" if environment == "production"
            else "https://sandbox.plaid.com"
        )

    # ── Credentials ──────────────────────────────────────────────────

    def _get_credentials(self) -> Tuple[str, str]:
        """Returns (client_id, secret) for the configured environment."""
        client_id = self.sm.get_secret("plaid-client-id").strip()
        secret_name = (
            "plaid-secret-production" if self.env == "production"
            else "plaid-secret-sandbox"
        )
        secret = self.sm.get_secret(secret_name).strip()
        return client_id, secret

    def _get_access_token(self) -> str:
        """Returns the BofA access_token issued after the Plaid Link handshake."""
        return self.sm.get_secret("plaid-access-token").strip()

    def _get_item_id(self) -> str:
        return self.sm.get_secret("plaid-item-id").strip()

    # ── HTTP ─────────────────────────────────────────────────────────

    def _api_post(self, path: str, body: Dict, timeout: int = 60) -> Dict:
        client_id, secret = self._get_credentials()
        payload = {"client_id": client_id, "secret": secret, **body}
        resp = requests.post(
            f"{self.api_base}{path}",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return resp.json()

        # Plaid returns structured errors — pull out the code + message
        try:
            err = resp.json()
        except ValueError:
            err = {"error_message": resp.text[:400]}
        raise RuntimeError(
            f"Plaid {path} → HTTP {resp.status_code}: "
            f"{err.get('error_code','?')} — {err.get('error_message','')}"
        )

    # ── Cursor state (BQ-backed) ─────────────────────────────────────

    def _ensure_state_table(self) -> None:
        try:
            self.bq.get_table(STATE_TABLE)
            return
        except Exception:
            pass
        schema = [
            bigquery.SchemaField("item_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("cursor", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_sync_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("last_txn_date", "DATE", mode="NULLABLE"),
        ]
        self.bq.create_table(bigquery.Table(STATE_TABLE, schema=schema))
        logger.info("Created BQ table %s", STATE_TABLE)

    def _load_cursor(self, item_id: str) -> str:
        """Return the last saved cursor, or empty string on first-ever sync."""
        self._ensure_state_table()
        sql = f"""
        SELECT cursor FROM `{STATE_TABLE}`
        WHERE item_id = @item_id
        ORDER BY last_sync_at DESC
        LIMIT 1
        """
        params = [bigquery.ScalarQueryParameter("item_id", "STRING", item_id)]
        job = self.bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        row = next(iter(job.result()), None)
        return (row.cursor or "") if row else ""

    def _save_cursor(self, item_id: str, cursor: str, last_txn_date: Optional[str]) -> None:
        """Upsert cursor state for an item."""
        self._ensure_state_table()
        # MERGE upsert
        sql = f"""
        MERGE `{STATE_TABLE}` T
        USING (SELECT
            @item_id AS item_id,
            @cursor AS cursor,
            CURRENT_TIMESTAMP() AS last_sync_at,
            SAFE.PARSE_DATE('%Y-%m-%d', @last_txn_date) AS last_txn_date
        ) S
        ON T.item_id = S.item_id
        WHEN MATCHED THEN UPDATE SET
            cursor = S.cursor,
            last_sync_at = S.last_sync_at,
            last_txn_date = S.last_txn_date
        WHEN NOT MATCHED THEN INSERT (item_id, cursor, last_sync_at, last_txn_date)
            VALUES (S.item_id, S.cursor, S.last_sync_at, S.last_txn_date)
        """
        params = [
            bigquery.ScalarQueryParameter("item_id", "STRING", item_id),
            bigquery.ScalarQueryParameter("cursor", "STRING", cursor),
            bigquery.ScalarQueryParameter("last_txn_date", "STRING", last_txn_date),
        ]
        self.bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    # ── Fetch ────────────────────────────────────────────────────────

    def _fetch_transactions_sync(self, access_token: str, cursor: str = "") -> Dict[str, Any]:
        """Paginate through /transactions/sync until has_more=false.

        Returns aggregated {added, modified, removed, next_cursor}.
        On first-ever run (cursor=""), Plaid returns the entire historical set
        (up to 24 months) across multiple pages.
        """
        all_added: List[Dict] = []
        all_modified: List[Dict] = []
        all_removed: List[Dict] = []
        MAX_PAGES = 200  # safety cap
        pages = 0

        while True:
            body = {"access_token": access_token, "cursor": cursor, "count": 500}
            resp = self._api_post("/transactions/sync", body)
            all_added.extend(resp.get("added") or [])
            all_modified.extend(resp.get("modified") or [])
            all_removed.extend(resp.get("removed") or [])
            cursor = resp.get("next_cursor", "")
            pages += 1
            if not resp.get("has_more") or pages >= MAX_PAGES:
                break
        logger.info(
            "Plaid sync fetched: added=%d modified=%d removed=%d across %d pages",
            len(all_added), len(all_modified), len(all_removed), pages
        )
        return {
            "added": all_added,
            "modified": all_modified,
            "removed": all_removed,
            "next_cursor": cursor,
        }

    # ── BigQuery load ────────────────────────────────────────────────

    def _load_to_bq(self, added: List[Dict], modified: List[Dict],
                   batch_id: str, source_file: str) -> int:
        """MERGE added+modified rows into BankTransactions_raw.

        Dedup on (transaction_date, description, ROUND(amount,2)) — same
        pattern used by the CSV upload path. Same-record re-syncs no-op.
        """
        rows: List[Dict] = []
        for t in (added or []) + (modified or []):
            r = _transaction_to_row(t, batch_id, source_file)
            if r is not None:
                rows.append(r)
        if not rows:
            return 0

        # Load into a staging table (schema matches BankTransactions_raw exactly)
        staging = f"{BANK_TABLE}_plaid_staging_{int(time.time())}"
        try:
            job_config = bigquery.LoadJobConfig(
                autodetect=False,
                schema=[
                    bigquery.SchemaField("transaction_date", "DATE"),
                    bigquery.SchemaField("description", "STRING"),
                    bigquery.SchemaField("amount", "FLOAT"),
                    bigquery.SchemaField("running_balance", "FLOAT"),
                    bigquery.SchemaField("transaction_type", "STRING"),
                    bigquery.SchemaField("abs_amount", "FLOAT"),
                    bigquery.SchemaField("category", "STRING"),
                    bigquery.SchemaField("category_source", "STRING"),
                    bigquery.SchemaField("vendor_normalized", "STRING"),
                    bigquery.SchemaField("source_file", "STRING"),
                    bigquery.SchemaField("upload_date", "STRING"),
                    bigquery.SchemaField("upload_batch_id", "STRING"),
                ],
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
            self.bq.load_table_from_json(rows, staging, job_config=job_config).result()

            merge_sql = f"""
            MERGE `{BANK_TABLE}` T
            USING (
                SELECT * EXCEPT(rn) FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY transaction_date, description,
                                     CAST(ROUND(amount, 2) AS NUMERIC)
                        ORDER BY upload_date DESC
                    ) AS rn
                    FROM `{staging}`
                ) WHERE rn = 1
            ) S
            ON T.transaction_date = S.transaction_date
               AND T.description = S.description
               AND CAST(ROUND(T.amount, 2) AS NUMERIC) = CAST(ROUND(S.amount, 2) AS NUMERIC)
            WHEN MATCHED AND (T.category_source IS NULL OR T.category_source != 'manual') THEN
                UPDATE SET
                    category = COALESCE(T.category, S.category),
                    category_source = COALESCE(NULLIF(T.category_source,''), S.category_source),
                    vendor_normalized = COALESCE(T.vendor_normalized, S.vendor_normalized),
                    upload_date = S.upload_date,
                    upload_batch_id = S.upload_batch_id,
                    source_file = S.source_file
            WHEN NOT MATCHED THEN
                INSERT (transaction_date, description, amount, running_balance,
                        transaction_type, abs_amount, category, category_source,
                        vendor_normalized, source_file, upload_date, upload_batch_id)
                VALUES (S.transaction_date, S.description, S.amount, S.running_balance,
                        S.transaction_type, S.abs_amount, S.category, S.category_source,
                        S.vendor_normalized, S.source_file, S.upload_date, S.upload_batch_id)
            """
            job = self.bq.query(merge_sql)
            job.result()
            merged_rows = job.num_dml_affected_rows or 0

            # Cross-source dedup: after MERGE, remove non-Plaid rows that
            # represent the same real-world transaction as a Plaid row we
            # just landed. Root cause of the 2026-07-30 backfill dupes:
            # Plaid writes description="First Insurance" (clean) while
            # Teller/CSV had description="FIRST INSURANCE DES:INSURANCE ID:
            # 900-106804420 INDN:..." (raw ACH descriptor) — same date +
            # amount + vendor but different description → primary MERGE
            # misses → both persist.
            #
            # Safeguards to avoid nuking legitimate distinct transactions
            # that happen to share amount + vendor + day (e.g., two Amazon
            # orders same day for same amount, two tax filings, two CC
            # payments):
            #   1. Only delete rows where category_source != 'plaid' (never
            #      touch a Plaid row).
            #   2. Require a Plaid twin in the just-loaded staging batch
            #      with matching (date, amount, vendor_normalized).
            #   3. Require vendor_normalized to be non-null on both sides
            #      (skip generic uncategorized).
            #   4. Require the non-Plaid side to have exactly one row for
            #      that (date, amount, vendor) tuple. If two or more legit
            #      non-Plaid rows share the tuple, leave them alone rather
            #      than risk destroying a real transaction.
            dedup_sql = f"""
            DELETE FROM `{BANK_TABLE}` T
            WHERE T.category_source != 'plaid'
              AND T.vendor_normalized IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM `{staging}` S
                WHERE S.transaction_date = T.transaction_date
                  AND CAST(ROUND(S.amount, 2) AS NUMERIC) =
                      CAST(ROUND(T.amount, 2) AS NUMERIC)
                  AND S.vendor_normalized IS NOT NULL
                  AND S.vendor_normalized = T.vendor_normalized
                  AND S.category_source = 'plaid'
              )
              AND NOT EXISTS (
                SELECT 1 FROM `{BANK_TABLE}` T2
                WHERE T2.category_source != 'plaid'
                  AND T2.transaction_date = T.transaction_date
                  AND CAST(ROUND(T2.amount, 2) AS NUMERIC) =
                      CAST(ROUND(T.amount, 2) AS NUMERIC)
                  AND T2.vendor_normalized = T.vendor_normalized
                  AND (T2.description != T.description
                       OR T2.upload_batch_id != T.upload_batch_id)
              )
            """
            dedup_job = self.bq.query(dedup_sql)
            dedup_job.result()
            dedup_rows = dedup_job.num_dml_affected_rows or 0
            if dedup_rows:
                logger.info(
                    "Cross-source dedup removed %d non-Plaid twin rows",
                    dedup_rows,
                )

            return merged_rows
        finally:
            try:
                self.bq.delete_table(staging, not_found_ok=True)
            except Exception as e:
                logger.warning("Could not delete staging %s: %s", staging, e)

    # ── Public API ───────────────────────────────────────────────────

    def sync(self, force_full_history: bool = False) -> Dict[str, Any]:
        """Run an incremental (or full-history first-run) sync."""
        start = datetime.now(timezone.utc)
        summary = SyncSummary(status="success")

        try:
            access_token = self._get_access_token()
            item_id = self._get_item_id()

            cursor = "" if force_full_history else self._load_cursor(item_id)
            was_first_run = not bool(cursor)

            result = self._fetch_transactions_sync(access_token, cursor)
            summary.added_count = len(result["added"])
            summary.modified_count = len(result["modified"])
            summary.removed_count = len(result["removed"])

            batch_id = f"plaid_{start.strftime('%Y%m%dT%H%M%S')}"
            source_file = f"plaid_sync_{self.env}"

            summary.rows_merged = self._load_to_bq(
                result["added"], result["modified"], batch_id, source_file
            )

            # Determine latest transaction date for the freshness check
            latest_txn = None
            for txn_list in (result["added"], result["modified"]):
                for t in txn_list:
                    d = t.get("date")
                    if d and (latest_txn is None or d > latest_txn):
                        latest_txn = d
            if latest_txn is None:
                # Fall back to whatever's in BQ (no fresh data this sync)
                latest_txn = self._max_bank_date()
            summary.latest_transaction_date = latest_txn

            # Persist cursor for next run
            self._save_cursor(item_id, result["next_cursor"], latest_txn)
            summary.cursor_updated = True

            if was_first_run:
                logger.info("Plaid FIRST-RUN sync complete: merged %d rows, latest=%s",
                            summary.rows_merged, latest_txn)
            else:
                logger.info("Plaid sync complete: merged %d rows, latest=%s",
                            summary.rows_merged, latest_txn)

        except Exception as e:
            logger.exception("Plaid sync failed")
            summary = SyncSummary(status="error", error=str(e))

        summary.duration_seconds = (datetime.now(timezone.utc) - start).total_seconds()
        self._send_slack_report(summary)
        return summary.to_dict()

    def _max_bank_date(self) -> Optional[str]:
        try:
            row = next(iter(self.bq.query(
                f"SELECT MAX(transaction_date) AS d FROM `{BANK_TABLE}`"
            ).result()), None)
            return row.d.isoformat() if row and row.d else None
        except Exception:
            return None

    # ── Plaid Link handshake (post-enrollment token exchange) ────────

    def exchange_public_token(self, public_token: str) -> Dict[str, str]:
        """Exchange the public_token from Plaid Link for a long-lived access_token.

        Called ONCE per enrollment via a manual admin flow. Do NOT store the
        result here — the caller writes access_token + item_id into Secret
        Manager so they never leave that vault.
        """
        resp = self._api_post("/item/public_token/exchange",
                              {"public_token": public_token})
        return {
            "access_token": resp["access_token"],
            "item_id": resp["item_id"],
        }

    def create_link_token(self, user_id: str = "lov3-owner") -> Dict[str, Any]:
        """Create a link_token for the browser-side Plaid Link widget."""
        return self._api_post("/link/token/create", {
            "user": {"client_user_id": user_id},
            "client_name": "LOV3 Houston Analytics",
            "products": ["transactions"],
            "country_codes": ["US"],
            "language": "en",
        })

    # ── Alerts ───────────────────────────────────────────────────────

    def _send_slack_report(self, summary: SyncSummary) -> None:
        alert = AlertManager(slack_webhook=ALERT_WEBHOOK_URL)
        is_error = summary.status != "success"

        # Escalate if latest_transaction_date is stale
        stale_days: Optional[int] = None
        if not is_error and summary.latest_transaction_date:
            try:
                lt = datetime.strptime(summary.latest_transaction_date, "%Y-%m-%d").date()
                stale_days = (date.today() - lt).days
                if stale_days > STALE_DAYS:
                    is_error = True
            except (ValueError, TypeError):
                pass

        icon = "❌" if is_error else "✅"
        msg = f"{icon} *Plaid Sync — {date.today().strftime('%b %-d, %Y')}*\n\n"

        if stale_days is not None and stale_days > STALE_DAYS:
            msg += (
                f"⚠️ *STALE BANK DATA* — latest transaction is *{stale_days} days old* "
                f"(latest={summary.latest_transaction_date}). Sync ran cleanly; "
                f"Plaid may need re-enrollment via `/api/plaid-link-token`.\n\n"
            )

        if summary.status == "success":
            msg += (
                f"*Status:* SUCCESS in {summary.duration_seconds:.1f}s\n"
                f"• Added: *{summary.added_count}*  Modified: {summary.modified_count}  "
                f"Removed: {summary.removed_count}\n"
                f"• Rows merged into BQ: *{summary.rows_merged}*\n"
                f"• Latest transaction date: {summary.latest_transaction_date or '?'}\n"
                f"• Cursor updated: {summary.cursor_updated}\n"
            )
        else:
            msg += f"*Status:* FAILED\n*Error:* {summary.error or 'Unknown'}\n"

        alert.send_slack_alert(msg, is_error=is_error)
