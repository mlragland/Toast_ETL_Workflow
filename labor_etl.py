"""
Labor ETL — Load Toast time entries into BigQuery daily.

Pulls clock-in/out records from Toast Labor API and loads into
LaborTimeEntries_raw table. Runs as part of the daily ETL pipeline
or standalone for backfills.

Usage:
    # Load yesterday (default when called from pipeline)
    python labor_etl.py

    # Specific date
    python labor_etl.py --date 20260501

    # Date range backfill
    python labor_etl.py --start 20240304 --end 20260501

    # Dry run
    python labor_etl.py --date 20260501 --dry-run
"""
import argparse
import logging
import time
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from dateutil.parser import isoparse
from google.cloud import bigquery, secretmanager

from config import PROJECT_ID, DATASET_ID

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TOAST_API_BASE = "https://ws-api.toasttab.com"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.LaborTimeEntries_raw"
CLOSED_DAYS = {0}  # Mon only — LOV3 open Tue-Sun


def _get_secret(name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    resource = f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
    return client.access_secret_version(name=resource).payload.data.decode("UTF-8").strip()


class ToastLaborETL:
    """Pull time entries from Toast and load to BigQuery."""

    def __init__(self):
        self.client_id = _get_secret("toast-api-client-id")
        self.client_secret = _get_secret("toast-api-client-secret")
        self.restaurant_guid = _get_secret("toast-restaurant-guid")
        self._token: Optional[str] = None
        self._token_expires: float = 0
        self.employees: Dict[str, str] = {}
        self.jobs: Dict[str, str] = {}
        self.bq = bigquery.Client(project=PROJECT_ID)

    def authenticate(self):
        if self._token and time.time() < self._token_expires - 60:
            return
        resp = requests.post(
            f"{TOAST_API_BASE}/authentication/v1/authentication/login",
            headers={"Content-Type": "application/json"},
            json={
                "clientId": self.client_id,
                "clientSecret": self.client_secret,
                "userAccessType": "TOAST_MACHINE_CLIENT",
            },
        )
        resp.raise_for_status()
        data = resp.json()["token"]
        self._token = data["accessToken"]
        self._token_expires = time.time() + data.get("expiresIn", 3600)

    @property
    def headers(self) -> Dict[str, str]:
        self.authenticate()
        return {
            "Authorization": f"Bearer {self._token}",
            "Toast-Restaurant-External-ID": self.restaurant_guid,
        }

    def _get(self, path: str, params: dict = None) -> Any:
        resp = requests.get(f"{TOAST_API_BASE}{path}", headers=self.headers, params=params)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            log.warning("Rate limited — waiting %ds", wait)
            time.sleep(wait)
            return self._get(path, params)
        resp.raise_for_status()
        return resp.json()

    def load_lookups(self):
        """Cache employee and job name lookups."""
        log.info("Loading employee and job lookups...")
        emps = self._get("/labor/v1/employees")
        self.employees = {
            e["guid"]: f"{e.get('firstName', '')} {e.get('lastName', '')}".strip()
            for e in emps
        }
        jobs = self._get("/labor/v1/jobs")
        self.jobs = {j["guid"]: j.get("title", j.get("name", "")) for j in jobs}
        log.info("  %d employees, %d jobs", len(self.employees), len(self.jobs))

    def pull_and_load(self, business_date: str, dry_run: bool = False) -> int:
        """Pull time entries for one date and load to BigQuery.

        Args:
            business_date: YYYYMMDD format
            dry_run: if True, don't write to BQ

        Returns: number of rows loaded
        """
        entries = self._get("/labor/v1/timeEntries", {"businessDate": business_date})
        if not entries:
            log.info("  %s: no time entries", business_date)
            return 0

        processing_date = f"{business_date[:4]}-{business_date[4:6]}-{business_date[6:]}"

        rows = []
        for e in entries:
            emp_guid = (e.get("employeeReference") or {}).get("guid", "")
            job_guid = (e.get("jobReference") or {}).get("guid", "")

            # Parse timestamps — Toast returns ISO 8601 like "2026-05-06T00:41:34.836+0000".
            # isoparse handles +0000 (Python <3.11 fromisoformat does not); BQ TIMESTAMP
            # requires "YYYY-MM-DD HH:MM:SS[.ffffff]", so convert to UTC and drop the TZ suffix.
            in_date = e.get("inDate")
            out_date = e.get("outDate")
            clock_in = None
            clock_out = None
            if in_date:
                try:
                    clock_in = isoparse(in_date).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                except (ValueError, TypeError):
                    clock_in = in_date
            if out_date:
                try:
                    clock_out = isoparse(out_date).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                except (ValueError, TypeError):
                    clock_out = out_date

            rows.append({
                "guid": e.get("guid", ""),
                "employee_guid": emp_guid,
                "employee_name": self.employees.get(emp_guid, ""),
                "job_guid": job_guid,
                "job_title": self.jobs.get(job_guid, ""),
                "clock_in": clock_in,
                "clock_out": clock_out,
                "regular_hours": float(e.get("regularHours", 0) or 0),
                "overtime_hours": float(e.get("overtimeHours", 0) or 0),
                "hourly_wage": float(e.get("hourlyWage", 0) or 0),
                "cash_sales": float(e.get("cashSales", 0) or 0),
                "non_cash_sales": float(e.get("nonCashSales", 0) or 0),
                "non_cash_tips": float(e.get("nonCashTips", 0) or 0),
                "cash_gratuity_service_charges": float(e.get("cashGratuityServiceCharges", 0) or 0),
                "non_cash_gratuity_service_charges": float(e.get("nonCashGratuityServiceCharges", 0) or 0),
                "tips_withheld": float(e.get("tipsWithheld", 0) or 0),
                "declared_cash_tips": float(e.get("declaredCashTips") or 0),
                "auto_clocked_out": bool(e.get("autoClockedOut", False)),
                "deleted": bool(e.get("deleted", False)),
                "processing_date": processing_date,
            })

        if dry_run:
            log.info("  %s: %d entries (dry run — not loaded)", business_date, len(rows))
            return len(rows)

        # Check if data already exists for this date
        check_q = f"""
        SELECT COUNT(*) as cnt FROM `{TABLE_ID}`
        WHERE processing_date = @processing_date
        """
        config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("processing_date", "DATE", processing_date),
        ])
        existing = list(self.bq.query(check_q, job_config=config).result())[0].cnt

        if existing > 0:
            log.info("  %s: %d entries already exist — skipping (use --force to reload)",
                     business_date, existing)
            return 0

        # Insert rows via streaming
        errors = self.bq.insert_rows_json(TABLE_ID, rows)
        if errors:
            log.error("  %s: BQ insert errors: %s", business_date, errors[:3])
            return 0

        log.info("  %s: loaded %d entries", business_date, len(rows))
        return len(rows)

    def run(self, start_date: str, end_date: str, dry_run: bool = False) -> Dict:
        """Load time entries for a date range.

        Args:
            start_date: YYYYMMDD
            end_date: YYYYMMDD
            dry_run: if True, don't write to BQ

        Returns: summary dict
        """
        self.load_lookups()

        start = datetime.strptime(start_date, "%Y%m%d").date()
        end = datetime.strptime(end_date, "%Y%m%d").date()

        total_rows = 0
        dates_processed = 0
        dates_skipped = 0
        d = start

        while d <= end:
            if d.weekday() in CLOSED_DAYS:
                d += timedelta(days=1)
                dates_skipped += 1
                continue

            dt_str = d.strftime("%Y%m%d")
            try:
                rows = self.pull_and_load(dt_str, dry_run)
                total_rows += rows
                dates_processed += 1
            except Exception as e:
                log.error("  %s: failed — %s", dt_str, e)

            time.sleep(0.1)
            d += timedelta(days=1)

        log.info("Done: %d dates processed, %d rows loaded, %d skipped (closed)",
                 dates_processed, total_rows, dates_skipped)
        return {
            "dates_processed": dates_processed,
            "total_rows": total_rows,
            "dates_skipped": dates_skipped,
        }


def load_yesterday():
    """Load yesterday's time entries — called from daily ETL pipeline."""
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    etl = ToastLaborETL()
    return etl.run(yesterday, yesterday)


def main():
    parser = argparse.ArgumentParser(description="Load Toast labor time entries to BigQuery")
    parser.add_argument("--date", help="Single date YYYYMMDD")
    parser.add_argument("--start", help="Start date YYYYMMDD")
    parser.add_argument("--end", help="End date YYYYMMDD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.date:
        start = end = args.date
    elif args.start and args.end:
        start, end = args.start, args.end
    else:
        # Default: yesterday
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        start = end = yesterday

    etl = ToastLaborETL()
    result = etl.run(start, end, args.dry_run)
    print(f"\nDone: {result['dates_processed']} dates, {result['total_rows']} rows")


if __name__ == "__main__":
    main()
