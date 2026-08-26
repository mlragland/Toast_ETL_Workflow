#!/usr/bin/env python3
"""
HotSchedules Schedule-Adherence Pipeline

Pulls scheduled shifts from HotSchedules SFTP (Fourth's General Purpose File
Import format v2.5 — shifts_YYYYMMDD_HHMMSS.txt), loads to BigQuery, and
joins against Toast LaborTimeEntries_raw to produce a scheduled-vs-actual
adherence report for LOV3 HTX.

Credentials come from Google Secret Manager (never CLI args or env files):
    hotschedules-sftp-username    -> LOV3Restaurant
    hotschedules-sftp-password
    hotschedules-company-id       -> 964327747
    hotschedules-concept-id       -> 3751
    hotschedules-store-id         -> 2900

BQ objects created (idempotent):
    toast_raw.HotSchedules_scheduled_shifts_raw
    toast_raw.hotschedules_employee_map        (hs_emp_id <-> toast guid)

Usage:
    # 1) One-time table setup + poll SFTP for new files + load to BQ
    python hotschedules_pull.py --sync

    # 2) Prior-week adherence report (defaults to last full Mon-Sun)
    python hotschedules_pull.py --adherence-report

    # 3) Explicit date range
    python hotschedules_pull.py --adherence-report --start 20260811 --end 20260817

    # 4) Also write CSV alongside the console output
    python hotschedules_pull.py --adherence-report --csv

    # 5) End-to-end verification with a synthetic sample file
    python hotschedules_pull.py --dry-run-with-sample
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import logging
import os
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

try:
    from config import PROJECT_ID  # type: ignore
except ImportError:
    PROJECT_ID = "toast-analytics-444116"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SFTP_HOST = "hs-sftp.hotschedules.com"
SFTP_PORT = 22
SFTP_EXPORT_DIR = "datastore/Export"
SFTP_TEST_DIR = "datastore/Test"

BQ_DATASET = "toast_raw"
BQ_SHIFTS_TABLE = "HotSchedules_scheduled_shifts_raw"
BQ_EMP_MAP_TABLE = "hotschedules_employee_map"

CHICAGO_TZ = "America/Chicago"

# Pipe-delimited column order from HotSchedules v2.5 spec, Shift Data Export.
SHIFT_COLUMNS = [
    "CompanyNum", "ConceptNum", "StoreNum",
    "BusinessDate", "WorkweekStart", "WorkweekEnd",
    "LocationID", "LocationName",
    "EmpID", "EmpFirstName", "EmpLastName",
    "JobCode", "JobName",
    "Rate", "OTRate",
    "ShiftStart", "ShiftEnd",
    "RegularMinutes", "OTMinutes",
    "RegularPay", "OTPay", "SpecialPay",
]


# ---------------------------------------------------------------------------
# Secret Manager helpers
# ---------------------------------------------------------------------------

def _secret(name: str) -> str:
    """Read the latest version of a Secret Manager secret."""
    return subprocess.check_output(
        ["gcloud", "secrets", "versions", "access", "latest",
         "--secret", name, "--project", PROJECT_ID],
        text=True,
    ).strip()


# ---------------------------------------------------------------------------
# SFTP client
# ---------------------------------------------------------------------------

class HotSchedulesSFTP:
    """Thin paramiko wrapper for the HotSchedules SFTP endpoint."""

    def __init__(self):
        import paramiko
        self._paramiko = paramiko
        self._transport: Optional[paramiko.Transport] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    def __enter__(self):
        self._transport = self._paramiko.Transport((SFTP_HOST, SFTP_PORT))
        self._transport.banner_timeout = 20
        self._transport.connect(
            username=_secret("hotschedules-sftp-username"),
            password=_secret("hotschedules-sftp-password"),
        )
        self._sftp = self._paramiko.SFTPClient.from_transport(self._transport)
        return self

    def __exit__(self, *exc):
        try:
            if self._sftp:
                self._sftp.close()
        finally:
            if self._transport:
                self._transport.close()

    def list_export_files(self, prefix: str = "shifts_") -> list[tuple[str, int, int]]:
        """Return [(filename, size, mtime), ...] for export files matching prefix."""
        out = []
        for entry in self._sftp.listdir_attr(SFTP_EXPORT_DIR):
            if stat.S_ISDIR(entry.st_mode):
                continue
            if entry.filename.startswith(prefix):
                out.append((entry.filename, entry.st_size, entry.st_mtime))
        return sorted(out)

    def download(self, remote_name: str, local_path: str, remote_dir: str = SFTP_EXPORT_DIR) -> None:
        self._sftp.get(f"{remote_dir}/{remote_name}", local_path)

    def upload(self, local_path: str, remote_name: str, remote_dir: str = SFTP_TEST_DIR) -> None:
        self._sftp.put(local_path, f"{remote_dir}/{remote_name}")


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

@dataclass
class ShiftRow:
    company_num: int
    concept_num: int
    store_num: int
    business_date: date
    workweek_start: Optional[date]
    workweek_end: Optional[date]
    location_id: Optional[int]
    location_name: str
    hs_emp_id: int
    emp_first_name: str
    emp_last_name: str
    job_code: Optional[int]
    job_name: str
    rate: float
    ot_rate: float
    shift_start: datetime
    shift_end: datetime
    regular_minutes: int
    ot_minutes: int
    regular_pay: float
    ot_pay: float
    special_pay: float
    source_file: str
    file_mtime: datetime
    ingested_at: datetime


def _pdate(s: str) -> Optional[date]:
    s = s.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    log.warning("Unparseable date: %r", s)
    return None


def _pdt(s: str) -> Optional[datetime]:
    s = s.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    log.warning("Unparseable timestamp: %r", s)
    return None


def _pint(s: str) -> Optional[int]:
    s = s.strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _pfloat(s: str) -> float:
    s = s.strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_shifts_file(path: str, source_file: str, file_mtime: datetime) -> list[ShiftRow]:
    """Parse a pipe-delimited shifts export per v2.5 spec.

    Skips header-like rows (non-numeric CompanyNum) — spec allows N header rows.
    """
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[ShiftRow] = []
    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.rstrip("\r\n")
            if not line.strip():
                continue
            parts = line.split("|")
            if len(parts) < len(SHIFT_COLUMNS):
                log.warning("%s:%d expected >=%d cols, got %d — skipping",
                            source_file, lineno, len(SHIFT_COLUMNS), len(parts))
                continue
            company = _pint(parts[0])
            if company is None:
                # header row
                continue
            biz = _pdate(parts[3])
            if biz is None:
                log.warning("%s:%d missing BusinessDate — skipping", source_file, lineno)
                continue
            rows.append(ShiftRow(
                company_num=company,
                concept_num=_pint(parts[1]) or 0,
                store_num=_pint(parts[2]) or 0,
                business_date=biz,
                workweek_start=_pdate(parts[4]),
                workweek_end=_pdate(parts[5]),
                location_id=_pint(parts[6]),
                location_name=parts[7].strip(),
                hs_emp_id=_pint(parts[8]) or 0,
                emp_first_name=parts[9].strip(),
                emp_last_name=parts[10].strip(),
                job_code=_pint(parts[11]),
                job_name=parts[12].strip(),
                rate=_pfloat(parts[13]),
                ot_rate=_pfloat(parts[14]),
                shift_start=_pdt(parts[15]) or datetime.combine(biz, datetime.min.time()),
                shift_end=_pdt(parts[16]) or datetime.combine(biz, datetime.min.time()),
                regular_minutes=_pint(parts[17]) or 0,
                ot_minutes=_pint(parts[18]) or 0,
                regular_pay=_pfloat(parts[19]),
                ot_pay=_pfloat(parts[20]),
                special_pay=_pfloat(parts[21]),
                source_file=source_file,
                file_mtime=file_mtime,
                ingested_at=ingested_at,
            ))
    return rows


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

def _bq_client():
    from google.cloud import bigquery
    return bigquery.Client(project=PROJECT_ID)


def ensure_tables() -> None:
    """Create the shifts + employee-map tables if they don't already exist."""
    from google.cloud import bigquery
    bq = _bq_client()

    shifts_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BQ_DATASET}.{BQ_SHIFTS_TABLE}` (
      company_num INT64,
      concept_num INT64,
      store_num INT64,
      business_date DATE,
      workweek_start DATE,
      workweek_end DATE,
      location_id INT64,
      location_name STRING,
      hs_emp_id INT64,
      emp_first_name STRING,
      emp_last_name STRING,
      job_code INT64,
      job_name STRING,
      rate FLOAT64,
      ot_rate FLOAT64,
      shift_start TIMESTAMP,
      shift_end TIMESTAMP,
      regular_minutes INT64,
      ot_minutes INT64,
      regular_pay FLOAT64,
      ot_pay FLOAT64,
      special_pay FLOAT64,
      source_file STRING,
      file_mtime TIMESTAMP,
      ingested_at TIMESTAMP
    )
    PARTITION BY business_date
    CLUSTER BY hs_emp_id, source_file
    """
    map_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BQ_DATASET}.{BQ_EMP_MAP_TABLE}` (
      hs_emp_id INT64,
      toast_employee_guid STRING,
      toast_external_employee_id STRING,
      employee_name_hs STRING,
      employee_name_toast STRING,
      match_method STRING,
      updated_at TIMESTAMP
    )
    CLUSTER BY hs_emp_id
    """
    for ddl in (shifts_ddl, map_ddl):
        bq.query(ddl).result()
    log.info("BQ tables verified: %s, %s", BQ_SHIFTS_TABLE, BQ_EMP_MAP_TABLE)


def already_ingested(source_file: str) -> bool:
    """Idempotency check — has this filename been loaded before?"""
    bq = _bq_client()
    from google.cloud import bigquery
    q = f"""
        SELECT COUNT(*) AS n
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_SHIFTS_TABLE}`
        WHERE source_file = @f
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("f", "STRING", source_file)]))
    return next(iter(job.result())).n > 0


def load_shifts(rows: list[ShiftRow]) -> int:
    """Stream rows into the shifts table. Returns row count loaded."""
    if not rows:
        return 0
    bq = _bq_client()
    from google.cloud import bigquery
    table = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_SHIFTS_TABLE}"
    payload = [{
        "company_num": r.company_num,
        "concept_num": r.concept_num,
        "store_num": r.store_num,
        "business_date": r.business_date.isoformat(),
        "workweek_start": r.workweek_start.isoformat() if r.workweek_start else None,
        "workweek_end": r.workweek_end.isoformat() if r.workweek_end else None,
        "location_id": r.location_id,
        "location_name": r.location_name,
        "hs_emp_id": r.hs_emp_id,
        "emp_first_name": r.emp_first_name,
        "emp_last_name": r.emp_last_name,
        "job_code": r.job_code,
        "job_name": r.job_name,
        "rate": r.rate,
        "ot_rate": r.ot_rate,
        "shift_start": r.shift_start.isoformat(),
        "shift_end": r.shift_end.isoformat(),
        "regular_minutes": r.regular_minutes,
        "ot_minutes": r.ot_minutes,
        "regular_pay": r.regular_pay,
        "ot_pay": r.ot_pay,
        "special_pay": r.special_pay,
        "source_file": r.source_file,
        "file_mtime": r.file_mtime.isoformat(),
        "ingested_at": r.ingested_at.isoformat(),
    } for r in rows]
    errors = bq.insert_rows_json(table, payload)
    if errors:
        raise RuntimeError(f"BQ insert errors: {errors[:3]}")
    return len(payload)


def refresh_employee_map() -> int:
    """Auto-populate hs_emp_id -> toast_guid via exact case-insensitive name match.

    Populates only unmapped hs_emp_ids. Manual overrides (match_method='manual')
    are preserved. Returns rows inserted/updated.
    """
    bq = _bq_client()
    q = f"""
    MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_EMP_MAP_TABLE}` T
    USING (
      WITH hs_names AS (
        SELECT DISTINCT
          hs_emp_id,
          LOWER(TRIM(CONCAT(emp_first_name, ' ', emp_last_name))) AS name_key,
          MAX(CONCAT(emp_first_name, ' ', emp_last_name)) OVER (PARTITION BY hs_emp_id) AS name_hs
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_SHIFTS_TABLE}`
      ),
      toast_names AS (
        SELECT DISTINCT
          employee_guid,
          employee_name AS name_toast,
          LOWER(TRIM(employee_name)) AS name_key
        FROM `{PROJECT_ID}.{BQ_DATASET}.LaborTimeEntries_raw`
        WHERE employee_name IS NOT NULL AND NOT deleted
      )
      SELECT h.hs_emp_id, t.employee_guid, h.name_hs, t.name_toast
      FROM hs_names h
      JOIN toast_names t USING (name_key)
    ) S
    ON T.hs_emp_id = S.hs_emp_id
    WHEN NOT MATCHED THEN
      INSERT (hs_emp_id, toast_employee_guid, employee_name_hs, employee_name_toast,
              match_method, updated_at)
      VALUES (S.hs_emp_id, S.employee_guid, S.name_hs, S.name_toast,
              'auto_name_exact', CURRENT_TIMESTAMP())
    WHEN MATCHED AND T.match_method != 'manual' AND T.toast_employee_guid IS NULL THEN
      UPDATE SET toast_employee_guid = S.employee_guid,
                 employee_name_toast = S.name_toast,
                 match_method = 'auto_name_exact',
                 updated_at = CURRENT_TIMESTAMP()
    """
    result = bq.query(q).result()
    log.info("Employee map refreshed (name-exact auto-match)")
    return getattr(result, "num_dml_affected_rows", 0) or 0


# ---------------------------------------------------------------------------
# Adherence report
# ---------------------------------------------------------------------------

ADHERENCE_SQL = f"""
DECLARE d_start DATE DEFAULT @d_start;
DECLARE d_end   DATE DEFAULT @d_end;

WITH scheduled AS (
  SELECT
    business_date,
    hs_emp_id,
    TRIM(CONCAT(emp_first_name, ' ', emp_last_name)) AS scheduled_name,
    job_name AS scheduled_job,
    shift_start AS scheduled_start,
    shift_end   AS scheduled_end,
    regular_minutes AS scheduled_minutes,
    regular_pay + ot_pay + special_pay AS scheduled_pay
  FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_SHIFTS_TABLE}`
  WHERE business_date BETWEEN d_start AND d_end
    AND source_file NOT LIKE '%_SAMPLE.txt'
),
actual AS (
  SELECT
    DATE(clock_in, '{CHICAGO_TZ}') AS business_date,
    employee_guid,
    employee_name,
    job_title,
    clock_in,
    clock_out,
    CAST(TIMESTAMP_DIFF(clock_out, clock_in, MINUTE) AS INT64) AS worked_minutes,
    regular_hours, overtime_hours
  FROM `{PROJECT_ID}.{BQ_DATASET}.LaborTimeEntries_raw`
  WHERE DATE(clock_in, '{CHICAGO_TZ}') BETWEEN d_start AND d_end
    AND clock_in IS NOT NULL AND clock_out IS NOT NULL
    AND NOT COALESCE(deleted, FALSE)
),
map AS (
  SELECT hs_emp_id, toast_employee_guid
  FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_EMP_MAP_TABLE}`
  WHERE toast_employee_guid IS NOT NULL
),
sched_matched AS (
  SELECT
    s.*,
    m.toast_employee_guid,
    a.employee_name AS actual_name,
    a.job_title     AS actual_job,
    a.clock_in, a.clock_out, a.worked_minutes,
    CASE WHEN a.clock_in IS NOT NULL
         THEN CAST(TIMESTAMP_DIFF(a.clock_in, s.scheduled_start, MINUTE) AS INT64) END AS late_by_min,
    CASE WHEN a.clock_out IS NOT NULL
         THEN CAST(TIMESTAMP_DIFF(s.scheduled_end, a.clock_out, MINUTE) AS INT64) END AS early_by_min
  FROM scheduled s
  LEFT JOIN map m ON s.hs_emp_id = m.hs_emp_id
  LEFT JOIN actual a
    ON a.employee_guid = m.toast_employee_guid
   AND a.business_date = s.business_date
   AND ABS(TIMESTAMP_DIFF(a.clock_in, s.scheduled_start, HOUR)) <= 6
)
SELECT
  business_date,
  scheduled_name,
  scheduled_job,
  FORMAT_TIMESTAMP('%a %m/%d %I:%M %p', scheduled_start, '{CHICAGO_TZ}') AS scheduled_in_ct,
  FORMAT_TIMESTAMP('%I:%M %p',           scheduled_end,   '{CHICAGO_TZ}') AS scheduled_out_ct,
  FORMAT_TIMESTAMP('%I:%M %p',           clock_in,        '{CHICAGO_TZ}') AS actual_in_ct,
  FORMAT_TIMESTAMP('%I:%M %p',           clock_out,       '{CHICAGO_TZ}') AS actual_out_ct,
  scheduled_minutes,
  COALESCE(worked_minutes, 0) AS worked_minutes,
  COALESCE(worked_minutes, 0) - scheduled_minutes AS delta_minutes,
  CASE
    WHEN clock_in IS NULL THEN 'NO SHOW'
    WHEN late_by_min > 10 THEN CONCAT('LATE ', CAST(late_by_min AS STRING), 'm')
    WHEN late_by_min < -10 THEN CONCAT('EARLY IN ', CAST(-late_by_min AS STRING), 'm')
    WHEN early_by_min > 10 THEN CONCAT('EARLY OUT ', CAST(early_by_min AS STRING), 'm')
    WHEN early_by_min < -10 THEN CONCAT('LATE OUT ', CAST(-early_by_min AS STRING), 'm')
    ELSE 'ON TIME'
  END AS adherence,
  toast_employee_guid IS NULL AS unmapped_employee
FROM sched_matched
ORDER BY business_date, scheduled_start, scheduled_name
"""

UNSCHEDULED_SQL = f"""
DECLARE d_start DATE DEFAULT @d_start;
DECLARE d_end   DATE DEFAULT @d_end;

WITH actual AS (
  SELECT
    DATE(clock_in, '{CHICAGO_TZ}') AS business_date,
    employee_guid, employee_name, job_title,
    clock_in, clock_out,
    CAST(TIMESTAMP_DIFF(clock_out, clock_in, MINUTE) AS INT64) AS worked_minutes
  FROM `{PROJECT_ID}.{BQ_DATASET}.LaborTimeEntries_raw`
  WHERE DATE(clock_in, '{CHICAGO_TZ}') BETWEEN d_start AND d_end
    AND clock_in IS NOT NULL AND clock_out IS NOT NULL
    AND NOT COALESCE(deleted, FALSE)
),
map AS (
  SELECT hs_emp_id, toast_employee_guid
  FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_EMP_MAP_TABLE}`
  WHERE toast_employee_guid IS NOT NULL
),
scheduled_key AS (
  SELECT DISTINCT m.toast_employee_guid, s.business_date
  FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_SHIFTS_TABLE}` s
  JOIN map m ON s.hs_emp_id = m.hs_emp_id
  WHERE s.business_date BETWEEN d_start AND d_end
    AND s.source_file NOT LIKE '%_SAMPLE.txt'
)
SELECT
  a.business_date,
  a.employee_name,
  a.job_title,
  FORMAT_TIMESTAMP('%a %m/%d %I:%M %p', a.clock_in,  '{CHICAGO_TZ}') AS clocked_in,
  FORMAT_TIMESTAMP('%I:%M %p',          a.clock_out, '{CHICAGO_TZ}') AS clocked_out,
  a.worked_minutes
FROM actual a
LEFT JOIN scheduled_key s
  ON s.toast_employee_guid = a.employee_guid
 AND s.business_date       = a.business_date
WHERE s.toast_employee_guid IS NULL
ORDER BY a.business_date, a.clock_in
"""


def _prior_lov3_workweek() -> tuple[date, date]:
    """Return (Fri, Thu) for LOV3's most recently completed HotSchedules workweek.

    LOV3's scheduling workweek runs Friday through Thursday (industry norm to
    keep the busy Fri/Sat nights inside one labor-cost window). Confirmed
    2026-08-25 from HS ASC → Reports → Manager's Schedule.
    """
    today = date.today()
    # Python weekday: Mon=0 ... Fri=4 ... Sun=6
    days_since_friday = (today.weekday() - 4) % 7  # 0 if today is Fri
    this_week_fri = today - timedelta(days=days_since_friday)
    prior_fri = this_week_fri - timedelta(days=7)
    prior_thu = prior_fri + timedelta(days=6)
    return prior_fri, prior_thu


def run_adherence_report(start_d: date, end_d: date, write_csv: bool = False) -> None:
    from google.cloud import bigquery
    bq = _bq_client()
    params = [
        bigquery.ScalarQueryParameter("d_start", "DATE", start_d.isoformat()),
        bigquery.ScalarQueryParameter("d_end",   "DATE", end_d.isoformat()),
    ]

    print("\n" + "=" * 108)
    print(f"  LOV3 HTX — Schedule Adherence  ({start_d} → {end_d})")
    print("=" * 108)

    # Adherence rows for scheduled shifts
    rows = list(bq.query(ADHERENCE_SQL,
                         job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    unsched = list(bq.query(UNSCHEDULED_SQL,
                            job_config=bigquery.QueryJobConfig(query_parameters=params)).result())

    if not rows and not unsched:
        print("\n  No scheduled shifts and no actual clock-ins found for this window.")
        print("  → Confirm HotSchedules SFTP export has landed shifts_*.txt files, and that")
        print("    LaborTimeEntries_raw has data for this period.\n")
        return

    if rows:
        print(f"\n  SCHEDULED SHIFTS ({len(rows)})")
        print(f"  {'Date':<11} {'Employee':<22} {'Job':<14} {'Scheduled':<22} {'Actual':<18} {'Sched':>5} {'Wrk':>5} {'Δ':>5}  {'Status':<18} Note")
        print(f"  {'-'*11} {'-'*22} {'-'*14} {'-'*22} {'-'*18} {'-'*5} {'-'*5} {'-'*5}  {'-'*18} {'-'*10}")

        n_noshow = n_late = n_early_out = n_on_time = n_unmapped = 0
        for r in rows:
            note = "unmapped emp" if r.unmapped_employee else ""
            actual_range = ""
            if r.actual_in_ct:
                actual_range = f"{r.actual_in_ct}-{r.actual_out_ct or '--'}"
            sched_range = f"{r.scheduled_in_ct}-{r.scheduled_out_ct}"
            print(f"  {str(r.business_date):<11} {r.scheduled_name[:22]:<22} "
                  f"{(r.scheduled_job or '')[:14]:<14} {sched_range:<22} {actual_range:<18} "
                  f"{r.scheduled_minutes:>5} {r.worked_minutes:>5} {r.delta_minutes:>+5}  "
                  f"{r.adherence:<18} {note}")
            if r.unmapped_employee: n_unmapped += 1
            if r.adherence == "NO SHOW":       n_noshow += 1
            elif r.adherence == "ON TIME":     n_on_time += 1
            elif r.adherence.startswith("LATE"): n_late += 1
            elif r.adherence.startswith("EARLY OUT"): n_early_out += 1

        print(f"\n  Summary: {n_on_time} on time · {n_late} late · {n_early_out} early-out · "
              f"{n_noshow} NO-SHOW · {n_unmapped} unmapped employees (add to {BQ_EMP_MAP_TABLE})")

    if unsched:
        print(f"\n  WORKED WITHOUT BEING SCHEDULED ({len(unsched)})")
        print(f"  {'Date':<11} {'Employee':<22} {'Job':<14} {'Clocked in':<22} {'Clocked out':<12} {'Wrk':>5}")
        print(f"  {'-'*11} {'-'*22} {'-'*14} {'-'*22} {'-'*12} {'-'*5}")
        for r in unsched:
            print(f"  {str(r.business_date):<11} {(r.employee_name or '')[:22]:<22} "
                  f"{(r.job_title or '')[:14]:<14} {r.clocked_in:<22} {r.clocked_out:<12} "
                  f"{r.worked_minutes:>5}")

    print()

    if write_csv:
        stamp = f"{start_d.strftime('%Y%m%d')}_{end_d.strftime('%Y%m%d')}"
        adh_path = f"lov3_schedule_adherence_{stamp}.csv"
        with open(adh_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["business_date","scheduled_name","scheduled_job","scheduled_in_ct",
                        "scheduled_out_ct","actual_in_ct","actual_out_ct","scheduled_minutes",
                        "worked_minutes","delta_minutes","adherence","unmapped_employee"])
            for r in rows:
                w.writerow([r.business_date, r.scheduled_name, r.scheduled_job,
                            r.scheduled_in_ct, r.scheduled_out_ct, r.actual_in_ct,
                            r.actual_out_ct, r.scheduled_minutes, r.worked_minutes,
                            r.delta_minutes, r.adherence, r.unmapped_employee])
        print(f"  Adherence CSV: {adh_path}")
        if unsched:
            uns_path = f"lov3_unscheduled_shifts_{stamp}.csv"
            with open(uns_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["business_date","employee_name","job_title","clocked_in",
                            "clocked_out","worked_minutes"])
                for r in unsched:
                    w.writerow([r.business_date, r.employee_name, r.job_title,
                                r.clocked_in, r.clocked_out, r.worked_minutes])
            print(f"  Unscheduled CSV: {uns_path}")
        print()


# ---------------------------------------------------------------------------
# SFTP sync (poll -> download -> parse -> load)
# ---------------------------------------------------------------------------

def sync_from_sftp() -> int:
    """Download any shifts_*.txt files we haven't already ingested. Returns rows loaded."""
    ensure_tables()
    total = 0
    with HotSchedulesSFTP() as hs:
        files = hs.list_export_files(prefix="shifts_")
        if not files:
            log.info("No shifts_*.txt files in %s (folder is empty — Fourth may not "
                     "have enabled the export cron yet)", SFTP_EXPORT_DIR)
            return 0
        log.info("Found %d shifts file(s) on SFTP", len(files))
        for name, size, mtime in files:
            if already_ingested(name):
                log.info("  SKIP %s (already ingested)", name)
                continue
            log.info("  PULL %s (%d bytes)", name, size)
            with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
                local = tmp.name
            try:
                hs.download(name, local)
                mtime_dt = datetime.utcfromtimestamp(mtime)
                rows = parse_shifts_file(local, source_file=name, file_mtime=mtime_dt)
                log.info("    parsed %d shift rows", len(rows))
                loaded = load_shifts(rows)
                log.info("    loaded %d rows to BQ", loaded)
                total += loaded
            finally:
                try: os.unlink(local)
                except OSError: pass
    refresh_employee_map()
    return total


# ---------------------------------------------------------------------------
# End-to-end sample verification (no real HS export needed)
# ---------------------------------------------------------------------------

def make_sample_file(target_path: str, prior_start: date) -> None:
    """Generate a synthetic pipe-delimited shifts file matching v2.5 spec.

    Uses real LOV3 employee names from LaborTimeEntries_raw so the mapping
    auto-match succeeds during verification.
    """
    bq = _bq_client()
    q = f"""
      SELECT DISTINCT employee_name, job_title
      FROM `{PROJECT_ID}.{BQ_DATASET}.LaborTimeEntries_raw`
      WHERE employee_name IS NOT NULL AND job_title IS NOT NULL AND NOT COALESCE(deleted, FALSE)
      ORDER BY employee_name
      LIMIT 6
    """
    staff = [(r.employee_name, r.job_title) for r in bq.query(q).result()]
    if not staff:
        raise RuntimeError("No LaborTimeEntries_raw rows to build sample from.")

    ww_end = prior_start + timedelta(days=6)
    company = int(_secret("hotschedules-company-id"))
    concept = int(_secret("hotschedules-concept-id"))
    store   = int(_secret("hotschedules-store-id"))
    lines: list[str] = []
    hs_emp_id = 10001
    for name, job in staff:
        first, _, last = name.partition(" ")
        # Two shifts for this staffer during the prior week
        for offset, (start_h, end_h) in enumerate([(17, 23), (19, 24)]):
            biz = prior_start + timedelta(days=offset * 2)
            shift_start = datetime.combine(biz, datetime.min.time()).replace(hour=start_h)
            shift_end   = datetime.combine(biz, datetime.min.time()).replace(hour=end_h % 24)
            reg_min = int((shift_end - shift_start).total_seconds() // 60)
            lines.append("|".join([
                str(company), str(concept), str(store),
                biz.strftime("%Y-%m-%d 00:00:00.000"),
                prior_start.strftime("%Y-%m-%d 00:00:00.000"),
                ww_end.strftime("%Y-%m-%d 00:00:00.000"),
                "1", "LOV3 HTX",
                str(hs_emp_id), first, last,
                "100", job,
                "15.00", "22.50",
                shift_start.strftime("%Y-%m-%d %H:%M:%S.000"),
                shift_end.strftime("%Y-%m-%d %H:%M:%S.000"),
                str(reg_min), "0",
                f"{reg_min * 15.00 / 60:.2f}", "0.00", "0.00",
            ]))
        hs_emp_id += 1
    with open(target_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    log.info("Wrote synthetic sample (%d shifts, %d staff): %s", len(lines), len(staff), target_path)


def dry_run_with_sample() -> None:
    """Full round-trip: generate sample -> upload to /Test/ -> parse locally -> load -> report."""
    ensure_tables()
    prior_fri, prior_thu = _prior_lov3_workweek()
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w") as tmp:
        local = tmp.name
    try:
        make_sample_file(local, prior_fri)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        remote_name = f"shifts_{stamp}_SAMPLE.txt"

        # Upload to /Test/ to prove SFTP write access
        with HotSchedulesSFTP() as hs:
            hs.upload(local, remote_name, remote_dir=SFTP_TEST_DIR)
        log.info("Uploaded sample to %s/%s", SFTP_TEST_DIR, remote_name)

        # Parse + load locally (Fourth doesn't move Test/ files to Export/)
        rows = parse_shifts_file(local, source_file=remote_name,
                                 file_mtime=datetime.utcnow())
        log.info("Parsed %d shift rows from sample", len(rows))
        if already_ingested(remote_name):
            log.info("Sample already ingested — skipping load")
        else:
            log.info("Loaded %d rows to BQ", load_shifts(rows))

        refresh_employee_map()
        run_adherence_report(prior_fri, prior_thu, write_csv=False)
    finally:
        try: os.unlink(local)
        except OSError: pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def main():
    p = argparse.ArgumentParser(description="HotSchedules scheduled-shifts pipeline + adherence report")
    p.add_argument("--sync", action="store_true", help="Poll SFTP + load new shifts to BQ")
    p.add_argument("--adherence-report", action="store_true",
                   help="Print scheduled-vs-actual report (default: prior full ISO week)")
    p.add_argument("--start", type=_parse_yyyymmdd, help="Report start date YYYYMMDD")
    p.add_argument("--end", type=_parse_yyyymmdd, help="Report end date YYYYMMDD")
    p.add_argument("--csv", action="store_true", help="Also write CSV files for the report")
    p.add_argument("--dry-run-with-sample", action="store_true",
                   help="End-to-end round-trip using a synthetic sample file")
    p.add_argument("--refresh-map", action="store_true", help="Re-run name-match to populate employee map")
    p.add_argument("--ensure-tables", action="store_true", help="Create BQ tables if missing and exit")
    args = p.parse_args()

    if not any([args.sync, args.adherence_report, args.dry_run_with_sample,
                args.refresh_map, args.ensure_tables]):
        p.print_help()
        sys.exit(1)

    if args.ensure_tables:
        ensure_tables()
        return
    if args.sync:
        n = sync_from_sftp()
        log.info("Sync complete: %d new rows loaded", n)
    if args.refresh_map:
        ensure_tables()
        refresh_employee_map()
    if args.dry_run_with_sample:
        dry_run_with_sample()
        return
    if args.adherence_report:
        if args.start and args.end:
            start_d, end_d = args.start, args.end
        else:
            start_d, end_d = _prior_lov3_workweek()
        run_adherence_report(start_d, end_d, write_csv=args.csv)


if __name__ == "__main__":
    main()
