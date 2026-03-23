"""
Toast SFTP to BigQuery ETL Pipeline
Cloud Run service for LOV3 Houston

Features:
- Daily automated ingestion from Toast SFTP
- Schema validation and drift detection
- Incremental loads with deduplication
- Error handling with Slack/email alerts
- Backfill support for historical data
"""

import os
import io
import re
import csv
import json
import logging
import hashlib
import calendar
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

import paramiko
import pandas as pd
from google.cloud import bigquery, secretmanager
from google.cloud.exceptions import NotFound
from flask import Flask, request, jsonify, Response
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
import google.auth
from googleapiclient.discovery import build as google_build
from googleapiclient.errors import HttpError as GoogleHttpError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration — constants and dataclasses moved to config.py / models.py
from config import *  # noqa: F403 — transitional wildcard during modularization
from models import PipelineResult, PipelineRunSummary, BankUploadResult
from dashboards import (
    _bank_review_html, _pnl_dashboard_html, _analysis_dashboard_html,
    _cash_recon_html, _menu_mix_html, _events_calendar_html,
    _customer_loyalty_html, _server_performance_html, _kitchen_speed_html,
    _labor_dashboard_html, _menu_engineering_html, _kpi_benchmarks_html,
    _budget_html, _event_roi_html,
)


class BofACSVParser:
    """Parses Bank of America CSV exports and auto-categorizes transactions"""

    def __init__(self, category_rules: List[Dict[str, str]],
                 check_register: Optional[Dict[str, Dict]] = None):
        self.category_rules = category_rules
        self.check_register = check_register or {}

    def parse(self, file_content: bytes, source_filename: str) -> pd.DataFrame:
        """Parse BofA CSV file content into a DataFrame.

        BofA CSV format: Date, Description, Amount, Running Bal.
        - Dates are MM/DD/YYYY
        - Negative amounts = debits, positive = credits
        """
        try:
            text = file_content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = file_content.decode("latin-1")

        # BofA CSVs have a summary header block before the real data.
        # Find the row that starts with "Date,Description" to use as header.
        lines = text.splitlines()
        data_start = 0
        for i, line in enumerate(lines):
            if line.strip().lower().startswith("date,"):
                data_start = i
                break

        csv_text = "\n".join(lines[data_start:])

        df = pd.read_csv(
            io.StringIO(csv_text),
            header=0,
            dtype=str,
            skip_blank_lines=True,
            quotechar='"',
        )

        # Normalize column names - BofA uses various header names
        col_map = {}
        for col in df.columns:
            lower = col.strip().lower()
            if "date" in lower and "post" not in lower:
                col_map[col] = "date_raw"
            elif "desc" in lower:
                col_map[col] = "description"
            elif "amount" in lower:
                col_map[col] = "amount_raw"
            elif "bal" in lower:
                col_map[col] = "running_balance_raw"

        df = df.rename(columns=col_map)

        required = {"date_raw", "description", "amount_raw"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"BofA CSV missing required columns: {missing}. "
                f"Found columns: {list(df.columns)}"
            )

        # Drop rows where date is missing (trailing blank rows)
        df = df.dropna(subset=["date_raw"])
        df = df[df["date_raw"].str.strip() != ""]

        # Parse dates - store as YYYY-MM-DD string for BigQuery DATE compatibility
        # BofA uses both MM/DD/YYYY and M/D/YY formats across different exports
        df["transaction_date"] = pd.to_datetime(
            df["date_raw"].str.strip(), format="mixed", dayfirst=False
        ).dt.strftime("%Y-%m-%d")

        # Parse amounts
        df["amount"] = pd.to_numeric(
            df["amount_raw"].str.replace(",", "").str.strip(),
            errors="coerce",
        )

        # Running balance (optional)
        if "running_balance_raw" in df.columns:
            df["running_balance"] = pd.to_numeric(
                df["running_balance_raw"].str.replace(",", "").str.strip(),
                errors="coerce",
            )
        else:
            df["running_balance"] = None

        # Derived columns
        df["transaction_type"] = df["amount"].apply(
            lambda x: "credit" if pd.notna(x) and x >= 0 else "debit"
        )
        df["abs_amount"] = df["amount"].abs()

        # Auto-categorize
        df["description"] = df["description"].fillna("").str.strip()
        categories = df["description"].apply(self._categorize)
        df["category"] = categories.apply(lambda x: x[0])
        df["category_source"] = categories.apply(lambda x: x[1])
        df["vendor_normalized"] = categories.apply(lambda x: x[2])

        # Metadata
        df["source_file"] = source_filename

        # Select final columns
        return df[
            [
                "transaction_date",
                "description",
                "amount",
                "running_balance",
                "transaction_type",
                "abs_amount",
                "category",
                "category_source",
                "vendor_normalized",
                "source_file",
            ]
        ].copy()

    @staticmethod
    def _extract_wire_vendor(description: str) -> Optional[str]:
        """Extract the actual vendor name from wire transfer descriptions.

        Outbound wires contain BNF:<beneficiary name> ID:...
        Inbound wires contain ORIG:1/<originator name> ID:...
        Returns None if not a wire or parsing fails.
        """
        if "WIRE TYPE:" not in description.upper():
            return None
        import re

        # Outbound: BNF:GREATLAND INVESTMENT INC. ID:...
        bnf = re.search(r"BNF:([^/]+?)\s*ID:", description)
        if bnf:
            return bnf.group(1).strip()
        # Inbound: ORIG:1/DERWIN ALONZO JAMES JR ID:...
        orig = re.search(r"ORIG:\d*/([^/]+?)\s*ID:", description)
        if orig:
            return orig.group(1).strip()
        return None

    @staticmethod
    def _normalize_description(description: str) -> str:
        """Strip common BofA boilerplate prefixes so keyword matching hits the vendor portion."""
        desc = description.strip()

        # ── Zelle ────────────────────────────────────────────────────────

        # Formats seen:
        #   Zelle payment to NAME Conf# xxx
        #   Zelle payment to NAME for MEMO"; Conf# xxx"
        #   Zelle payment from NAME Conf# xxx
        # We extract just the payee NAME.
        zelle = re.match(
            r'(?i)^Zelle\s+payment\s+(?:to|from)\s+'   # prefix
            r'(.+?)'                                     # payee (non-greedy)
            r'(?:\s+for\s+.*|\s+Conf#.*|[";]+.*)$',     # stop at "for", "Conf#", or quote artifacts
            desc,
        )
        if zelle:
            return zelle.group(1).strip().rstrip('";')

        # ACH: "ACH Debit SYSCO CORP ID:..." / "ACH Credit ..." / "ACH Hold ..."
        desc = re.sub(r"(?i)^ACH\s+(?:Debit|Credit|Hold)\s+", "", desc)
        # Debit card: "PURCHASE AUTHORIZED ON 01/15 COSTCO..." / "PURCHASE ON 01/15 ..."
        desc = re.sub(r"(?i)^PURCHASE\s+(?:AUTHORIZED\s+)?ON\s+\d{2}/\d{2}\s+", "", desc)
        # Checkcard: "CHECKCARD 0115 MERCHANT..."
        desc = re.sub(r"(?i)^CHECKCARD\s+\d{4}\s+", "", desc)
        # Trailing reference numbers: " ID:...", " Conf#..."
        desc = re.sub(r"\s+ID:\S*$", "", desc)
        desc = re.sub(r'\s+Conf#.*$', "", desc)
        return desc.strip()

    def _categorize(self, description: str) -> Tuple[str, str, str]:
        """Return (category, source, vendor_normalized) for a description.

        For "Check XXXX" descriptions, looks up the check register first
        to resolve the payee, then runs the payee through keyword rules.
        """
        # ── Check register lookup ────────────────────────────────────────
        check_match = re.match(r"(?i)^check\s+(\d+)$", description.strip())
        if check_match and self.check_register:
            check_num = check_match.group(1)
            entry = self.check_register.get(check_num)
            if entry:
                payee = entry.get("payee", "")
                # If the register already has a category, use it
                if entry.get("category"):
                    vendor = entry.get("vendor_normalized", payee)
                    return (entry["category"], "check_register", vendor)
                # Otherwise, run the payee through keyword rules
                payee_upper = payee.upper()
                for rule in self.category_rules:
                    keyword = rule["keyword"].strip().upper()
                    if re.search(r"\b" + re.escape(keyword) + r"\b", payee_upper):
                        vendor = rule.get("vendor_normalized", payee)
                        return (rule["category"], "check_register", vendor)
                # No rule matched — still use payee as vendor_normalized
                return ("Uncategorized", "uncategorized", payee or description)
            # Check not in register — fall through to uncategorized
            return ("Uncategorized", "uncategorized", description)

        # ── Toast POS transaction detection ────────────────────────────
        # Toast sends multiple ACH types that need different categories:
        #   TOAST DES:DEP       = daily credit card settlement (Revenue)
        #   TOAST DES:EOM       = end-of-month adjustment (Revenue)
        #   TOAST, INC. DES:YYYYMMDD = monthly settlement report (Revenue)
        #   Toast, Inc DES:Toast = monthly platform/subscription fee (OPEX)
        #   TOAST DES:REF       = processing fee refund (OPEX)
        desc_upper = description.strip().upper()
        if "TOAST" in desc_upper and "DES:" in desc_upper:
            if "DES:DEP" in desc_upper:
                return ("1. Revenue/Sales Revenue", "auto", "Toast Deposit")
            if "DES:EOM" in desc_upper:
                return ("1. Revenue/Sales Revenue", "auto", "Toast EOM Adjustment")
            if re.search(r"TOAST,?\s*INC\.?\s+DES:\d{8}", desc_upper):
                return ("1. Revenue/Sales Revenue", "auto", "Toast Settlement")
            if "DES:REF" in desc_upper:
                return ("5. Operating Expenses (OPEX)/POS & Technology Fees", "auto", "Toast Refund")
            if re.search(r"TOAST,?\s*INC\s+DES:TOAST", desc_upper):
                return ("5. Operating Expenses (OPEX)/POS & Technology Fees", "auto", "Toast Platform Fee")

        # ── Standard keyword matching ────────────────────────────────────
        normalized = self._normalize_description(description)
        norm_upper = normalized.upper()
        for rule in self.category_rules:
            keyword = rule["keyword"].strip().upper()
            if re.search(r"\b" + re.escape(keyword) + r"\b", norm_upper):
                vendor = rule.get("vendor_normalized", description)
                # For wire transfers, prefer the parsed beneficiary/originator
                wire_vendor = self._extract_wire_vendor(description)
                if wire_vendor:
                    vendor = wire_vendor
                return (rule["category"], "auto", vendor)
        # No rule matched — still try to extract wire vendor for better naming
        wire_vendor = self._extract_wire_vendor(description)
        if wire_vendor:
            return ("Uncategorized", "uncategorized", wire_vendor)
        # Use the normalized description (e.g. Zelle payee name) not the raw one
        return ("Uncategorized", "uncategorized", normalized or description)


class BankCategoryManager:
    """CRUD for bank transaction category rules stored in BigQuery"""

    TABLE = "BankCategoryRules"

    def __init__(self, bq_client: bigquery.Client, dataset_id: str):
        self.bq_client = bq_client
        self.dataset_id = dataset_id
        self.table_ref = f"{PROJECT_ID}.{self.dataset_id}.{self.TABLE}"

    def _ensure_table(self) -> None:
        """Create the rules table if it doesn't exist."""
        try:
            self.bq_client.get_table(self.table_ref)
        except NotFound:
            schema = [
                bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("vendor_normalized", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
            ]
            table = bigquery.Table(self.table_ref, schema=schema)
            self.bq_client.create_table(table)
            logger.info(f"Created {self.TABLE} table")

    def seed_defaults(self) -> int:
        """Seed default rules if table is empty. Returns number seeded."""
        self._ensure_table()

        count_query = f"SELECT COUNT(*) as cnt FROM `{self.table_ref}`"
        result = list(self.bq_client.query(count_query).result())[0]
        if result.cnt > 0:
            return 0

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        rows = [
            {
                "keyword": r["keyword"],
                "category": r["category"],
                "vendor_normalized": r.get("vendor_normalized", ""),
                "created_at": now,
                "updated_at": now,
            }
            for r in DEFAULT_CATEGORY_RULES
        ]
        errors = self.bq_client.insert_rows_json(self.table_ref, rows)
        if errors:
            logger.error(f"Error seeding category rules: {errors}")
            return 0
        logger.info(f"Seeded {len(rows)} default category rules")
        return len(rows)

    def list_rules(self) -> List[Dict]:
        """Return all rules."""
        self._ensure_table()
        self.seed_defaults()
        query = f"""
        SELECT keyword, category, vendor_normalized
        FROM `{self.table_ref}`
        ORDER BY LENGTH(keyword) DESC, keyword
        """
        rows = list(self.bq_client.query(query).result())
        return [
            {
                "keyword": r.keyword,
                "category": r.category,
                "vendor_normalized": r.vendor_normalized or "",
            }
            for r in rows
        ]

    def upsert_rule(self, keyword: str, category: str, vendor_normalized: str = "") -> None:
        """Add or update a single rule."""
        self._ensure_table()
        now = datetime.utcnow()
        merge_sql = f"""
        MERGE `{self.table_ref}` T
        USING (SELECT @keyword AS keyword, @category AS category,
               @vendor AS vendor_normalized, @now AS ts) S
        ON UPPER(T.keyword) = UPPER(S.keyword)
        WHEN MATCHED THEN
            UPDATE SET category = S.category,
                       vendor_normalized = S.vendor_normalized,
                       updated_at = S.ts
        WHEN NOT MATCHED THEN
            INSERT (keyword, category, vendor_normalized, created_at, updated_at)
            VALUES (S.keyword, S.category, S.vendor_normalized, S.ts, S.ts)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("keyword", "STRING", keyword),
                bigquery.ScalarQueryParameter("category", "STRING", category),
                bigquery.ScalarQueryParameter("vendor", "STRING", vendor_normalized),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
            ]
        )
        self.bq_client.query(merge_sql, job_config=job_config).result()

    def delete_rule(self, keyword: str) -> None:
        """Delete a rule by keyword."""
        delete_sql = f"""
        DELETE FROM `{self.table_ref}`
        WHERE UPPER(keyword) = UPPER(@keyword)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("keyword", "STRING", keyword),
            ]
        )
        self.bq_client.query(delete_sql, job_config=job_config).result()


class CheckRegisterSync:
    """Sync a Google Sheet check register into BigQuery.

    Reads (check_number, payee, amount, memo, category) from the sheet,
    then MERGEs into the CheckRegister BigQuery table keyed on check_number.
    Auth uses Application Default Credentials (same service account as BQ).
    """

    TABLE = "CheckRegister"
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

    # Fuzzy column name mapping: sheet header → internal name
    _COL_ALIASES = {
        "check_number": ["check number", "check_number", "check #", "check no", "check", "number", "chk", "chk #", "chk no"],
        "payee": ["payee", "vendor", "pay to", "pay to the order of", "name", "paid to"],
        "amount": ["amount", "amt", "total"],
        "memo": ["memo", "note", "notes", "description", "desc", "expense"],
        "category": ["category", "expense category", "cat", "type"],
    }

    def __init__(self, bq_client: bigquery.Client, dataset_id: str):
        self.bq_client = bq_client
        self.dataset_id = dataset_id
        self.table_ref = f"{PROJECT_ID}.{self.dataset_id}.{self.TABLE}"

    def _ensure_table(self) -> None:
        """Create the CheckRegister table if it doesn't exist."""
        try:
            self.bq_client.get_table(self.table_ref)
        except NotFound:
            schema = [
                bigquery.SchemaField("check_number", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("payee", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("vendor_normalized", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("amount", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("memo", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("synced_at", "TIMESTAMP", mode="NULLABLE"),
            ]
            table = bigquery.Table(self.table_ref, schema=schema)
            self.bq_client.create_table(table)
            logger.info(f"Created {self.TABLE} table")

    def _resolve_columns(self, headers: List[str]) -> Dict[str, int]:
        """Map sheet column headers to internal names via fuzzy matching.

        Returns dict of internal_name → column_index.
        """
        mapping: Dict[str, int] = {}
        for idx, raw in enumerate(headers):
            h = raw.strip().lower()
            for internal, aliases in self._COL_ALIASES.items():
                if h in aliases and internal not in mapping:
                    mapping[internal] = idx
                    break
        return mapping

    def _read_sheet(self, sheet_id: str = CHECK_REGISTER_SHEET_ID) -> List[Dict]:
        """Read check register rows from the 'check_register_master' sheet only."""
        creds, _ = google.auth.default(scopes=self.SCOPES)
        service = google_build("sheets", "v4", credentials=creds, cache_discovery=False)

        # Only read from the check_register_master sheet — other sheets in
        # the workbook are duplicates or used for analytics.
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=f"'{CHECK_REGISTER_SHEET_NAME}'"
        ).execute()
        rows = result.get("values", [])

        if len(rows) < 2:
            logger.warning("Check register sheet has fewer than 2 rows")
            return []

        col_map = self._resolve_columns(rows[0])
        if "check_number" not in col_map:
            raise ValueError(
                f"Could not find a check number column in sheet headers: {rows[0]}"
            )
        if "payee" not in col_map:
            raise ValueError(
                f"Could not find a payee column in sheet headers: {rows[0]}"
            )

        records: List[Dict] = []
        for row in rows[1:]:
            def cell(name: str) -> str:
                idx = col_map.get(name)
                if idx is None or idx >= len(row):
                    return ""
                return str(row[idx]).strip()

            check_num = cell("check_number")
            # Skip blank rows or non-numeric check numbers
            if not check_num or not re.search(r"\d+", check_num):
                continue
            # Normalize to digits only (e.g. "#1414" → "1414")
            check_num = re.sub(r"[^\d]", "", check_num)

            payee = cell("payee")
            amount_str = cell("amount")
            amount = None
            if amount_str:
                try:
                    amount = float(amount_str.replace(",", "").replace("$", ""))
                except ValueError:
                    pass

            records.append({
                "check_number": check_num,
                "payee": payee,
                "category": cell("category"),
                "vendor_normalized": payee,  # default to payee name
                "amount": amount,
                "memo": cell("memo"),
            })

        return records

    def sync_from_sheet(self, sheet_id: str = CHECK_REGISTER_SHEET_ID) -> int:
        """Pull the Google Sheet and MERGE into BigQuery. Returns row count."""
        self._ensure_table()
        records = self._read_sheet(sheet_id)
        if not records:
            logger.info("No check register rows to sync")
            return 0

        now = pd.Timestamp.utcnow()
        for r in records:
            r["synced_at"] = now

        # Load into temp table then MERGE
        df = pd.DataFrame(records)
        temp_table = f"{self.TABLE}_staging"
        temp_ref = f"{PROJECT_ID}.{self.dataset_id}.{temp_table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        self.bq_client.load_table_from_dataframe(
            df, temp_ref, job_config=job_config
        ).result()

        # Deduplicate staging table (sheet may have duplicate check numbers);
        # keep the last row per check_number (latest in sheet order).
        merge_sql = f"""
        MERGE `{self.table_ref}` T
        USING (
            SELECT * FROM `{temp_ref}`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY check_number ORDER BY synced_at DESC) = 1
        ) S
        ON T.check_number = S.check_number
        WHEN MATCHED THEN
            UPDATE SET
                payee = S.payee,
                category = S.category,
                vendor_normalized = S.vendor_normalized,
                amount = S.amount,
                memo = S.memo,
                synced_at = S.synced_at
        WHEN NOT MATCHED THEN
            INSERT (check_number, payee, category, vendor_normalized,
                    amount, memo, synced_at)
            VALUES (S.check_number, S.payee, S.category,
                    S.vendor_normalized, S.amount, S.memo, S.synced_at)
        """
        job = self.bq_client.query(merge_sql)
        job.result()
        self.bq_client.delete_table(temp_ref, not_found_ok=True)

        logger.info(f"Synced {len(records)} check register rows from Google Sheet")
        return len(records)

    def load_from_csv(self, file_content: bytes) -> int:
        """Load check register from a CSV upload (fallback). Returns row count."""
        self._ensure_table()
        try:
            text = file_content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = file_content.decode("latin-1")

        df = pd.read_csv(io.StringIO(text), dtype=str)
        # Normalize column names to lowercase
        df.columns = [c.strip().lower().replace(" ", "_").replace("#", "number") for c in df.columns]

        if "check_number" not in df.columns:
            raise ValueError(f"CSV must have a check_number column. Found: {list(df.columns)}")
        if "payee" not in df.columns:
            raise ValueError(f"CSV must have a payee column. Found: {list(df.columns)}")

        df["check_number"] = df["check_number"].astype(str).str.replace(r"[^\d]", "", regex=True)
        df = df[df["check_number"].str.len() > 0]

        if "vendor_normalized" not in df.columns:
            df["vendor_normalized"] = df["payee"]
        if "category" not in df.columns:
            df["category"] = ""
        if "amount" not in df.columns:
            df["amount"] = None
        else:
            df["amount"] = pd.to_numeric(
                df["amount"].str.replace(",", "").str.replace("$", ""),
                errors="coerce",
            )
        if "memo" not in df.columns:
            df["memo"] = ""

        now = pd.Timestamp.utcnow()
        df["synced_at"] = now

        final = df[["check_number", "payee", "category", "vendor_normalized",
                     "amount", "memo", "synced_at"]].copy()

        # MERGE same as sync_from_sheet
        temp_table = f"{self.TABLE}_staging"
        temp_ref = f"{PROJECT_ID}.{self.dataset_id}.{temp_table}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        self.bq_client.load_table_from_dataframe(
            final, temp_ref, job_config=job_config
        ).result()

        merge_sql = f"""
        MERGE `{self.table_ref}` T
        USING (
            SELECT * FROM `{temp_ref}`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY check_number ORDER BY synced_at DESC) = 1
        ) S
        ON T.check_number = S.check_number
        WHEN MATCHED THEN
            UPDATE SET
                payee = S.payee,
                category = IFNULL(NULLIF(S.category, ''), T.category),
                vendor_normalized = S.vendor_normalized,
                amount = S.amount,
                memo = S.memo,
                synced_at = S.synced_at
        WHEN NOT MATCHED THEN
            INSERT (check_number, payee, category, vendor_normalized,
                    amount, memo, synced_at)
            VALUES (S.check_number, S.payee, S.category,
                    S.vendor_normalized, S.amount, S.memo, S.synced_at)
        """
        job = self.bq_client.query(merge_sql)
        job.result()
        self.bq_client.delete_table(temp_ref, not_found_ok=True)

        logger.info(f"Loaded {len(final)} check register rows from CSV")
        return len(final)

    def get_lookup(self) -> Dict[str, Dict]:
        """Return a dict of check_number → {payee, category, vendor_normalized}
        from the BigQuery table for use in categorization."""
        self._ensure_table()
        query = f"""
        SELECT check_number, payee, category, vendor_normalized
        FROM `{self.table_ref}`
        """
        rows = list(self.bq_client.query(query).result())
        return {
            r.check_number: {
                "payee": r.payee or "",
                "category": r.category or "",
                "vendor_normalized": r.vendor_normalized or r.payee or "",
            }
            for r in rows
        }


class SecretManager:
    """Handles GCP Secret Manager for credentials"""
    
    def __init__(self, project_id: str):
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = project_id
    
    def get_secret(self, secret_name: str) -> str:
        """Retrieve secret value from Secret Manager"""
        name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    
    def get_sftp_key(self) -> str:
        """Get SFTP private key"""
        return self.get_secret("toast-sftp-private-key")


class ToastSFTPClient:
    """SFTP client for Toast data files"""
    
    def __init__(self, host: str, port: int, username: str, private_key: str):
        self.host = host
        self.port = port
        self.username = username
        self.private_key = private_key
        self._client = None
        self._sftp = None
    
    def connect(self):
        """Establish SFTP connection"""
        key_file = io.StringIO(self.private_key)
        private_key = paramiko.RSAKey.from_private_key(key_file)
        
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            pkey=private_key,
            look_for_keys=False
        )
        self._sftp = self._client.open_sftp()
        logger.info(f"Connected to SFTP: {self.host}")
    
    def disconnect(self):
        """Close SFTP connection"""
        if self._sftp:
            self._sftp.close()
        if self._client:
            self._client.close()
        logger.info("Disconnected from SFTP")
    
    def list_files(self, date_str: str) -> List[str]:
        """List files for a given date (YYYYMMDD format)"""
        try:
            path = f"185129/{date_str}"
            files = self._sftp.listdir(path)
            return [f for f in files if f.endswith('.csv')]
        except FileNotFoundError:
            logger.warning(f"No directory found for date: {date_str}")
            return []

    def download_file(self, date_str: str, filename: str) -> bytes:
        """Download file contents as bytes"""
        path = f"185129/{date_str}/{filename}"
        with self._sftp.file(path, 'r') as f:
            return f.read()
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class SchemaValidator:
    """Validates and detects schema changes"""
    
    def __init__(self, bq_client: bigquery.Client, dataset_id: str):
        self.bq_client = bq_client
        self.dataset_id = dataset_id
    
    def get_table_schema(self, table_loc: str) -> Dict[str, str]:
        """Get current BigQuery table schema"""
        try:
            table_ref = f"{PROJECT_ID}.{self.dataset_id}.{table_loc}"
            table = self.bq_client.get_table(table_ref)
            return {field.name: field.field_type for field in table.schema}
        except NotFound:
            return {}
    
    def detect_schema_changes(
        self, 
        df: pd.DataFrame, 
        table_loc: str,
        column_mapping: Dict[str, str]
    ) -> Tuple[bool, List[str]]:
        """
        Compare DataFrame schema to BigQuery table schema
        Returns (has_changes, list of change descriptions)
        """
        changes = []
        bq_schema = self.get_table_schema(table_loc)
        
        if not bq_schema:
            return False, ["Table does not exist - will be created"]
        
        # Map CSV columns to BQ column names
        csv_columns = set(column_mapping.get(col, col.lower().replace(' ', '_').replace('#', 'number')) 
                         for col in df.columns)
        bq_columns = set(bq_schema.keys())
        
        # Find new columns in CSV
        new_cols = csv_columns - bq_columns
        for col in new_cols:
            changes.append(f"NEW COLUMN: {col}")
        
        # Find removed columns
        removed_cols = bq_columns - csv_columns
        # Exclude computed columns from "removed" check
        computed = {"processing_date", "calculated_total"}
        removed_cols = removed_cols - computed
        for col in removed_cols:
            changes.append(f"REMOVED COLUMN: {col}")
        
        return len(changes) > 0, changes


class DataTransformer:
    """Transforms Toast CSV data for BigQuery"""
    
    @staticmethod
    def parse_toast_datetime(date_str: str) -> Optional[str]:
        """Parse Toast datetime format and return as ISO string for BigQuery"""
        if pd.isna(date_str) or date_str == '':
            return None

        formats = [
            "%m/%d/%y %I:%M %p",
            "%m/%d/%y %I:%M:%S %p",
            "%m/%d/%Y %I:%M %p",
            "%m/%d/%Y %I:%M:%S %p",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d"
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(str(date_str).strip(), fmt)
                # Return as ISO format string for BigQuery TIMESTAMP compatibility
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

        # If no format matches, return original string (BigQuery may still parse it)
        logger.warning(f"Could not parse datetime: {date_str}")
        return str(date_str).strip() if date_str else None
    
    @staticmethod
    def parse_duration(duration_str: str) -> Optional[str]:
        """Parse duration string (HH:MM:SS) to TIME format"""
        if pd.isna(duration_str) or duration_str == '':
            return None
        return str(duration_str).strip()

    @staticmethod
    def prepare_for_bigquery(df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for BigQuery loading - SIMPLIFIED APPROACH.

        Convert everything to either:
        - float64 for numeric columns
        - string for everything else

        This avoids type inference issues with BigQuery autodetect.
        """
        df = df.copy()

        # Columns that are truly numeric (amounts, counts, percentages, IDs)
        numeric_columns = {
            # Money amounts
            'amount', 'tip', 'gratuity', 'total', 'tax', 'discount', 'discount_amount',
            'net_price', 'gross_price', 'swiped_card_amount', 'keyed_card_amount',
            'amount_tendered', 'refund_amount', 'refund_tip_amount', 'v_mc_d_fees',
            'net_amount', 'gross_amount', 'void_amount', 'avg_price',
            'gross_amount_incl_voids',
            # Counts
            'qty', 'guest_count', 'table_size', 'calculated_total',
            'qty_sold', 'void_qty', 'item_qty', 'item_qty_incl_voids', 'num_orders',
            # Percentages
            'pct_of_net_sales', 'pct_of_ttl_qty_incl_voids', 'pct_of_ttl_amt_incl_voids',
            'pct_of_ttl_num_orders', 'pct_qty_group', 'pct_qty_menu', 'pct_qty_all',
            'pct_net_amt_group', 'pct_net_amt_menu', 'pct_net_amt_all',
            # Integer IDs (keep as numeric)
            'order_id', 'check_id', 'payment_id', 'order_number', 'check_number',
            'item_selection_id', 'id', 'entry_id'
        }

        for col in df.columns:
            # Skip processing_date - it's handled specially
            if col == 'processing_date':
                continue

            # Numeric columns: ensure they're float64
            if col in numeric_columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception:
                    pass
                continue

            # Boolean columns: keep as bool
            if str(df[col].dtype) in ('bool', 'boolean'):
                continue

            # Everything else: convert to string with explicit dtype
            # This includes datetime columns, text columns, IDs, etc.
            df[col] = df[col].apply(
                lambda x: str(x) if pd.notna(x) and str(x) != '' else None
            )
            # Force the column to be object type to ensure pyarrow treats it as string
            df[col] = df[col].astype('object')

        # Final pass: ensure all non-numeric columns are strings
        for col in df.columns:
            if col == 'processing_date':
                continue
            if col not in numeric_columns:
                if df[col].dtype != 'object':
                    df[col] = df[col].astype('object')

        return df

    def transform_dataframe(
        self, 
        df: pd.DataFrame, 
        config: Dict,
        processing_date: str
    ) -> pd.DataFrame:
        """Apply transformations to DataFrame"""
        
        # Rename columns using mapping
        column_mapping = config.get("column_mapping", {})
        df = df.rename(columns=column_mapping)
        
        # Convert remaining column names to snake_case and sanitize for BigQuery
        # BigQuery column names: letters, numbers, underscores only; must start with letter or underscore
        import re
        def sanitize_column_name(col: str) -> str:
            name = col.lower()
            name = name.replace(' ', '_')
            name = name.replace('#', 'number')
            name = name.replace('?', '')
            name = name.replace('/', '_')
            name = name.replace('(', '').replace(')', '')
            name = name.replace('-', '_')
            name = name.replace('.', '_')
            # Remove any remaining invalid characters
            name = re.sub(r'[^a-z0-9_]', '', name)
            # Ensure it starts with letter or underscore
            if name and name[0].isdigit():
                name = '_' + name
            return name

        df.columns = [
            column_mapping.get(col, sanitize_column_name(col))
            for col in df.columns
        ]
        
        # Parse datetime columns - convert to ISO string format for BigQuery
        date_columns = config.get("date_columns", [])
        for col in date_columns:
            mapped_col = column_mapping.get(col, col.lower().replace(' ', '_'))
            if mapped_col in df.columns:
                # Convert to ISO string format - BigQuery will store as STRING
                df[mapped_col] = df[mapped_col].apply(self.parse_toast_datetime)
        
        # Handle duration columns
        if 'duration_opened_to_paid' in df.columns:
            df['duration_opened_to_paid'] = df['duration_opened_to_paid'].apply(self.parse_duration)
        
        # Add computed columns
        df['processing_date'] = pd.to_datetime(processing_date).date()
        
        # Calculate total if applicable
        if all(col in df.columns for col in ['amount', 'tax', 'tip', 'gratuity']):
            df['calculated_total'] = df['amount'] + df['tax'] + df['tip'] + df['gratuity']
        
        # Convert boolean columns - use string 'true'/'false' for reliable BigQuery loading
        bool_columns = ['voided', 'deferred', 'tax_exempt']
        for col in bool_columns:
            if col in df.columns:
                def to_bool_string(x):
                    if pd.isna(x) or x == '':
                        return None
                    x_str = str(x).lower().strip()
                    if x_str in ('true', 'yes', '1', 'y'):
                        return 'true'
                    elif x_str in ('false', 'no', '0', 'n'):
                        return 'false'
                    return None
                df[col] = df[col].apply(to_bool_string)

        # Prepare datatypes for BigQuery (handle nullable integers and floats)
        df = self.prepare_for_bigquery(df)

        return df


class BigQueryLoader:
    """Handles BigQuery load operations"""
    
    def __init__(self, client: bigquery.Client, dataset_id: str):
        self.client = client
        self.dataset_id = dataset_id
    
    def get_table_ref(self, table_loc: str) -> str:
        return f"{PROJECT_ID}.{self.dataset_id}.{table_loc}"
    
    def table_exists(self, table_loc: str) -> bool:
        try:
            self.client.get_table(self.get_table_ref(table_loc))
            return True
        except NotFound:
            return False
    
    def create_table_from_df(self, df: pd.DataFrame, table_loc: str):
        """Create table with explicit schema derived from DataFrame"""
        table_ref = self.get_table_ref(table_loc)

        # Build explicit schema - don't rely on autodetect
        schema = []
        for col in df.columns:
            dtype = str(df[col].dtype)

            if col == 'processing_date':
                bq_type = 'DATE'
            elif dtype in ('int64', 'Int64'):
                bq_type = 'INT64'
            elif dtype == 'float64':
                bq_type = 'FLOAT64'
            elif dtype in ('bool', 'boolean'):
                bq_type = 'BOOL'
            else:
                # Default to STRING for all other types (including object)
                bq_type = 'STRING'

            schema.append(bigquery.SchemaField(col, bq_type, mode='NULLABLE'))

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Created table {table_loc} with {len(df)} rows")
    
    def upsert_data(
        self, 
        df: pd.DataFrame, 
        table_loc: str, 
        primary_key: List[str],
        processing_date: str
    ) -> Tuple[int, int]:
        """
        Upsert data using MERGE statement
        Returns (rows_inserted, rows_updated)
        """
        if df.empty:
            return 0, 0
        
        # Create temp table
        temp_table = f"{table_loc}_temp_{processing_date.replace('-', '')}"
        temp_ref = self.get_table_ref(temp_table)
        
        # Load to temp table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        job = self.client.load_table_from_dataframe(df, temp_ref, job_config=job_config)
        job.result()
        
        # Build MERGE statement
        target_ref = self.get_table_ref(table_loc)
        
        # Build join condition
        join_conditions = " AND ".join([f"T.{pk} = S.{pk}" for pk in primary_key])
        
        # Build update columns (exclude primary keys)
        update_cols = [col for col in df.columns if col not in primary_key]
        update_set = ", ".join([f"T.{col} = S.{col}" for col in update_cols])
        
        # Build insert columns
        all_cols = ", ".join(df.columns)
        source_cols = ", ".join([f"S.{col}" for col in df.columns])
        
        merge_sql = f"""
        MERGE `{target_ref}` T
        USING `{temp_ref}` S
        ON {join_conditions}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({all_cols})
            VALUES ({source_cols})
        """
        
        # Execute merge
        query_job = self.client.query(merge_sql)
        result = query_job.result()
        
        # Get stats
        rows_affected = query_job.num_dml_affected_rows or 0
        
        # Clean up temp table
        self.client.delete_table(temp_ref, not_found_ok=True)
        
        logger.info(f"Merged {rows_affected} rows into {table_loc}")
        return rows_affected, 0  # BigQuery MERGE doesn't separate insert/update counts
    
    def append_data(self, df: pd.DataFrame, table_loc: str) -> int:
        """Append data to existing table"""
        if df.empty:
            return 0
        
        table_ref = self.get_table_ref(table_loc)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        
        return len(df)
    
    def delete_date_partition(self, table_loc: str, processing_date: str):
        """Delete existing data for a processing date"""
        table_ref = self.get_table_ref(table_loc)
        
        delete_sql = f"""
        DELETE FROM `{table_ref}`
        WHERE processing_date = '{processing_date}'
        """
        
        query_job = self.client.query(delete_sql)
        query_job.result()
        logger.info(f"Deleted existing data for {processing_date} from {table_loc}")


class AlertManager:
    """Handles alerting for pipeline events"""
    
    def __init__(self, slack_webhook: str = "", email: str = ""):
        self.slack_webhook = slack_webhook
        self.email = email
    
    def send_slack_alert(self, message: str, is_error: bool = False):
        """Send Slack notification"""
        if not self.slack_webhook:
            return
        
        import requests
        
        color = "#ff0000" if is_error else "#36a64f"
        payload = {
            "attachments": [{
                "color": color,
                "title": "Toast ETL Pipeline Alert",
                "text": message,
                "footer": "LOV3 Analytics Pipeline",
                "ts": int(datetime.now().timestamp())
            }]
        }
        
        try:
            requests.post(self.slack_webhook, json=payload, timeout=10)
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
    
    def send_summary_alert(self, summary: PipelineRunSummary):
        """Send pipeline run summary"""
        duration = (summary.end_time - summary.start_time).total_seconds()
        
        status_emoji = "✅" if summary.status == "success" else "❌"
        
        message = f"""
{status_emoji} *Pipeline Run Complete*
• Run ID: `{summary.run_id}`
• Date Processed: {summary.processing_date}
• Status: {summary.status.upper()}
• Duration: {duration:.1f}s
• Files: {summary.files_processed} processed, {summary.files_failed} failed
• Total Rows: {summary.total_rows:,}
"""
        
        if summary.errors:
            message += "\n*Errors:*\n" + "\n".join([f"• {e}" for e in summary.errors[:5]])
        
        self.send_slack_alert(message, is_error=summary.status != "success")


class WeeklyReportGenerator:
    """Generates and sends weekly summary reports via email"""

    def __init__(self):
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.secret_manager = SecretManager(PROJECT_ID)

    def get_week_dates(self, week_ending: str = None) -> Tuple[str, str]:
        """
        Calculate the Monday-Sunday date range for the prior week.

        Args:
            week_ending: Optional date string (YYYYMMDD) for the Sunday ending the week.
                        Defaults to last Sunday.

        Returns:
            Tuple of (monday_date, sunday_date) as YYYY-MM-DD strings
        """
        if week_ending:
            end_date = datetime.strptime(week_ending, "%Y%m%d")
        else:
            # Find last Sunday
            today = datetime.now()
            days_since_sunday = (today.weekday() + 1) % 7
            if days_since_sunday == 0:
                days_since_sunday = 7  # If today is Sunday, go to previous Sunday
            end_date = today - timedelta(days=days_since_sunday)

        # Monday is 6 days before Sunday
        start_date = end_date - timedelta(days=6)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def query_revenue_summary(self, start_date: str, end_date: str) -> Dict:
        """Query total revenue, tax, tips, and average check size"""
        query = f"""
        SELECT
            COALESCE(SUM(amount), 0) as total_revenue,
            COALESCE(SUM(tax), 0) as total_tax,
            COALESCE(SUM(tip), 0) as total_tips,
            COALESCE(SUM(gratuity), 0) as total_gratuity,
            COALESCE(SUM(total), 0) as grand_total,
            COALESCE(AVG(total), 0) as avg_check_size,
            COUNT(*) as total_checks
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        result = list(self.bq_client.query(query).result())[0]
        return {
            "total_revenue": float(result.total_revenue or 0),
            "total_tax": float(result.total_tax or 0),
            "total_tips": float(result.total_tips or 0),
            "total_gratuity": float(result.total_gratuity or 0),
            "grand_total": float(result.grand_total or 0),
            "avg_check_size": float(result.avg_check_size or 0),
            "total_checks": int(result.total_checks or 0)
        }

    def query_order_metrics(self, start_date: str, end_date: str) -> Dict:
        """Query order counts, guest counts, and orders by dining option"""
        # Total orders and guests
        totals_query = f"""
        SELECT
            COUNT(DISTINCT order_id) as total_orders,
            COALESCE(SUM(guest_count), 0) as total_guests
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        totals = list(self.bq_client.query(totals_query).result())[0]

        # Orders by dining option - consolidate duplicates like "Bar, Bar" into "Bar"
        dining_query = f"""
        SELECT
            COALESCE(
                TRIM(SPLIT(dining_options, ',')[SAFE_OFFSET(0)]),
                'Unknown'
            ) as dining_option,
            COUNT(DISTINCT order_id) as order_count,
            COALESCE(SUM(total), 0) as revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        GROUP BY dining_option
        ORDER BY revenue DESC
        """
        dining_results = list(self.bq_client.query(dining_query).result())

        return {
            "total_orders": int(totals.total_orders or 0),
            "total_guests": int(totals.total_guests or 0),
            "by_dining_option": [
                {
                    "option": row.dining_option,
                    "orders": int(row.order_count),
                    "revenue": float(row.revenue or 0)
                }
                for row in dining_results
            ]
        }

    def query_top_items(self, start_date: str, end_date: str) -> Dict:
        """Query top 10 menu items by quantity and by revenue"""
        # Top by quantity
        qty_query = f"""
        SELECT
            menu_item,
            SUM(qty) as total_qty,
            SUM(net_price) as total_revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
            AND menu_item IS NOT NULL
        GROUP BY menu_item
        ORDER BY total_qty DESC
        LIMIT 10
        """
        by_qty = list(self.bq_client.query(qty_query).result())

        # Top by revenue
        rev_query = f"""
        SELECT
            menu_item,
            SUM(qty) as total_qty,
            SUM(net_price) as total_revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
            AND menu_item IS NOT NULL
        GROUP BY menu_item
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        by_rev = list(self.bq_client.query(rev_query).result())

        return {
            "by_quantity": [
                {
                    "item": row.menu_item,
                    "quantity": int(row.total_qty or 0),
                    "revenue": float(row.total_revenue or 0)
                }
                for row in by_qty
            ],
            "by_revenue": [
                {
                    "item": row.menu_item,
                    "quantity": int(row.total_qty or 0),
                    "revenue": float(row.total_revenue or 0)
                }
                for row in by_rev
            ]
        }

    def query_server_performance(self, start_date: str, end_date: str) -> List[Dict]:
        """Query revenue and order count by server with gratuity split"""
        query = f"""
        SELECT
            COALESCE(server, 'Unknown') as server_name,
            COUNT(DISTINCT order_id) as order_count,
            COALESCE(SUM(total), 0) as total_revenue,
            COALESCE(SUM(tip), 0) as total_tips,
            COALESCE(SUM(gratuity), 0) as total_gratuity
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        GROUP BY server
        ORDER BY total_revenue DESC
        LIMIT 15
        """
        results = list(self.bq_client.query(query).result())
        return [
            {
                "server": row.server_name,
                "orders": int(row.order_count),
                "revenue": float(row.total_revenue or 0),
                "tips": float(row.total_tips or 0),
                "gratuity": float(row.total_gratuity or 0),
                "server_grat": float(row.total_gratuity or 0) * 0.70,
                "lov3_grat": float(row.total_gratuity or 0) * 0.30
            }
            for row in results
        ]

    def query_daily_breakdown(self, start_date: str, end_date: str) -> List[Dict]:
        """Query revenue and orders per day with prior week comparison"""
        query = f"""
        WITH current_week AS (
            SELECT
                processing_date,
                FORMAT_DATE('%A', processing_date) as day_name,
                COUNT(DISTINCT order_id) as order_count,
                COALESCE(SUM(total), 0) as total_revenue,
                COALESCE(SUM(guest_count), 0) as guest_count
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
                AND (voided IS NULL OR voided = 'false')
            GROUP BY processing_date
        ),
        prior_week AS (
            SELECT
                DATE_ADD(processing_date, INTERVAL 7 DAY) as matching_date,
                COALESCE(SUM(total), 0) as prior_revenue
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN DATE_SUB(DATE '{start_date}', INTERVAL 7 DAY)
                AND DATE_SUB(DATE '{end_date}', INTERVAL 7 DAY)
                AND (voided IS NULL OR voided = 'false')
            GROUP BY processing_date
        )
        SELECT
            c.processing_date,
            c.day_name,
            c.order_count,
            c.total_revenue,
            c.guest_count,
            COALESCE(p.prior_revenue, 0) as prior_revenue
        FROM current_week c
        LEFT JOIN prior_week p ON c.processing_date = p.matching_date
        ORDER BY c.processing_date
        """
        results = list(self.bq_client.query(query).result())
        daily_data = []
        for row in results:
            revenue = float(row.total_revenue or 0)
            prior = float(row.prior_revenue or 0)
            pct_change = ((revenue - prior) / prior * 100) if prior > 0 else 0
            daily_data.append({
                "date": str(row.processing_date),
                "day": row.day_name,
                "orders": int(row.order_count),
                "revenue": revenue,
                "guests": int(row.guest_count or 0),
                "prior_revenue": prior,
                "pct_change": round(pct_change, 1)
            })
        return daily_data

    def query_payment_types(self, start_date: str, end_date: str) -> List[Dict]:
        """Query payment breakdown by type"""
        query = f"""
        SELECT
            COALESCE(payment_type, 'Unknown') as payment_type,
            COUNT(*) as transaction_count,
            COALESCE(SUM(total), 0) as total_amount
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY payment_type
        ORDER BY total_amount DESC
        """
        results = list(self.bq_client.query(query).result())
        return [
            {
                "type": row.payment_type,
                "transactions": int(row.transaction_count),
                "amount": float(row.total_amount or 0)
            }
            for row in results
        ]

    def query_week_over_week(self, start_date: str, end_date: str) -> Dict:
        """Compare current week vs prior week and same week last year"""
        current_start = datetime.strptime(start_date, "%Y-%m-%d")
        prior_start = (current_start - timedelta(days=7)).strftime("%Y-%m-%d")
        prior_end = (current_start - timedelta(days=1)).strftime("%Y-%m-%d")
        ly_start = (current_start - timedelta(weeks=52)).strftime("%Y-%m-%d")
        ly_end = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(weeks=52)).strftime("%Y-%m-%d")

        query = f"""
        WITH current_week AS (
            SELECT
                SUM(total) as revenue,
                COUNT(DISTINCT order_id) as orders,
                SUM(guest_count) as guests,
                SUM(tip) as tips,
                AVG(total) as avg_check
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
                AND (voided IS NULL OR voided = 'false')
        ),
        prior_week AS (
            SELECT
                SUM(total) as revenue,
                COUNT(DISTINCT order_id) as orders,
                SUM(guest_count) as guests
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{prior_start}' AND '{prior_end}'
                AND (voided IS NULL OR voided = 'false')
        ),
        last_year AS (
            SELECT
                SUM(total) as revenue,
                COUNT(DISTINCT order_id) as orders
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{ly_start}' AND '{ly_end}'
                AND (voided IS NULL OR voided = 'false')
        )
        SELECT
            COALESCE(c.revenue, 0) as current_revenue,
            COALESCE(c.orders, 0) as current_orders,
            COALESCE(c.guests, 0) as current_guests,
            COALESCE(c.tips, 0) as current_tips,
            COALESCE(c.avg_check, 0) as avg_check,
            COALESCE(p.revenue, 0) as prior_revenue,
            COALESCE(p.orders, 0) as prior_orders,
            COALESCE(p.guests, 0) as prior_guests,
            COALESCE(ly.revenue, 0) as ly_revenue,
            COALESCE(ly.orders, 0) as ly_orders
        FROM current_week c, prior_week p, last_year ly
        """
        result = list(self.bq_client.query(query).result())[0]

        current_revenue = float(result.current_revenue or 0)
        prior_revenue = float(result.prior_revenue or 0)
        ly_revenue = float(result.ly_revenue or 0)

        wow_change = ((current_revenue - prior_revenue) / prior_revenue * 100) if prior_revenue > 0 else 0
        yoy_change = ((current_revenue - ly_revenue) / ly_revenue * 100) if ly_revenue > 0 else 0

        current_orders = int(result.current_orders or 0)
        prior_orders = int(result.prior_orders or 0)
        orders_change = ((current_orders - prior_orders) / prior_orders * 100) if prior_orders > 0 else 0

        return {
            "current_week": {
                "revenue": current_revenue,
                "orders": current_orders,
                "guests": int(result.current_guests or 0),
                "tips": float(result.current_tips or 0),
                "avg_check": float(result.avg_check or 0),
                "orders_per_day": round(current_orders / 7, 1)
            },
            "prior_week": {
                "revenue": prior_revenue,
                "orders": prior_orders,
                "guests": int(result.prior_guests or 0)
            },
            "last_year": {
                "revenue": ly_revenue,
                "orders": int(result.ly_orders or 0)
            },
            "changes": {
                "revenue_pct": round(wow_change, 1),
                "orders_pct": round(orders_change, 1),
                "yoy_pct": round(yoy_change, 1)
            }
        }

    def query_product_mix(self, start_date: str, end_date: str) -> Dict:
        """Query product mix by sales category"""
        query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN sales_category = 'Liquor' THEN net_price ELSE 0 END), 0) as liquor_revenue,
            COALESCE(SUM(CASE WHEN sales_category = 'Food' THEN net_price ELSE 0 END), 0) as food_revenue,
            COALESCE(SUM(CASE WHEN sales_category = 'Hookah' THEN net_price ELSE 0 END), 0) as hookah_revenue,
            COALESCE(SUM(net_price), 0) as total_revenue,
            COALESCE(SUM(CASE WHEN LOWER(menu_item) LIKE '%btl%' THEN net_price ELSE 0 END), 0) as bottle_service
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        result = list(self.bq_client.query(query).result())[0]

        total = float(result.total_revenue or 1)
        liquor = float(result.liquor_revenue or 0)
        food = float(result.food_revenue or 0)
        hookah = float(result.hookah_revenue or 0)

        return {
            "liquor": {"revenue": liquor, "pct": round(liquor / total * 100, 1) if total > 0 else 0},
            "food": {"revenue": food, "pct": round(food / total * 100, 1) if total > 0 else 0},
            "hookah": {"revenue": hookah, "pct": round(hookah / total * 100, 1) if total > 0 else 0},
            "bottle_service": float(result.bottle_service or 0),
            "total": total
        }

    def query_high_check_analysis(self, start_date: str, end_date: str) -> Dict:
        """Query high-check rate (checks > $200)"""
        query = f"""
        SELECT
            COUNT(*) as total_checks,
            COUNTIF(total > 200) as high_checks
        FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        result = list(self.bq_client.query(query).result())[0]

        total = int(result.total_checks or 0)
        high = int(result.high_checks or 0)

        return {
            "total_checks": total,
            "high_checks": high,
            "high_check_rate": round(high / total * 100, 1) if total > 0 else 0,
            "target": 8.0,
            "status": "ON TARGET" if (high / total * 100 if total > 0 else 0) >= 8 else "BELOW TARGET"
        }

    def query_discount_void_control(self, start_date: str, end_date: str) -> Dict:
        """Query discount and void metrics"""
        discount_query = f"""
        SELECT
            COALESCE(SUM(discount_amount), 0) as total_discounts,
            COALESCE(SUM(amount + discount_amount), 0) as gross_plus_disc
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        disc_result = list(self.bq_client.query(discount_query).result())[0]

        void_query = f"""
        SELECT
            COUNT(DISTINCT payment_id) as total_payments,
            COUNTIF(void_date IS NOT NULL AND void_date != '') as voided_payments,
            COALESCE(SUM(CASE WHEN void_date IS NOT NULL AND void_date != '' THEN total ELSE 0 END), 0) as voided_amount
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        void_result = list(self.bq_client.query(void_query).result())[0]

        total_discounts = float(disc_result.total_discounts or 0)
        gross = float(disc_result.gross_plus_disc or 1)
        discount_rate = round(total_discounts / gross * 100, 1) if gross > 0 else 0

        total_payments = int(void_result.total_payments or 0)
        voided_payments = int(void_result.voided_payments or 0)
        void_rate = round(voided_payments / total_payments * 100, 2) if total_payments > 0 else 0

        return {
            "discounts": {
                "total": total_discounts,
                "gross_sales": gross,
                "rate": discount_rate,
                "benchmark": 5.0,
                "status": "OK" if discount_rate < 5 else "FLAG - HIGH DISCOUNTS"
            },
            "voids": {
                "total_payments": total_payments,
                "voided_payments": voided_payments,
                "voided_amount": float(void_result.voided_amount or 0),
                "rate": void_rate,
                "benchmark": 1.0,
                "status": "OK" if void_rate < 1 else "FLAG - HIGH VOIDS"
            }
        }

    def query_discount_breakdown(self, start_date: str, end_date: str) -> Dict:
        """Query discount breakdown by reason"""
        # Get gross sales for percentage calculation
        gross_query = f"""
        SELECT COALESCE(SUM(amount + discount_amount), 0) as gross_sales
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        gross_result = list(self.bq_client.query(gross_query).result())[0]
        gross_sales = float(gross_result.gross_sales or 1)

        query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Manager Comp%' THEN discount ELSE 0 END), 0) as manager_comp,
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Open $%' OR reason_of_discount LIKE '%Open %%' THEN discount ELSE 0 END), 0) as open_discount,
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Owner Comp%' THEN discount ELSE 0 END), 0) as owner_comp,
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Birthday%' THEN discount ELSE 0 END), 0) as birthday_comp,
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Spillage%' OR reason_of_discount LIKE '%Quality%' THEN discount ELSE 0 END), 0) as spillage_quality
        FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND reason_of_discount IS NOT NULL
            AND reason_of_discount != ''
        """
        result = list(self.bq_client.query(query).result())[0]

        manager_comp = float(result.manager_comp or 0)
        open_discount = float(result.open_discount or 0)
        mgr_comp_pct = round(manager_comp / gross_sales * 100, 2) if gross_sales > 0 else 0

        return {
            "manager_comp": {
                "total": manager_comp,
                "pct": mgr_comp_pct,
                "threshold_pct": 4.0,
                "status": "FLAG - EXCEEDS 4%" if mgr_comp_pct > 4 else "OK"
            },
            "open_discount": {
                "total": open_discount,
                "threshold": 0,
                "status": "FLAG - SHOULD BE $0" if open_discount > 0 else "OK"
            },
            "owner_comp": float(result.owner_comp or 0),
            "birthday_comp": float(result.birthday_comp or 0),
            "spillage_quality": float(result.spillage_quality or 0)
        }

    def query_server_flags(self, start_date: str, end_date: str) -> Dict:
        """Query servers with low tip rate, high discount, or high void rate"""
        # Low tip rate (<6%)
        low_tip_query = f"""
        SELECT server, order_count, weekly_revenue, total_tips, tip_rate_pct
        FROM (
            SELECT
                server,
                COUNT(DISTINCT order_id) as order_count,
                ROUND(SUM(total), 2) as weekly_revenue,
                ROUND(SUM(tip), 2) as total_tips,
                ROUND(SUM(tip) * 100.0 / NULLIF(SUM(amount), 0), 1) as tip_rate_pct
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY server
            HAVING COUNT(DISTINCT order_id) >= 10
        )
        WHERE tip_rate_pct < 6
        ORDER BY tip_rate_pct
        """
        low_tip = list(self.bq_client.query(low_tip_query).result())

        # High discount rate (>15%)
        high_disc_query = f"""
        SELECT server, order_count, weekly_revenue, total_discounts, discount_rate_pct
        FROM (
            SELECT
                server,
                COUNT(DISTINCT order_id) as order_count,
                ROUND(SUM(total), 2) as weekly_revenue,
                ROUND(SUM(discount_amount), 2) as total_discounts,
                ROUND(SUM(discount_amount) * 100.0 / NULLIF(SUM(amount + discount_amount), 0), 1) as discount_rate_pct
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY server
            HAVING COUNT(DISTINCT order_id) >= 10
        )
        WHERE discount_rate_pct > 15
        ORDER BY discount_rate_pct DESC
        """
        high_disc = list(self.bq_client.query(high_disc_query).result())

        # High void rate (>2%)
        high_void_query = f"""
        SELECT server, total_payments, voided_payments, void_rate_pct, voided_amount
        FROM (
            SELECT
                server,
                COUNT(DISTINCT payment_id) as total_payments,
                COUNTIF(void_date IS NOT NULL AND void_date != '') as voided_payments,
                ROUND(COUNTIF(void_date IS NOT NULL AND void_date != '') * 100.0 / NULLIF(COUNT(DISTINCT payment_id), 0), 2) as void_rate_pct,
                ROUND(SUM(CASE WHEN void_date IS NOT NULL AND void_date != '' THEN total ELSE 0 END), 2) as voided_amount
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY server
            HAVING COUNT(DISTINCT payment_id) >= 10
        )
        WHERE void_rate_pct > 2
        ORDER BY void_rate_pct DESC
        """
        high_void = list(self.bq_client.query(high_void_query).result())

        return {
            "low_tip": [{"server": r.server, "orders": r.order_count, "revenue": float(r.weekly_revenue), "tips": float(r.total_tips), "tip_rate": float(r.tip_rate_pct)} for r in low_tip],
            "high_discount": [{"server": r.server, "orders": r.order_count, "revenue": float(r.weekly_revenue), "discounts": float(r.total_discounts), "discount_rate": float(r.discount_rate_pct)} for r in high_disc],
            "high_void": [{"server": r.server, "payments": r.total_payments, "voided": r.voided_payments, "void_rate": float(r.void_rate_pct), "voided_amount": float(r.voided_amount)} for r in high_void]
        }

    def query_cash_control(self, start_date: str, end_date: str) -> Dict:
        """Query cash control metrics"""
        cash_query = f"""
        SELECT
            COUNTIF(payment_type = 'Cash' OR payment_type LIKE '%CASH%') as cash_payments,
            COUNT(DISTINCT payment_id) as total_payments
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        cash_result = list(self.bq_client.query(cash_query).result())[0]

        entries_query = f"""
        SELECT
            COUNTIF(action = 'NO_SALE') as no_sale_count,
            COALESCE(SUM(CASE WHEN action = 'CLOSE_OUT_OVERAGE' THEN amount ELSE 0 END), 0) as cash_overage,
            COALESCE(SUM(CASE WHEN action = 'CLOSE_OUT_SHORTAGE' THEN amount ELSE 0 END), 0) as cash_shortage
        FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        entries_result = list(self.bq_client.query(entries_query).result())[0]

        cash_payments = int(cash_result.cash_payments or 0)
        total_payments = int(cash_result.total_payments or 1)
        cash_pct = round(cash_payments / total_payments * 100, 1) if total_payments > 0 else 0

        no_sale = int(entries_result.no_sale_count or 0)
        overage = float(entries_result.cash_overage or 0)
        shortage = float(entries_result.cash_shortage or 0)
        variance = abs(overage) + abs(shortage)

        return {
            "cash_pct": cash_pct,
            "cash_payments": cash_payments,
            "total_payments": total_payments,
            "cash_benchmark": 17.0,
            "cash_status": "OK" if 14 <= cash_pct <= 20 else "REVIEW",
            "no_sale_count": no_sale,
            "no_sale_threshold": 100,
            "no_sale_status": "FLAG - HIGH NO_SALE" if no_sale > 100 else "OK",
            "overage": overage,
            "shortage": shortage,
            "total_variance": variance,
            "variance_threshold": 50,
            "variance_status": "FLAG - HIGH VARIANCE" if variance > 50 else "OK"
        }

    def query_top_cash_handlers(self, start_date: str, end_date: str) -> List[Dict]:
        """Query top cash handlers"""
        query = f"""
        SELECT employee, entry_count, cash_collected, no_sale_count, payout_count
        FROM (
            SELECT
                employee,
                COUNT(*) as entry_count,
                ROUND(SUM(CASE WHEN action = 'CASH_COLLECTED' THEN amount ELSE 0 END), 2) as cash_collected,
                COUNTIF(action = 'NO_SALE') as no_sale_count,
                COUNTIF(action = 'PAY_OUT') as payout_count,
                ROW_NUMBER() OVER (ORDER BY SUM(CASE WHEN action = 'CASH_COLLECTED' THEN amount ELSE 0 END) DESC) as rank_num
            FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY employee
        )
        WHERE rank_num <= 10
        ORDER BY rank_num
        """
        results = list(self.bq_client.query(query).result())
        return [{"employee": r.employee, "entries": r.entry_count, "cash_collected": float(r.cash_collected or 0), "no_sales": r.no_sale_count, "payouts": r.payout_count} for r in results]

    def query_operational_efficiency(self, start_date: str, end_date: str) -> Dict:
        """Query kitchen fulfillment and operational metrics"""
        kitchen_query = f"""
        SELECT
            COUNT(*) as total_tickets,
            COUNTIF(fulfilled_date IS NOT NULL AND fulfilled_date != '') as fulfilled_tickets
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        kitchen_result = list(self.bq_client.query(kitchen_query).result())[0]

        total_tickets = int(kitchen_result.total_tickets or 0)
        fulfilled = int(kitchen_result.fulfilled_tickets or 0)
        fulfillment_rate = round(fulfilled / total_tickets * 100, 1) if total_tickets > 0 else 0

        # Station performance
        station_query = f"""
        SELECT
            station,
            COUNT(*) as ticket_count,
            COUNTIF(fulfilled_date IS NOT NULL AND fulfilled_date != '') as fulfilled_count
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY station
        ORDER BY ticket_count DESC
        """
        stations = list(self.bq_client.query(station_query).result())

        return {
            "total_tickets": total_tickets,
            "fulfilled_tickets": fulfilled,
            "fulfillment_rate": fulfillment_rate,
            "fulfillment_target": 99.0,
            "fulfillment_status": "OK" if fulfillment_rate >= 99 else "FLAG",
            "stations": [{"station": s.station, "tickets": s.ticket_count, "fulfilled": s.fulfilled_count, "rate": round(s.fulfilled_count / s.ticket_count * 100, 1) if s.ticket_count > 0 else 0} for s in stations]
        }

    def query_weekly_scorecard(self, start_date: str, end_date: str) -> Dict:
        """Generate weekly scorecard summary"""
        rev_query = f"""
        SELECT ROUND(SUM(total), 2) as weekly_revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        rev = list(self.bq_client.query(rev_query).result())[0]

        disc_query = f"""
        SELECT ROUND(SUM(discount_amount) * 100.0 / NULLIF(SUM(amount + discount_amount), 0), 1) as discount_rate
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        disc = list(self.bq_client.query(disc_query).result())[0]

        void_query = f"""
        SELECT ROUND(COUNTIF(void_date IS NOT NULL AND void_date != '') * 100.0 / NULLIF(COUNT(*), 0), 2) as void_rate
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        void = list(self.bq_client.query(void_query).result())[0]

        cash_query = f"""
        SELECT COUNTIF(action = 'NO_SALE') as no_sale_count
        FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        cash = list(self.bq_client.query(cash_query).result())[0]

        kitchen_query = f"""
        SELECT ROUND(COUNTIF(fulfilled_date IS NOT NULL AND fulfilled_date != '') * 100.0 / NULLIF(COUNT(*), 0), 1) as fulfillment_rate
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        kitchen = list(self.bq_client.query(kitchen_query).result())[0]

        weekly_revenue = float(rev.weekly_revenue or 0)
        discount_rate = float(disc.discount_rate or 0)
        void_rate = float(void.void_rate or 0)
        no_sale_count = int(cash.no_sale_count or 0)
        fulfillment_rate = float(kitchen.fulfillment_rate or 0)

        return {
            "revenue": {"value": weekly_revenue, "target": 100000, "status": "PASS" if weekly_revenue >= 100000 else "BELOW TARGET"},
            "discount": {"value": discount_rate, "target": 8, "status": "PASS" if discount_rate < 8 else "REVIEW"},
            "void": {"value": void_rate, "target": 1, "status": "PASS" if void_rate < 1 else "REVIEW"},
            "cash": {"value": no_sale_count, "target": 100, "status": "PASS" if no_sale_count <= 100 else "REVIEW"},
            "kitchen": {"value": fulfillment_rate, "target": 99, "status": "PASS" if fulfillment_rate >= 99 else "REVIEW"}
        }

    # ─── Business-Day-Aware Queries ─────────────────────────────────────────
    # These methods use the 4AM cutoff to assign revenue to the correct
    # business day. LOV3 is a nightlife venue: revenue at 1 AM Saturday
    # belongs to Friday's business day.

    def query_revenue_by_business_day(self, start_date: str, end_date: str) -> List[Dict]:
        """Revenue breakdown by day-of-week using the 4AM business day cutoff.

        paid_date in PaymentDetails_raw is STRING, so we CAST to DATETIME
        before applying the business-day logic.
        """
        bd = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
        dow = BUSINESS_DOW_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
        query = f"""
        WITH business AS (
            SELECT
                {bd} AS business_date,
                {dow} AS dow_name,
                EXTRACT(DAYOFWEEK FROM {bd}) AS dow_num,
                amount, tip, gratuity, total
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
                AND (void_date IS NULL OR void_date = '')
                AND paid_date IS NOT NULL AND paid_date != ''
        )
        SELECT
            dow_name,
            dow_num,
            COUNT(*) AS txn_count,
            ROUND(SUM(amount), 2) AS net_sales,
            ROUND(SUM(tip), 2) AS tips,
            ROUND(SUM(gratuity), 2) AS gratuity,
            ROUND(SUM(total), 2) AS gross_revenue,
            ROUND(AVG(total), 2) AS avg_check,
            COUNT(DISTINCT business_date) AS num_days
        FROM business
        GROUP BY dow_name, dow_num
        ORDER BY dow_num
        """
        rows = list(self.bq_client.query(query).result())
        return [
            {
                "day": row.dow_name,
                "txn_count": int(row.txn_count or 0),
                "net_sales": float(row.net_sales or 0),
                "tips": float(row.tips or 0),
                "gratuity": float(row.gratuity or 0),
                "gross_revenue": float(row.gross_revenue or 0),
                "avg_check": float(row.avg_check or 0),
                "num_days": int(row.num_days or 0),
                "avg_daily_revenue": round(
                    float(row.gross_revenue or 0) / max(int(row.num_days or 1), 1), 2
                ),
            }
            for row in rows
        ]

    def query_monthly_pnl(self, start_date: str, end_date: str) -> List[Dict]:
        """Monthly P&L combining Toast revenue with bank expenses.

        Uses centralized LOV3 business assumptions for gratuity split,
        cash reconciliation, and true labor calculation.
        """
        bq = self.bq_client

        # Monthly revenue from Toast
        rev_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', processing_date) AS month,
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS tips,
            COALESCE(SUM(gratuity), 0) AS gratuity,
            COALESCE(SUM(total), 0) AS gross_revenue,
            COUNT(DISTINCT order_id) AS order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        GROUP BY month ORDER BY month
        """
        rev_rows = {r.month: r for r in bq.query(rev_query).result()}

        # Monthly bank expenses (debits)
        # transaction_date is STRING in BankTransactions_raw, must CAST to DATE
        exp_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', CAST(transaction_date AS DATE)) AS month,
            category,
            ROUND(SUM(abs_amount), 2) AS total
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND transaction_type = 'debit'
        GROUP BY month, category
        ORDER BY month, total DESC
        """
        exp_rows = list(bq.query(exp_query).result())

        # Monthly cash collected (Toast) vs deposited (bank)
        cash_toast_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', processing_date) AS month,
            COALESCE(SUM(CASE WHEN payment_type = 'Cash' OR payment_type LIKE '%CASH%'
                         THEN total ELSE 0 END), 0) AS cash_collected
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY month
        """
        cash_toast = {r.month: float(r.cash_collected or 0)
                      for r in bq.query(cash_toast_query).result()}

        cash_bank_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', CAST(transaction_date AS DATE)) AS month,
            COALESCE(SUM(abs_amount), 0) AS cash_deposited
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND transaction_type = 'credit'
            AND (LOWER(category) LIKE '%cash deposit%'
                 OR LOWER(category) LIKE '%cash account transfer%'
                 OR LOWER(description) LIKE '%counter credit%')
        GROUP BY month
        """
        cash_bank = {r.month: float(r.cash_deposited or 0)
                     for r in bq.query(cash_bank_query).result()}

        # Build expense dict by month
        expenses_by_month: Dict[str, Dict[str, float]] = {}
        for row in exp_rows:
            m = row.month
            if m not in expenses_by_month:
                expenses_by_month[m] = {}
            expenses_by_month[m][row.category] = float(row.total or 0)

        # Helper: sum categories matching keywords
        def _sum_matching(cats: Dict[str, float], keywords: List[str]) -> float:
            return sum(v for k, v in cats.items()
                       if any(kw.lower() in k.lower() for kw in keywords))

        all_months = sorted(set(list(rev_rows.keys()) +
                                list(expenses_by_month.keys())))
        results = []
        for m in all_months:
            rev = rev_rows.get(m)
            net_sales = float(rev.net_sales or 0) if rev else 0.0
            tips = float(rev.tips or 0) if rev else 0.0
            grat = float(rev.gratuity or 0) if rev else 0.0
            gross = float(rev.gross_revenue or 0) if rev else 0.0

            grat_retained = round(grat * GRAT_RETAIN_PCT, 2)
            pass_through = round(tips + grat * GRAT_PASSTHROUGH_PCT, 2)
            adj_revenue = round(net_sales + grat_retained, 2)

            cats = expenses_by_month.get(m, {})
            total_exp = sum(v for k, v in cats.items()
                            if "revenue" not in k.lower())
            cogs = _sum_matching(cats, ["cost of goods", "cogs"])
            labor_gross = _sum_matching(cats, ["3. labor", "labor cost", "payroll"])
            labor_true = round(max(labor_gross - pass_through, 0), 2)
            marketing = _sum_matching(cats, ["marketing", "promotions",
                                              "entertainment", "event"])
            opex = _sum_matching(cats, ["operating expenses", "opex"])

            adj_expenses = round(max(total_exp - pass_through, 0), 2)
            net_profit = round(adj_revenue - adj_expenses, 2)

            cash_coll = cash_toast.get(m, 0)
            cash_dep = cash_bank.get(m, 0)
            unreconciled = round(cash_coll - cash_dep, 2)

            rev_denom = adj_revenue if adj_revenue > 0 else 1
            results.append({
                "month": m,
                "net_sales": net_sales,
                "gratuity_retained": grat_retained,
                "adjusted_revenue": adj_revenue,
                "pass_through_to_staff": pass_through,
                "cogs": cogs,
                "cogs_pct": round(cogs / rev_denom * 100, 1),
                "labor_gross": labor_gross,
                "labor_true": labor_true,
                "labor_pct": round(labor_true / rev_denom * 100, 1),
                "marketing": marketing,
                "opex": opex,
                "total_expenses_adjusted": adj_expenses,
                "net_profit": net_profit,
                "margin_pct": round(net_profit / rev_denom * 100, 1),
                "cash_collected_toast": cash_coll,
                "cash_deposited_bank": cash_dep,
                "unreconciled_cash": unreconciled,
                "order_count": int(rev.order_count or 0) if rev else 0,
            })

        return results

    def query_hourly_revenue_profile(self, start_date: str, end_date: str) -> List[Dict]:
        """Hourly revenue profile using business-day-aware grouping.

        Shows what hours generate revenue, with proper attribution of
        post-midnight hours to the prior business day.
        """
        bd = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
        query = f"""
        WITH parsed AS (
            SELECT
                EXTRACT(HOUR FROM CAST(paid_date AS DATETIME)) AS hour_of_day,
                {bd} AS business_date,
                amount, tip, gratuity, total
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
                AND (void_date IS NULL OR void_date = '')
                AND paid_date IS NOT NULL AND paid_date != ''
        )
        SELECT
            hour_of_day,
            COUNT(*) AS txn_count,
            ROUND(SUM(total), 2) AS gross_revenue,
            ROUND(AVG(total), 2) AS avg_check,
            COUNT(DISTINCT business_date) AS num_days
        FROM parsed
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
        rows = list(self.bq_client.query(query).result())
        return [
            {
                "hour": int(row.hour_of_day),
                "txn_count": int(row.txn_count or 0),
                "gross_revenue": float(row.gross_revenue or 0),
                "avg_check": float(row.avg_check or 0),
                "num_days": int(row.num_days or 0),
                "avg_daily_revenue": round(
                    float(row.gross_revenue or 0) / max(int(row.num_days or 1), 1), 2
                ),
            }
            for row in rows
        ]

    def generate_insights(
        self,
        revenue: Dict,
        orders: Dict,
        top_items: Dict,
        servers: List[Dict],
        daily: List[Dict],
        payments: List[Dict],
        wow: Dict
    ) -> Tuple[List[str], List[str]]:
        """Generate key insights and recommendations from the data"""
        insights = []
        recommendations = []

        # Managers - don't suggest they share techniques or flag for tip issues
        MANAGERS = ["Tony Winn", "Sossity Taylor", "Tiffany Loving", "Ashley Baines"]
        # Bottle Manager is a pool of sales from multiple servers, not a single person
        POOL_ACCOUNTS = ["Bottle Manager"]

        def is_manager(name: str) -> bool:
            return any(mgr.lower() in name.lower() for mgr in MANAGERS)

        def is_pool_account(name: str) -> bool:
            return any(pool.lower() in name.lower() for pool in POOL_ACCOUNTS)

        # Revenue insights
        if wow["changes"]["revenue_pct"] > 10:
            insights.append(f"Strong revenue growth of {wow['changes']['revenue_pct']:.1f}% compared to last week")
        elif wow["changes"]["revenue_pct"] < -10:
            insights.append(f"Revenue declined {abs(wow['changes']['revenue_pct']):.1f}% from last week")
            recommendations.append("Review marketing efforts and consider promotions to boost traffic")

        # Average check analysis
        if revenue["total_checks"] > 0:
            avg_check = revenue["avg_check_size"]
            if avg_check > 50:
                insights.append(f"Strong average check size of ${avg_check:.2f}")
            elif avg_check < 25:
                insights.append(f"Average check size is ${avg_check:.2f}")
                recommendations.append("Train staff on upselling techniques to increase average check size")

        # Best/worst day analysis
        if daily:
            best_day = max(daily, key=lambda x: x["revenue"])
            worst_day = min(daily, key=lambda x: x["revenue"])
            insights.append(f"Best performing day: {best_day['day']} (${best_day['revenue']:,.2f})")
            insights.append(f"Slowest day: {worst_day['day']} (${worst_day['revenue']:,.2f})")
            if worst_day["revenue"] < best_day["revenue"] * 0.5:
                recommendations.append(f"Consider {worst_day['day']} specials or promotions to boost slow day sales")

        # Top performer insight
        if servers:
            top_server = servers[0]
            server_name = top_server['server']
            if is_pool_account(server_name):
                insights.append(f"Top revenue: {server_name} (pooled bottle service) with ${top_server['revenue']:,.2f} in sales")
            elif is_manager(server_name):
                insights.append(f"Top revenue: {server_name} (Manager) with ${top_server['revenue']:,.2f} in sales")
            else:
                insights.append(f"Top server: {server_name} with ${top_server['revenue']:,.2f} in sales")

            # Only suggest sharing techniques for non-managers and non-pool accounts
            if len(servers) > 1 and not is_manager(server_name) and not is_pool_account(server_name):
                avg_server_revenue = sum(s["revenue"] for s in servers) / len(servers)
                if top_server["revenue"] > avg_server_revenue * 1.5:
                    recommendations.append(f"Have {server_name} share sales techniques with the team")

        # Tip analysis
        if servers:
            total_tips = sum(s["tips"] for s in servers)
            total_server_revenue = sum(s["revenue"] for s in servers)
            if total_server_revenue > 0:
                tip_pct = (total_tips / total_server_revenue) * 100
                insights.append(f"Average tip rate: {tip_pct:.1f}%")
                if tip_pct < 15:
                    recommendations.append("Tip rate below industry average - focus on service quality")

        # Menu insights
        if top_items["by_quantity"]:
            top_item = top_items["by_quantity"][0]
            insights.append(f"Best seller: {top_item['item']} ({top_item['quantity']} sold)")

        if top_items["by_revenue"]:
            top_revenue_item = top_items["by_revenue"][0]
            if top_revenue_item["item"] != top_items["by_quantity"][0]["item"]:
                insights.append(f"Highest revenue item: {top_revenue_item['item']} (${top_revenue_item['revenue']:,.2f})")

        # Dining option insights
        if orders["by_dining_option"]:
            dine_in = next((d for d in orders["by_dining_option"] if "dine" in d["option"].lower()), None)
            takeout = next((d for d in orders["by_dining_option"] if "take" in d["option"].lower()), None)
            if dine_in and takeout:
                total_orders = orders["total_orders"]
                if total_orders > 0:
                    dine_in_pct = (dine_in["orders"] / total_orders) * 100
                    insights.append(f"Dine-in represents {dine_in_pct:.0f}% of orders")

        # Guest analysis
        if orders["total_guests"] > 0 and orders["total_orders"] > 0:
            avg_party_size = orders["total_guests"] / orders["total_orders"]
            insights.append(f"Average party size: {avg_party_size:.1f} guests")

        # Payment method insights
        if payments:
            cash_payment = next((p for p in payments if "cash" in p["type"].lower()), None)
            if cash_payment and revenue["grand_total"] > 0:
                cash_pct = (cash_payment["amount"] / revenue["grand_total"]) * 100
                insights.append(f"Cash transactions: {cash_pct:.1f}% of total")

        # General recommendations
        if orders["total_orders"] > 0 and wow["changes"]["orders_pct"] > 0:
            recommendations.append("Order volume is growing - ensure adequate staffing for peak times")

        if not recommendations:
            recommendations.append("Continue current operations - metrics are stable")

        return insights, recommendations

    def generate_html_report(
        self,
        start_date: str,
        end_date: str,
        revenue: Dict,
        orders: Dict,
        top_items: Dict,
        servers: List[Dict],
        daily: List[Dict],
        payments: List[Dict],
        wow: Dict,
        product_mix: Dict = None,
        high_check: Dict = None,
        disc_void: Dict = None,
        disc_breakdown: Dict = None,
        server_flags: Dict = None,
        cash_control: Dict = None,
        cash_handlers: List[Dict] = None,
        ops_efficiency: Dict = None,
        scorecard: Dict = None
    ) -> str:
        """Generate formatted HTML email report"""

        # Set defaults for optional params
        product_mix = product_mix or {}
        high_check = high_check or {}
        disc_void = disc_void or {"discounts": {}, "voids": {}}
        disc_breakdown = disc_breakdown or {}
        server_flags = server_flags or {"low_tip": [], "high_discount": [], "high_void": []}
        cash_control = cash_control or {}
        cash_handlers = cash_handlers or []
        ops_efficiency = ops_efficiency or {"stations": []}
        scorecard = scorecard or {}

        # Generate insights
        insights, recommendations = self.generate_insights(
            revenue, orders, top_items, servers, daily, payments, wow
        )

        # Format currency helper
        def fmt_currency(val: float) -> str:
            return f"${val:,.2f}"

        # Format percentage with arrow
        def fmt_change(val: float) -> str:
            arrow = "▲" if val > 0 else "▼" if val < 0 else "→"
            color = "#22c55e" if val > 0 else "#ef4444" if val < 0 else "#6b7280"
            return f'<span style="color: {color}">{arrow} {abs(val):.1f}%</span>'

        # Build top items by quantity table
        top_items_qty_html = ""
        for i, item in enumerate(top_items["by_quantity"][:10], 1):
            top_items_qty_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{i}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{item['item']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{item['quantity']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(item['revenue'])}</td>
            </tr>
            """

        # Build top items by revenue table
        top_items_rev_html = ""
        for i, item in enumerate(top_items["by_revenue"][:10], 1):
            top_items_rev_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{i}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{item['item']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{item['quantity']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(item['revenue'])}</td>
            </tr>
            """

        # Build server performance table with gratuity split
        servers_html = ""
        for server in servers[:10]:
            servers_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{server['server']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{server['orders']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server['revenue'])}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server['tips'])}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server.get('gratuity', 0))}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server.get('server_grat', 0))}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server.get('lov3_grat', 0))}</td>
            </tr>
            """

        # Build daily breakdown table with prior week comparison
        daily_html = ""
        for day in daily:
            pct_chg = day.get('pct_change', 0)
            pct_color = '#22c55e' if pct_chg > 0 else '#ef4444' if pct_chg < 0 else '#6b7280'
            pct_arrow = '▲' if pct_chg > 0 else '▼' if pct_chg < 0 else '→'
            daily_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{day['day']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{day['date']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{day['orders']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{day['guests']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(day['revenue'])}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right; color: #6b7280;">{fmt_currency(day.get('prior_revenue', 0))}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right; color: {pct_color};">{pct_arrow} {abs(pct_chg):.1f}%</td>
            </tr>
            """

        # Build payment types table
        payments_html = ""
        for pmt in payments:
            payments_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{pmt['type']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{pmt['transactions']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(pmt['amount'])}</td>
            </tr>
            """

        # Build dining options table
        dining_html = ""
        for opt in orders["by_dining_option"]:
            dining_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{opt['option']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{opt['orders']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(opt['revenue'])}</td>
            </tr>
            """

        # Build insights list
        insights_html = ""
        for insight in insights:
            insights_html += f'<li style="margin-bottom: 8px; color: #374151;">{insight}</li>'

        # Build recommendations list
        recommendations_html = ""
        for rec in recommendations:
            recommendations_html += f'<li style="margin-bottom: 8px; color: #374151;">{rec}</li>'

        # Build server flags tables
        low_tip_html = ""
        for s in server_flags.get("low_tip", []):
            low_tip_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{s["server"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["orders"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(s["revenue"])}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["tip_rate"]}%</td></tr>'

        high_disc_html = ""
        for s in server_flags.get("high_discount", []):
            high_disc_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{s["server"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["orders"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(s["discounts"])}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["discount_rate"]}%</td></tr>'

        high_void_html = ""
        for s in server_flags.get("high_void", []):
            high_void_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{s["server"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["payments"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(s["voided_amount"])}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["void_rate"]}%</td></tr>'

        # Build cash handlers table
        cash_handlers_html = ""
        for h in cash_handlers:
            cash_handlers_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{h["employee"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(h["cash_collected"])}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{h["no_sales"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{h["payouts"]}</td></tr>'

        # Build station performance table
        stations_html = ""
        for st in ops_efficiency.get("stations", []):
            stations_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{st["station"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{st["tickets"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{st["fulfilled"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{st["rate"]}%</td></tr>'

        # Status badge helper
        def status_badge(status: str) -> str:
            if not status or status == 'N/A':
                return '<span style="background: #e5e7eb; color: #6b7280; padding: 4px 8px; border-radius: 4px; font-size: 12px;">N/A</span>'
            if "OK" in status or "PASS" in status or "ON TARGET" in status:
                return f'<span style="background: #dcfce7; color: #166534; padding: 4px 8px; border-radius: 4px; font-size: 12px;">{status}</span>'
            else:
                return f'<span style="background: #fee2e2; color: #991b1b; padding: 4px 8px; border-radius: 4px; font-size: 12px;">{status}</span>'

        # Pre-extract nested dict values to avoid f-string escaping issues
        sc_rev = scorecard.get('revenue') or {}
        sc_disc = scorecard.get('discount') or {}
        sc_void = scorecard.get('void') or {}
        sc_cash = scorecard.get('cash') or {}
        sc_kit = scorecard.get('kitchen') or {}

        sc_rev_val = fmt_currency(sc_rev.get('value', 0))
        sc_rev_status = status_badge(sc_rev.get('status', 'N/A'))
        sc_disc_val = f"{sc_disc.get('value', 0):.1f}"
        sc_disc_status = status_badge(sc_disc.get('status', 'N/A'))
        sc_void_val = f"{sc_void.get('value', 0):.2f}"
        sc_void_status = status_badge(sc_void.get('status', 'N/A'))
        sc_cash_val = sc_cash.get('value', 0)
        sc_cash_status = status_badge(sc_cash.get('status', 'N/A'))
        sc_kit_val = f"{sc_kit.get('value', 0):.1f}"
        sc_kit_status = status_badge(sc_kit.get('status', 'N/A'))

        pm_liq = product_mix.get('liquor') or {}
        pm_food = product_mix.get('food') or {}
        pm_hook = product_mix.get('hookah') or {}
        pm_liq_rev = fmt_currency(pm_liq.get('revenue', 0))
        pm_liq_pct = pm_liq.get('pct', 0)
        pm_food_rev = fmt_currency(pm_food.get('revenue', 0))
        pm_food_pct = pm_food.get('pct', 0)
        pm_hook_rev = fmt_currency(pm_hook.get('revenue', 0))
        pm_hook_pct = pm_hook.get('pct', 0)
        pm_bottle = fmt_currency(product_mix.get('bottle_service', 0))

        dv_disc = disc_void.get('discounts') or {}
        dv_void = disc_void.get('voids') or {}
        dv_disc_total = fmt_currency(dv_disc.get('total', 0))
        dv_disc_rate = f"{dv_disc.get('rate', 0):.1f}"
        dv_disc_status = status_badge(dv_disc.get('status', 'N/A'))
        dv_void_cnt = dv_void.get('voided_payments', 0)
        dv_void_total = dv_void.get('total_payments', 0)
        dv_void_rate = f"{dv_void.get('rate', 0):.2f}"
        dv_void_status = status_badge(dv_void.get('status', 'N/A'))
        dv_void_amt = fmt_currency(dv_void.get('voided_amount', 0))

        db_mgr = disc_breakdown.get('manager_comp') or {}
        db_open = disc_breakdown.get('open_discount') or {}
        db_mgr_total = fmt_currency(db_mgr.get('total', 0))
        db_mgr_status = status_badge(db_mgr.get('status', 'N/A'))
        db_open_total = fmt_currency(db_open.get('total', 0))
        db_open_status = status_badge(db_open.get('status', 'N/A'))
        db_owner = fmt_currency(disc_breakdown.get('owner_comp', 0))
        db_birthday = fmt_currency(disc_breakdown.get('birthday_comp', 0))
        db_spillage = fmt_currency(disc_breakdown.get('spillage_quality', 0))

        hc_total = high_check.get('total_checks', 0)
        hc_high = high_check.get('high_checks', 0)
        hc_rate = f"{high_check.get('high_check_rate', 0):.1f}"
        hc_status = status_badge(high_check.get('status', 'N/A'))

        cc_pct = f"{cash_control.get('cash_pct', 0):.1f}"
        cc_status = status_badge(cash_control.get('cash_status', 'N/A'))
        cc_nosale = cash_control.get('no_sale_count', 0)
        cc_nosale_status = status_badge(cash_control.get('no_sale_status', 'N/A'))
        cc_variance = fmt_currency(cash_control.get('total_variance', 0))
        cc_over = fmt_currency(cash_control.get('overage', 0))
        cc_short = fmt_currency(cash_control.get('shortage', 0))
        cc_var_status = status_badge(cash_control.get('variance_status', 'N/A'))

        oe_tickets = ops_efficiency.get('total_tickets', 0)
        oe_fulfilled = ops_efficiency.get('fulfilled_tickets', 0)
        oe_rate = f"{ops_efficiency.get('fulfillment_rate', 0):.1f}"
        oe_status = status_badge(ops_efficiency.get('fulfillment_status', 'N/A'))

        # Pre-compute avg party size
        avg_party_size = f"{orders['total_guests'] / orders['total_orders']:.1f}" if orders['total_orders'] > 0 else "0"

        wow_rev = fmt_currency(wow['current_week']['revenue'])
        wow_orders = wow['current_week']['orders']
        wow_guests = wow['current_week']['guests']
        wow_tips = fmt_currency(wow['current_week'].get('tips', 0))
        wow_avg = fmt_currency(wow['current_week'].get('avg_check', 0))
        wow_per_day = wow['current_week'].get('orders_per_day', 0)
        wow_rev_chg = fmt_change(wow['changes']['revenue_pct'])
        wow_ord_chg = fmt_change(wow['changes']['orders_pct'])
        wow_yoy_chg = fmt_change(wow['changes'].get('yoy_pct', 0))
        wow_prior_guests = wow['prior_week']['guests']

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background-color: #f9fafb;">

    <div style="background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white; padding: 30px; border-radius: 12px 12px 0 0;">
        <h1 style="margin: 0 0 10px 0; font-size: 28px;">LOV3 Houston Weekly Report</h1>
        <p style="margin: 0; opacity: 0.9; font-size: 16px;">{start_date} to {end_date}</p>
    </div>

    <!-- Weekly Scorecard Summary -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Weekly Scorecard</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Metric</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Value</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Target</th>
                    <th style="padding: 12px 8px; text-align: center; font-weight: 600; color: #374151;">Status</th>
                </tr>
            </thead>
            <tbody>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">Weekly Revenue</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_rev_val}</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">$100,000</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_rev_status}</td></tr>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">Discount Rate</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_disc_val}%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">&lt;8%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_disc_status}</td></tr>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">Void Rate</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_void_val}%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">&lt;1%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_void_status}</td></tr>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">NO_SALE Count</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_cash_val}</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">&lt;100</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_cash_status}</td></tr>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">Kitchen Fulfillment</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_kit_val}%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">&gt;99%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_kit_status}</td></tr>
            </tbody>
        </table>
    </div>

    <!-- Week over Week Summary -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Revenue & Volume KPIs</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 150px; background: #f0fdf4; padding: 20px; border-radius: 8px;">
                <div style="font-size: 14px; color: #166534; margin-bottom: 5px;">Revenue</div>
                <div style="font-size: 24px; font-weight: bold; color: #15803d;">{wow_rev}</div>
                <div style="font-size: 12px; margin-top: 5px;">WoW: {wow_rev_chg}</div>
                <div style="font-size: 12px;">YoY: {wow_yoy_chg}</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #eff6ff; padding: 20px; border-radius: 8px;">
                <div style="font-size: 14px; color: #1e40af; margin-bottom: 5px;">Orders</div>
                <div style="font-size: 24px; font-weight: bold; color: #1d4ed8;">{wow_orders}</div>
                <div style="font-size: 12px; margin-top: 5px;">WoW: {wow_ord_chg}</div>
                <div style="font-size: 12px;">{wow_per_day}/day</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #fef3c7; padding: 20px; border-radius: 8px;">
                <div style="font-size: 14px; color: #92400e; margin-bottom: 5px;">Avg Check</div>
                <div style="font-size: 24px; font-weight: bold; color: #b45309;">{wow_avg}</div>
                <div style="font-size: 12px; margin-top: 5px;">Target: $90</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f3e8ff; padding: 20px; border-radius: 8px;">
                <div style="font-size: 14px; color: #7c3aed; margin-bottom: 5px;">Tips</div>
                <div style="font-size: 24px; font-weight: bold; color: #6d28d9;">{wow_tips}</div>
                <div style="font-size: 12px; margin-top: 5px;">Guests: {wow_guests}</div>
            </div>
        </div>
    </div>

    <!-- Revenue Summary -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Revenue Summary</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <tr style="background: #f0fdf4;">
                <td style="padding: 12px 8px; border-bottom: 1px solid #e5e7eb; font-weight: bold; color: #166534;">Net Sales</td>
                <td style="padding: 12px 8px; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: bold; font-size: 18px; color: #15803d;">{fmt_currency(revenue['total_revenue'])}</td>
                <td style="padding: 12px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; font-size: 12px;">Sales before gratuity, tax & tips</td>
            </tr>
            <tr>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; padding-left: 20px;">+ Auto Gratuity (20%)</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{fmt_currency(revenue['total_gratuity'])}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; font-size: 12px;">Automatic service charge</td>
            </tr>
            <tr>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; padding-left: 20px;">+ Tax Collected</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{fmt_currency(revenue['total_tax'])}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; font-size: 12px;">Sales tax</td>
            </tr>
            <tr>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; padding-left: 20px;">+ Voluntary Tips</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{fmt_currency(revenue['total_tips'])}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; font-size: 12px;">Additional tips beyond 20% auto gratuity</td>
            </tr>
            <tr style="background: #eff6ff;">
                <td style="padding: 12px 8px; font-weight: bold; color: #1e40af;">= Total Collected</td>
                <td style="padding: 12px 8px; text-align: right; font-weight: bold; font-size: 18px; color: #1d4ed8;">{fmt_currency(revenue['grand_total'])}</td>
                <td style="padding: 12px 8px; color: #6b7280; font-size: 12px;">Net Sales + Grat + Tax + Tips</td>
            </tr>
            <tr>
                <td style="padding: 10px 8px; color: #6b7280;">Average Check Size</td>
                <td style="padding: 10px 8px; text-align: right; font-weight: 500;">{fmt_currency(revenue['avg_check_size'])}</td>
                <td style="padding: 10px 8px; color: #6b7280; font-size: 12px;">Per order average</td>
            </tr>
        </table>
    </div>

    <!-- Order Metrics -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Order Metrics</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{orders['total_orders']}</div>
                <div style="font-size: 14px; color: #6b7280;">Total Orders</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{orders['total_guests']}</div>
                <div style="font-size: 14px; color: #6b7280;">Total Guests</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{avg_party_size}</div>
                <div style="font-size: 14px; color: #6b7280;">Avg Party Size</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{revenue['total_checks']}</div>
                <div style="font-size: 14px; color: #6b7280;">Total Checks</div>
            </div>
        </div>
    </div>

    <!-- Product Mix -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Product Mix</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 20px;">
            <div style="flex: 1; min-width: 150px; background: #fef3c7; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #b45309;">{pm_liq_rev}</div>
                <div style="font-size: 14px; color: #92400e;">Liquor ({pm_liq_pct}%)</div>
                <div style="font-size: 11px; color: #6b7280;">Target: 70-80%</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #dcfce7; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #166534;">{pm_food_rev}</div>
                <div style="font-size: 14px; color: #15803d;">Food ({pm_food_pct}%)</div>
                <div style="font-size: 11px; color: #6b7280;">Target: 15-20%</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #e0e7ff; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #4338ca;">{pm_hook_rev}</div>
                <div style="font-size: 14px; color: #3730a3;">Hookah ({pm_hook_pct}%)</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #fce7f3; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #be185d;">{pm_bottle}</div>
                <div style="font-size: 14px; color: #9d174d;">Bottle Service</div>
            </div>
        </div>
    </div>

    <!-- High-Check Analysis -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">High-Check Analysis ($200+)</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{hc_total}</div>
                <div style="font-size: 14px; color: #6b7280;">Total Checks</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{hc_high}</div>
                <div style="font-size: 14px; color: #6b7280;">High Checks ($200+)</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{hc_rate}%</div>
                <div style="font-size: 14px; color: #6b7280;">High-Check Rate</div>
                <div style="font-size: 11px; color: #6b7280;">Target: &gt;8%</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                {hc_status}
            </div>
        </div>
    </div>

    <!-- Discount & Void Control -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Discount & Void Control</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Total Discounts</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{dv_disc_total}</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">Rate: {dv_disc_rate}% (Target: &lt;5%)</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: center;">{dv_disc_status}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Voided Payments</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{dv_void_cnt} of {dv_void_total}</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">Rate: {dv_void_rate}% (Target: &lt;1%)</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: center;">{dv_void_status}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; color: #6b7280;">Voided Amount</td>
                <td colspan="3" style="padding: 10px 0; text-align: right; font-weight: 500;">{dv_void_amt}</td>
            </tr>
        </table>
    </div>

    <!-- Discount Breakdown -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Discount Breakdown by Type</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Manager Comp</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{db_mgr_total}</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">Target: &lt;4% of Gross</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: center;">{db_mgr_status}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Open $ Discounts</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{db_open_total}</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">Target: $0</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: center;">{db_open_status}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Owner Comp</td>
                <td colspan="3" style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{db_owner}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Birthday Comp</td>
                <td colspan="3" style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{db_birthday}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; color: #6b7280;">Spillage/Quality</td>
                <td colspan="3" style="padding: 10px 0; text-align: right; font-weight: 500;">{db_spillage}</td>
            </tr>
        </table>
    </div>

    <!-- Daily Breakdown -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Daily Breakdown</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Day</th>
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Date</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Guests</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Prior Week</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">% Change</th>
                </tr>
            </thead>
            <tbody>
                {daily_html}
            </tbody>
        </table>
    </div>

    <!-- Top Menu Items by Quantity -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Top 10 Menu Items (by Quantity)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">#</th>
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Item</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Qty Sold</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                </tr>
            </thead>
            <tbody>
                {top_items_qty_html}
            </tbody>
        </table>
    </div>

    <!-- Top Menu Items by Revenue -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Top 10 Menu Items (by Revenue)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">#</th>
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Item</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Qty Sold</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                </tr>
            </thead>
            <tbody>
                {top_items_rev_html}
            </tbody>
        </table>
    </div>

    <!-- Server Performance -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Top 10 Servers by Revenue</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Server</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Tips</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Gratuity</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Server Grat (70%)</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">LOV3 Grat (30%)</th>
                </tr>
            </thead>
            <tbody>
                {servers_html}
            </tbody>
        </table>
    </div>

    <!-- Server Flags: Low Tip Rate -->
    {"" if not server_flags.get('low_tip') else f'''
    <div style="background: #fef2f2; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #991b1b; font-size: 20px;">FLAG: Low Tip Rate (&lt;6%)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #fee2e2;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #991b1b;">Server</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Revenue</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Tip Rate</th>
                </tr>
            </thead>
            <tbody>
                {low_tip_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Server Flags: High Discount Rate -->
    {"" if not server_flags.get('high_discount') else f'''
    <div style="background: #fef2f2; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #991b1b; font-size: 20px;">FLAG: High Discount Rate (&gt;15%)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #fee2e2;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #991b1b;">Server</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Discounts</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Disc Rate</th>
                </tr>
            </thead>
            <tbody>
                {high_disc_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Server Flags: High Void Rate -->
    {"" if not server_flags.get('high_void') else f'''
    <div style="background: #fef2f2; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #991b1b; font-size: 20px;">FLAG: High Void Rate (&gt;2%)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #fee2e2;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #991b1b;">Server</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Payments</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Voided Amt</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Void Rate</th>
                </tr>
            </thead>
            <tbody>
                {high_void_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Dining Options -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Orders by Dining Option</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Dining Option</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                </tr>
            </thead>
            <tbody>
                {dining_html}
            </tbody>
        </table>
    </div>

    <!-- Payment Types -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Payment Methods</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Payment Type</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Transactions</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Amount</th>
                </tr>
            </thead>
            <tbody>
                {payments_html}
            </tbody>
        </table>
    </div>

    <!-- Cash Control -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Cash Control</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 20px;">
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{cc_pct}%</div>
                <div style="font-size: 14px; color: #6b7280;">Cash Transactions</div>
                <div style="font-size: 11px; color: #6b7280;">Benchmark: ~17%</div>
                <div style="margin-top: 8px;">{cc_status}</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{cc_nosale}</div>
                <div style="font-size: 14px; color: #6b7280;">NO_SALE Count</div>
                <div style="font-size: 11px; color: #6b7280;">Threshold: 100</div>
                <div style="margin-top: 8px;">{cc_nosale_status}</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{cc_variance}</div>
                <div style="font-size: 14px; color: #6b7280;">Cash Variance</div>
                <div style="font-size: 11px; color: #6b7280;">Over: {cc_over} / Short: {cc_short}</div>
                <div style="margin-top: 8px;">{cc_var_status}</div>
            </div>
        </div>
    </div>

    <!-- Top Cash Handlers -->
    {"" if not cash_handlers else f'''
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Top Cash Handlers</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Employee</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Cash Collected</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">NO_SALEs</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Payouts</th>
                </tr>
            </thead>
            <tbody>
                {cash_handlers_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Operational Efficiency -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Operational Efficiency</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 20px;">
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{oe_tickets}</div>
                <div style="font-size: 14px; color: #6b7280;">Kitchen Tickets</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{oe_fulfilled}</div>
                <div style="font-size: 14px; color: #6b7280;">Fulfilled</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{oe_rate}%</div>
                <div style="font-size: 14px; color: #6b7280;">Fulfillment Rate</div>
                <div style="font-size: 11px; color: #6b7280;">Target: 99%</div>
                <div style="margin-top: 8px;">{oe_status}</div>
            </div>
        </div>
    </div>

    <!-- Station Performance -->
    {"" if not ops_efficiency.get('stations') else f'''
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Station Performance</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Station</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Tickets</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Fulfilled</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Rate</th>
                </tr>
            </thead>
            <tbody>
                {stations_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Key Insights -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Key Insights</h2>
        <ul style="margin: 0; padding-left: 20px;">
            {insights_html}
        </ul>
    </div>

    <!-- Recommendations -->
    <div style="background: white; padding: 25px; border-radius: 0 0 12px 12px;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Recommendations</h2>
        <ul style="margin: 0; padding-left: 20px;">
            {recommendations_html}
        </ul>
    </div>

    <!-- Footer -->
    <div style="text-align: center; padding: 20px; color: #6b7280; font-size: 12px;">
        <p>This report was automatically generated by the LOV3 Analytics Pipeline.</p>
        <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} CST</p>
    </div>

</body>
</html>
        """
        return html

    def send_email(self, to_email: str, subject: str, html_content: str) -> bool:
        """Send email via SendGrid API"""
        try:
            api_key = self.secret_manager.get_secret("sendgrid-api-key")

            message = Mail(
                from_email=Email("maurice.ragland@lov3htx.com", "LOV3 Analytics"),
                to_emails=To(to_email),
                subject=subject,
                html_content=Content("text/html", html_content)
            )

            sg = SendGridAPIClient(api_key)
            response = sg.send(message)

            logger.info(f"Email sent to {to_email}, status code: {response.status_code}")
            return response.status_code in (200, 201, 202)

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            raise

    def generate_and_send_report(self, week_ending: str = None, to_email: str = None) -> Dict:
        """Generate and send the weekly report"""
        start_date, end_date = self.get_week_dates(week_ending)

        logger.info(f"Generating weekly report for {start_date} to {end_date}")

        # Query all data
        revenue = self.query_revenue_summary(start_date, end_date)
        orders = self.query_order_metrics(start_date, end_date)
        top_items = self.query_top_items(start_date, end_date)
        servers = self.query_server_performance(start_date, end_date)
        daily = self.query_daily_breakdown(start_date, end_date)
        payments = self.query_payment_types(start_date, end_date)
        wow = self.query_week_over_week(start_date, end_date)
        product_mix = self.query_product_mix(start_date, end_date)
        high_check = self.query_high_check_analysis(start_date, end_date)
        disc_void = self.query_discount_void_control(start_date, end_date)
        disc_breakdown = self.query_discount_breakdown(start_date, end_date)
        server_flags = self.query_server_flags(start_date, end_date)
        cash_control = self.query_cash_control(start_date, end_date)
        cash_handlers = self.query_top_cash_handlers(start_date, end_date)
        ops_efficiency = self.query_operational_efficiency(start_date, end_date)
        scorecard = self.query_weekly_scorecard(start_date, end_date)

        # Generate HTML
        html = self.generate_html_report(
            start_date, end_date,
            revenue, orders, top_items, servers, daily, payments, wow,
            product_mix, high_check, disc_void, disc_breakdown, server_flags,
            cash_control, cash_handlers, ops_efficiency, scorecard
        )

        # Send email
        recipient = to_email or REPORT_EMAIL
        subject = f"LOV3 Houston Weekly Report: {start_date} to {end_date}"

        success = self.send_email(recipient, subject, html)

        return {
            "success": success,
            "week_start": start_date,
            "week_end": end_date,
            "recipient": recipient,
            "summary": {
                "total_revenue": revenue["grand_total"],
                "total_orders": orders["total_orders"],
                "total_guests": orders["total_guests"],
                "wow_revenue_change": wow["changes"]["revenue_pct"]
            }
        }


class ToastPipeline:
    """Main pipeline orchestrator"""

    def __init__(self):
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.secret_manager = SecretManager(PROJECT_ID)
        self.schema_validator = SchemaValidator(self.bq_client, DATASET_ID)
        self.transformer = DataTransformer()
        self.loader = BigQueryLoader(self.bq_client, DATASET_ID)
        self.alert_manager = AlertManager(ALERT_WEBHOOK_URL, ALERT_EMAIL)
    
    def generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        hash_suffix = hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()[:6]
        return f"run_{timestamp}_{hash_suffix}"
    
    def process_file(
        self, 
        sftp_client: ToastSFTPClient,
        date_str: str,
        filename: str
    ) -> PipelineResult:
        """Process a single file"""
        result = PipelineResult(filename=filename, status="pending")
        
        try:
            # Check if we have config for this file
            if filename not in FILE_CONFIGS:
                result.status = "skipped"
                result.error_message = "No configuration for file"
                return result
            
            config = FILE_CONFIGS[filename]
            table_loc = config["table"]
            
            # Download file
            logger.info(f"Downloading {filename}...")
            file_bytes = sftp_client.download_file(date_str, filename)
            
            # Parse CSV
            df = pd.read_csv(io.BytesIO(file_bytes))
            result.rows_processed = len(df)
            
            if df.empty:
                result.status = "skipped"
                result.error_message = "Empty file"
                return result
            
            # Check for schema changes
            has_changes, changes = self.schema_validator.detect_schema_changes(
                df, table_loc, config.get("column_mapping", {})
            )
            result.schema_changes = changes
            
            if has_changes:
                logger.warning(f"Schema changes detected for {filename}: {changes}")
            
            # Transform data
            processing_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            df = self.transformer.transform_dataframe(df, config, processing_date)
            
            # Load to BigQuery
            if not self.loader.table_exists(table_loc):
                # Create new table
                self.loader.create_table_from_df(df, table_loc)
                result.rows_inserted = len(df)
            else:
                # Delete existing data for this date and append
                self.loader.delete_date_partition(table_loc, processing_date)
                rows = self.loader.append_data(df, table_loc)
                result.rows_inserted = rows
            
            result.status = "success"
            logger.info(f"Successfully processed {filename}: {result.rows_inserted} rows")
            
        except Exception as e:
            result.status = "error"
            result.error_message = str(e)
            logger.error(f"Error processing {filename}: {e}")
        
        return result
    
    def run(self, processing_date: str = None, backfill_days: int = 0) -> PipelineRunSummary:
        """
        Run the pipeline
        
        Args:
            processing_date: Date to process (YYYYMMDD), defaults to yesterday
            backfill_days: Number of days to backfill (0 = just processing_date)
        """
        # Default to yesterday
        if not processing_date:
            yesterday = datetime.now() - timedelta(days=1)
            processing_date = yesterday.strftime("%Y%m%d")
        
        summary = PipelineRunSummary(
            run_id=self.generate_run_id(),
            processing_date=processing_date,
            start_time=datetime.now()
        )
        
        try:
            # Get SFTP credentials
            sftp_key = self.secret_manager.get_sftp_key()
            
            # Process dates
            dates_to_process = [processing_date]
            if backfill_days > 0:
                base_date = datetime.strptime(processing_date, "%Y%m%d")
                for i in range(1, backfill_days + 1):
                    prev_date = base_date - timedelta(days=i)
                    dates_to_process.append(prev_date.strftime("%Y%m%d"))
            
            with ToastSFTPClient(SFTP_HOST, SFTP_PORT, SFTP_USER, sftp_key) as sftp:
                for date_str in dates_to_process:
                    logger.info(f"Processing date: {date_str}")
                    
                    # List available files
                    files = sftp.list_files(date_str)
                    
                    if not files:
                        summary.errors.append(f"No files found for {date_str}")
                        continue
                    
                    # Process each file
                    for filename in files:
                        result = self.process_file(sftp, date_str, filename)
                        summary.results.append(result)
                        
                        if result.status == "success":
                            summary.files_processed += 1
                            summary.total_rows += result.rows_inserted
                        elif result.status == "error":
                            summary.files_failed += 1
                            summary.errors.append(f"{filename}: {result.error_message}")
            
            summary.status = "success" if summary.files_failed == 0 else "partial_success"
            
        except Exception as e:
            summary.status = "error"
            summary.errors.append(f"Pipeline error: {str(e)}")
            logger.error(f"Pipeline failed: {e}")
        
        finally:
            summary.end_time = datetime.now()
            self.alert_manager.send_summary_alert(summary)
        
        return summary


# Flask app for Cloud Run
app = Flask(__name__)


@app.route("/", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "toast-etl-pipeline"})


@app.route("/run", methods=["POST"])
def run_pipeline():
    """
    Trigger pipeline run
    
    Request body:
    {
        "processing_date": "20250129",  // optional, defaults to yesterday
        "backfill_days": 0  // optional, number of days to backfill
    }
    """
    data = request.get_json(silent=True) or {}
    
    processing_date = data.get("processing_date")
    backfill_days = data.get("backfill_days", 0)
    
    pipeline = ToastPipeline()
    summary = pipeline.run(processing_date, backfill_days)
    
    return jsonify({
        "run_id": summary.run_id,
        "status": summary.status,
        "processing_date": summary.processing_date,
        "files_processed": summary.files_processed,
        "files_failed": summary.files_failed,
        "total_rows": summary.total_rows,
        "duration_seconds": (summary.end_time - summary.start_time).total_seconds(),
        "errors": summary.errors
    })


@app.route("/backfill", methods=["POST"])
def backfill():
    """
    Backfill historical data
    
    Request body:
    {
        "start_date": "20250101",
        "end_date": "20250129"
    }
    """
    data = request.get_json()
    
    if not data or "start_date" not in data or "end_date" not in data:
        return jsonify({"error": "start_date and end_date required"}), 400
    
    start_date = datetime.strptime(data["start_date"], "%Y%m%d")
    end_date = datetime.strptime(data["end_date"], "%Y%m%d")
    
    if start_date > end_date:
        return jsonify({"error": "start_date must be before end_date"}), 400
    
    days = (end_date - start_date).days
    
    pipeline = ToastPipeline()
    summary = pipeline.run(data["end_date"], backfill_days=days)
    
    return jsonify({
        "run_id": summary.run_id,
        "status": summary.status,
        "date_range": f"{data['start_date']} to {data['end_date']}",
        "files_processed": summary.files_processed,
        "total_rows": summary.total_rows,
        "errors": summary.errors
    })


@app.route("/status/<table_loc>", methods=["GET"])
def table_status(table_loc: str):
    """Get status of a specific table"""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_loc}"
        table = client.get_table(table_ref)

        # Get latest processing date
        query = f"""
        SELECT MAX(processing_date) as latest_date, COUNT(*) as total_rows
        FROM `{table_ref}`
        """
        result = list(client.query(query).result())[0]

        return jsonify({
            "table": table_loc,
            "total_rows": table.num_rows,
            "size_mb": table.num_bytes / (1024 * 1024),
            "latest_processing_date": str(result.latest_date) if result.latest_date else None,
            "modified": table.modified.isoformat()
        })

    except NotFound:
        return jsonify({"error": f"Table {table_loc} not found"}), 404


@app.route("/weekly-report", methods=["POST"])
def weekly_report():
    """
    Generate and send weekly summary report

    Request body (all optional):
    {
        "week_ending": "20250126",  // Sunday date (YYYYMMDD), defaults to last Sunday
        "to_email": "custom@email.com"  // Override recipient email
    }
    """
    data = request.get_json(silent=True) or {}

    week_ending = data.get("week_ending")
    to_email = data.get("to_email")

    try:
        generator = WeeklyReportGenerator()
        result = generator.generate_and_send_report(week_ending, to_email)

        return jsonify({
            "status": "success" if result["success"] else "failed",
            "week_start": result["week_start"],
            "week_end": result["week_end"],
            "recipient": result["recipient"],
            "summary": result["summary"]
        })

    except Exception as e:
        logger.error(f"Weekly report failed: {e}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


@app.route("/upload-bank-csv", methods=["POST"])
def upload_bank_csv():
    """
    Upload a Bank of America CSV export.

    Accepts multipart file upload. Auto-categorizes transactions and loads
    to BigQuery (idempotent by batch_id derived from file hash).

    Returns summary with category breakdown.
    """
    if "file" not in request.files:
        return jsonify({"error": "No file provided. Use multipart form with key 'file'."}), 400

    uploaded = request.files["file"]
    if uploaded.filename == "":
        return jsonify({"error": "Empty filename"}), 400

    try:
        file_content = uploaded.read()
        source_filename = uploaded.filename or "unknown.csv"

        # Deterministic batch_id from file content for idempotent re-uploads
        batch_id = hashlib.sha256(file_content).hexdigest()[:16]

        bq_client = bigquery.Client(project=PROJECT_ID)
        loader = BigQueryLoader(bq_client, DATASET_ID)
        cat_manager = BankCategoryManager(bq_client, DATASET_ID)

        # Get rules from BigQuery (seeds defaults on first call)
        rules = cat_manager.list_rules()

        # Sync check register from Google Sheet for "Check XXXX" lookups
        check_register: Dict[str, Dict] = {}
        try:
            register = CheckRegisterSync(bq_client, DATASET_ID)
            register.sync_from_sheet()
            check_register = register.get_lookup()
        except Exception as reg_err:
            logger.warning(f"Check register sync skipped: {reg_err}")

        parser = BofACSVParser(rules, check_register=check_register)

        df = parser.parse(file_content, source_filename)

        if df.empty:
            return jsonify({"error": "No transactions found in CSV"}), 400

        # Add upload metadata
        df["upload_date"] = datetime.now().strftime("%Y-%m-%d")
        df["upload_batch_id"] = batch_id

        # Build result summary before loading
        result = BankUploadResult(
            batch_id=batch_id,
            filename=source_filename,
            status="success",
            rows_loaded=len(df),
            total_debits=float(df.loc[df["transaction_type"] == "debit", "abs_amount"].sum()),
            total_credits=float(df.loc[df["transaction_type"] == "credit", "abs_amount"].sum()),
        )

        # Category breakdown
        cat_summary = (
            df.groupby("category")["abs_amount"]
            .sum()
            .sort_values(ascending=False)
            .to_dict()
        )
        result.transactions_by_category = {k: round(v, 2) for k, v in cat_summary.items()}

        # Date range
        min_date = df["transaction_date"].min()
        max_date = df["transaction_date"].max()
        result.date_range = f"{min_date} to {max_date}"

        # MERGE-based dedup: upsert on (transaction_date, description, amount)
        # so overlapping CSV uploads never create duplicate rows
        table_name = "BankTransactions_raw"
        if loader.table_exists(table_name):
            # Load into a temp table, then MERGE into the target
            temp_table = f"{table_name}_staging_{batch_id[:8]}"
            loader.create_table_from_df(df, temp_table)

            target_ref = loader.get_table_ref(table_name)
            temp_ref = loader.get_table_ref(temp_table)

            # Dedup key: (transaction_date, description, amount)
            # On match: only update rows that haven't been manually categorized
            # so dashboard edits are preserved across re-uploads
            key_cols = ["transaction_date", "description", "amount"]
            # Metadata columns safe to always refresh
            metadata_cols = ["upload_date", "upload_batch_id", "source_file",
                             "running_balance"]
            # Category columns only refreshed if not manually set
            category_cols = ["category", "category_source", "vendor_normalized"]
            non_key_cols = [c for c in df.columns if c not in key_cols
                           and c not in metadata_cols and c not in category_cols]

            join_cond = " AND ".join(
                [f"T.{k} = S.{k}" for k in key_cols]
            )

            # Always update metadata
            update_parts = [f"T.{c} = S.{c}" for c in metadata_cols if c in df.columns]
            # Update remaining non-key/non-category cols
            update_parts += [f"T.{c} = S.{c}" for c in non_key_cols]
            # Only overwrite category fields if NOT manually categorized
            for c in category_cols:
                if c in df.columns:
                    update_parts.append(
                        f"T.{c} = IF(T.category_source = 'manual', T.{c}, S.{c})"
                    )
            update_set = ", ".join(update_parts)

            all_cols = ", ".join(df.columns)
            src_cols = ", ".join([f"S.{c}" for c in df.columns])

            merge_sql = f"""
            MERGE `{target_ref}` T
            USING `{temp_ref}` S
            ON {join_cond}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({all_cols})
                VALUES ({src_cols})
            """
            merge_job = bq_client.query(merge_sql)
            merge_job.result()
            rows_merged = merge_job.num_dml_affected_rows or len(df)

            # Clean up staging table
            bq_client.delete_table(temp_ref, not_found_ok=True)

            logger.info(f"Bank MERGE complete: {rows_merged} rows affected")
        else:
            loader.create_table_from_df(df, table_name)

        logger.info(
            f"Bank CSV uploaded: {source_filename}, batch={batch_id}, "
            f"rows={len(df)}, debits=${result.total_debits:,.2f}, "
            f"credits=${result.total_credits:,.2f}"
        )

        return jsonify(asdict(result))

    except ValueError as e:
        logger.error(f"Bank CSV parse error: {e}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Bank CSV upload failed: {e}")
        return jsonify({"error": f"Upload failed: {str(e)}"}), 500


@app.route("/bank-categories", methods=["GET", "POST"])
def bank_categories():
    """
    Manage bank transaction auto-categorization rules.

    GET: List all keyword -> category rules.
    POST: Add/update/delete rules.
      Body for upsert: {"action": "upsert", "keyword": "SYSCO", "category": "COGS/Food", "vendor_normalized": "Sysco"}
      Body for delete: {"action": "delete", "keyword": "SYSCO"}
    """
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        manager = BankCategoryManager(bq_client, DATASET_ID)

        if request.method == "GET":
            rules = manager.list_rules()
            return jsonify({"rules": rules, "count": len(rules)})

        # POST
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400

        action = data.get("action", "upsert")

        if action == "delete":
            keyword = data.get("keyword")
            if not keyword:
                return jsonify({"error": "keyword required for delete"}), 400
            manager.delete_rule(keyword)
            return jsonify({"status": "deleted", "keyword": keyword})

        # upsert
        keyword = data.get("keyword")
        category = data.get("category")
        if not keyword or not category:
            return jsonify({"error": "keyword and category required"}), 400
        vendor = data.get("vendor_normalized", "")
        manager.upsert_rule(keyword, category, vendor)
        return jsonify({
            "status": "upserted",
            "keyword": keyword,
            "category": category,
            "vendor_normalized": vendor,
        })

    except Exception as e:
        logger.error(f"Bank categories error: {e}")
        return jsonify({"error": str(e)}), 500


# ─── Bank Transaction Review Dashboard ──────────────────────────────────────


@app.route("/api/bank-transactions", methods=["GET"])
def api_bank_transactions():
    """
    Paginated bank transaction API with filtering.

    Query params:
        status: uncategorized | categorized | all (default: all)
        limit: max rows (default 50, max 500)
        offset: pagination offset (default 0)
        sort: date_desc | date_asc | amount_desc | amount_asc (default: date_desc)
        search: free-text search on description
        date_from: YYYY-MM-DD
        date_to: YYYY-MM-DD

    Returns: summary stats, distinct categories, paginated transaction rows.
    """
    try:
        status = request.args.get("status", "all")
        limit = min(int(request.args.get("limit", 50)), 500)
        offset = int(request.args.get("offset", 0))
        sort = request.args.get("sort", "date_desc")
        search = request.args.get("search", "").strip()
        date_from = request.args.get("date_from", "")
        date_to = request.args.get("date_to", "")

        bq_client = bigquery.Client(project=PROJECT_ID)
        table = f"`{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`"

        # Build WHERE clauses
        where_parts: List[str] = []
        params: List[bigquery.ScalarQueryParameter] = []

        if status == "uncategorized":
            where_parts.append("(category_source = 'uncategorized' OR category = 'Uncategorized')")
        elif status == "categorized":
            where_parts.append("category_source != 'uncategorized' AND category != 'Uncategorized'")

        if search:
            where_parts.append("UPPER(description) LIKE CONCAT('%', UPPER(@search), '%')")
            params.append(bigquery.ScalarQueryParameter("search", "STRING", search))

        if date_from:
            where_parts.append("transaction_date >= @date_from")
            params.append(bigquery.ScalarQueryParameter("date_from", "STRING", date_from))

        if date_to:
            where_parts.append("transaction_date <= @date_to")
            params.append(bigquery.ScalarQueryParameter("date_to", "STRING", date_to))

        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # Sort mapping
        sort_map = {
            "date_desc": "transaction_date DESC, description",
            "date_asc": "transaction_date ASC, description",
            "amount_desc": "ABS(amount) DESC",
            "amount_asc": "ABS(amount) ASC",
        }
        order_sql = sort_map.get(sort, "transaction_date DESC, description")

        # Summary stats (unfiltered totals for KPI cards)
        summary_sql = f"""
        SELECT
            COUNTIF(category_source = 'uncategorized' OR category = 'Uncategorized') AS uncategorized_count,
            COALESCE(SUM(CASE WHEN category_source = 'uncategorized' OR category = 'Uncategorized'
                         THEN ABS(amount) END), 0) AS uncategorized_total,
            COUNTIF(category_source != 'uncategorized' AND category != 'Uncategorized') AS categorized_count,
            COUNT(*) AS total_count
        FROM {table}
        """
        summary_row = list(bq_client.query(summary_sql).result())[0]

        # Upload metadata — last upload date, newest transaction
        meta_sql = f"""
        SELECT
            MAX(upload_date) AS last_upload_date,
            MAX(transaction_date) AS newest_transaction_date,
            MIN(transaction_date) AS oldest_transaction_date
        FROM {table}
        """
        meta_row = list(bq_client.query(meta_sql).result())[0]
        last_upload_date = str(meta_row.last_upload_date) if meta_row.last_upload_date else None
        newest_txn_date = str(meta_row.newest_transaction_date) if meta_row.newest_transaction_date else None
        oldest_txn_date = str(meta_row.oldest_transaction_date) if meta_row.oldest_transaction_date else None

        # Last upload file info
        last_upload_file = None
        if last_upload_date:
            file_sql = f"""
            SELECT source_file, COUNT(*) AS row_count
            FROM {table}
            WHERE upload_date = @upload_date
            GROUP BY source_file
            ORDER BY row_count DESC
            LIMIT 1
            """
            file_cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("upload_date", "STRING", last_upload_date),
            ])
            file_rows = list(bq_client.query(file_sql, job_config=file_cfg).result())
            if file_rows:
                last_upload_file = file_rows[0].source_file

        # Distinct categories (from both transactions and rules)
        cat_sql = f"""
        SELECT DISTINCT category FROM (
            SELECT DISTINCT category FROM {table} WHERE category != 'Uncategorized'
            UNION DISTINCT
            SELECT DISTINCT category FROM `{PROJECT_ID}.{DATASET_ID}.BankCategoryRules`
        ) ORDER BY category
        """
        try:
            categories = [r.category for r in bq_client.query(cat_sql).result()]
        except Exception:
            categories = []

        # Filtered count
        count_sql = f"SELECT COUNT(*) AS cnt FROM {table}{where_sql}"
        job_cfg = bigquery.QueryJobConfig(query_parameters=params[:])
        filtered_count = list(bq_client.query(count_sql, job_config=job_cfg).result())[0].cnt

        # Paginated rows
        rows_sql = f"""
        SELECT transaction_date, description, amount, transaction_type,
               category, category_source, vendor_normalized
        FROM {table}{where_sql}
        ORDER BY {order_sql}
        LIMIT @limit OFFSET @offset
        """
        row_params = params + [
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
        job_cfg = bigquery.QueryJobConfig(query_parameters=row_params)
        rows = [
            {
                "transaction_date": str(r.transaction_date),
                "description": r.description,
                "amount": float(r.amount) if r.amount is not None else 0,
                "transaction_type": r.transaction_type,
                "category": r.category or "Uncategorized",
                "category_source": r.category_source or "uncategorized",
                "vendor_normalized": r.vendor_normalized or "",
            }
            for r in bq_client.query(rows_sql, job_config=job_cfg).result()
        ]

        return jsonify({
            "summary": {
                "uncategorized_count": summary_row.uncategorized_count,
                "uncategorized_total": round(float(summary_row.uncategorized_total), 2),
                "categorized_count": summary_row.categorized_count,
                "total_count": summary_row.total_count,
                "last_upload_date": last_upload_date,
                "last_upload_file": last_upload_file,
                "newest_transaction_date": newest_txn_date,
                "oldest_transaction_date": oldest_txn_date,
            },
            "categories": categories,
            "filtered_count": filtered_count,
            "limit": limit,
            "offset": offset,
            "transactions": rows,
        })

    except Exception as e:
        logger.error(f"Bank transactions API error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/bank-transactions/categorize", methods=["POST"])
def api_bank_transactions_categorize():
    """
    Bulk-update transaction categories.

    Request body:
    {
        "updates": [{
            "transaction_date": "2025-12-15",
            "description": "SOME VENDOR INC",
            "amount": -1234.56,
            "new_category": "2. Cost of Goods Sold/Food COGS",
            "vendor_normalized": "Some Vendor",
            "create_rule": true,
            "rule_keyword": "SOME VENDOR"
        }]
    }
    """
    try:
        data = request.get_json()
        if not data or "updates" not in data:
            return jsonify({"error": "JSON body with 'updates' array required"}), 400

        updates = data["updates"]
        if not updates:
            return jsonify({"error": "No updates provided"}), 400

        bq_client = bigquery.Client(project=PROJECT_ID)
        table = f"`{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`"
        cat_manager = BankCategoryManager(bq_client, DATASET_ID)

        updated = 0
        rules_created = 0
        errors: List[str] = []

        for item in updates:
            txn_date = item.get("transaction_date")
            desc = item.get("description")
            amount = item.get("amount")
            new_cat = item.get("new_category")

            if not all([txn_date, desc, new_cat]) or amount is None:
                errors.append(f"Skipped incomplete update: {item}")
                continue

            vendor = item.get("vendor_normalized", desc)

            # Update the transaction(s) in BigQuery
            update_sql = f"""
            UPDATE {table}
            SET category = @new_cat,
                category_source = 'manual',
                vendor_normalized = @vendor
            WHERE transaction_date = @txn_date
              AND description = @desc
              AND ROUND(amount, 2) = ROUND(@amount, 2)
            """
            job_cfg = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("new_cat", "STRING", new_cat),
                    bigquery.ScalarQueryParameter("vendor", "STRING", vendor),
                    bigquery.ScalarQueryParameter("txn_date", "STRING", txn_date),
                    bigquery.ScalarQueryParameter("desc", "STRING", desc),
                    bigquery.ScalarQueryParameter("amount", "FLOAT64", float(amount)),
                ]
            )
            result = bq_client.query(update_sql, job_config=job_cfg).result()
            updated += result.num_dml_affected_rows or 0

            # Optionally create a rule
            if item.get("create_rule") and item.get("rule_keyword"):
                try:
                    cat_manager.upsert_rule(item["rule_keyword"], new_cat, vendor)
                    rules_created += 1
                except Exception as rule_err:
                    errors.append(f"Rule creation failed for '{item['rule_keyword']}': {rule_err}")

        return jsonify({
            "status": "ok",
            "rows_updated": updated,
            "rules_created": rules_created,
            "errors": errors,
        })

    except Exception as e:
        logger.error(f"Bank transaction categorize error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/bank-transactions/delete", methods=["POST"])
def api_bank_transactions_delete():
    """
    Delete one or more transactions from BankTransactions_raw.

    Request body:
    {
        "deletes": [
            {"transaction_date": "2025-12-15", "description": "SOME VENDOR INC", "amount": -1234.56}
        ]
    }
    """
    try:
        data = request.get_json()
        if not data or "deletes" not in data:
            return jsonify({"error": "JSON body with 'deletes' array required"}), 400

        deletes = data["deletes"]
        if not deletes:
            return jsonify({"error": "No deletes provided"}), 400

        logger.info(f"Delete request received with {len(deletes)} item(s): {json.dumps(deletes[:3])}")

        bq_client = bigquery.Client(project=PROJECT_ID)
        table = f"`{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`"

        deleted = 0
        errors: List[str] = []

        for item in deletes:
            txn_date = item.get("transaction_date")
            desc = item.get("description")
            amount = item.get("amount")

            logger.info(f"Delete item: date={txn_date!r}, desc={desc!r}, amount={amount!r} (type={type(amount).__name__})")

            if not all([txn_date, desc]) or amount is None:
                errors.append(f"Skipped incomplete delete: {item}")
                logger.warning(f"Skipped incomplete delete: {item}")
                continue

            if amount == 0:
                # amount=0 from the API means NULL in BigQuery (see serialization in api_bank_transactions)
                delete_sql = f"""
                DELETE FROM {table}
                WHERE transaction_date = @txn_date
                  AND description = @desc
                  AND (amount IS NULL OR ROUND(amount, 2) = 0)
                """
                job_cfg = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("txn_date", "STRING", txn_date),
                        bigquery.ScalarQueryParameter("desc", "STRING", desc),
                    ]
                )
            else:
                delete_sql = f"""
                DELETE FROM {table}
                WHERE transaction_date = @txn_date
                  AND description = @desc
                  AND ROUND(amount, 2) = ROUND(@amount, 2)
                """
                job_cfg = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("txn_date", "STRING", txn_date),
                        bigquery.ScalarQueryParameter("desc", "STRING", desc),
                        bigquery.ScalarQueryParameter("amount", "FLOAT64", float(amount)),
                    ]
                )
            result = bq_client.query(delete_sql, job_config=job_cfg).result()
            affected = result.num_dml_affected_rows or 0
            logger.info(f"Delete result: {affected} row(s) affected for desc={desc!r}")
            deleted += affected

        return jsonify({
            "status": "ok",
            "rows_deleted": deleted,
            "errors": errors,
        })

    except Exception as e:
        logger.error(f"Bank transaction delete error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/bank-review", methods=["GET"])
def bank_review():
    """Interactive HTML dashboard for reviewing and categorizing bank transactions."""
    return Response(_bank_review_html(), mimetype="text/html")



# ---------------------------------------------------------------------------
# P&L Dashboard
# ---------------------------------------------------------------------------

@app.route("/pnl", methods=["GET"])
def pnl_dashboard():
    """Interactive P&L summary dashboard."""
    return Response(_pnl_dashboard_html(), mimetype="text/html")



# ---------------------------------------------------------------------------
# Comprehensive Analysis Dashboard
# ---------------------------------------------------------------------------

@app.route("/analysis", methods=["GET"])
def analysis_dashboard():
    """Interactive comprehensive analysis dashboard."""
    return Response(_analysis_dashboard_html(), mimetype="text/html")



@app.route("/cash-recon", methods=["GET"])
def cash_recon_dashboard():
    """Interactive cash reconciliation dashboard."""
    return Response(_cash_recon_html(), mimetype="text/html")



@app.route("/menu-mix", methods=["GET"])
def menu_mix_dashboard():
    """Interactive menu mix / item analysis dashboard."""
    return Response(_menu_mix_html(), mimetype="text/html")



# ─── Events & Promotional Calendar ──────────────────────────────────────────

@app.route("/events")
def events_page():
    """Events & Promotional Calendar dashboard."""
    return Response(_events_calendar_html(), mimetype="text/html")



# ─── Guest Intelligence Dashboard ────────────────────────────────────────────

@app.route("/loyalty", methods=["GET"])
def loyalty_page():
    """Guest Intelligence dashboard — card-based segmentation & analytics."""
    return Response(_customer_loyalty_html(), mimetype="text/html")



# ─── Server Performance Dashboard ──────────────────────────────────────────
@app.route("/servers", methods=["GET"])
def servers_page():
    return _server_performance_html()



# ─── Kitchen Speed Dashboard ──────────────────────────────────────────────
@app.route("/kitchen", methods=["GET"])
def kitchen_page():
    return _kitchen_speed_html()



# ─── Labor Dashboard ──────────────────────────────────────────────────────
@app.route("/labor", methods=["GET"])
def labor_page():
    return _labor_dashboard_html()



# ─── Menu Engineering Dashboard ───────────────────────────────────────────
@app.route("/menu-eng", methods=["GET"])
def menu_eng_page():
    return _menu_engineering_html()



# ─── KPI Benchmarks Dashboard ────────────────────────────────────────────────
@app.route("/kpi-benchmarks", methods=["GET"])
def kpi_benchmarks_page():
    return Response(_kpi_benchmarks_html(), mimetype="text/html")



# ─── Budget Tracker Dashboard ─────────────────────────────────────────────────
@app.route("/budget", methods=["GET"])
def budget_page():
    return Response(_budget_html(), mimetype="text/html")



# ─── Event ROI Dashboard ─────────────────────────────────────────────────────
@app.route("/event-roi", methods=["GET"])
def event_roi_page():
    return Response(_event_roi_html(), mimetype="text/html")



@app.route("/api/events-calendar", methods=["POST"])
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


# ─── Server Performance API ────────────────────────────────────────────────
@app.route("/api/server-performance", methods=["POST"])
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
        WHERE processing_date BETWEEN @start_date AND @end_date
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
        WHERE processing_date BETWEEN @start_date AND @end_date
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
@app.route("/api/kitchen-speed", methods=["POST"])
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
        WHERE processing_date BETWEEN @start_date AND @end_date
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
        WHERE processing_date BETWEEN @start_date AND @end_date
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
        WHERE processing_date BETWEEN @start_date AND @end_date
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
        WHERE processing_date BETWEEN @start_date AND @end_date
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
@app.route("/api/labor-analysis", methods=["POST"])
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
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND transaction_type = 'debit'
            AND (LOWER(category) LIKE '%labor%' OR LOWER(category) LIKE '%payroll%')
        GROUP BY vendor ORDER BY total DESC
        """

        rev_rows = list(bq.query(rev_sql).result())
        exp_rows = list(bq.query(exp_sql).result())
        mrev_rows = list(bq.query(mrev_sql).result())
        mexp_rows = list(bq.query(mexp_sql).result())
        vendor_rows = list(bq.query(vendor_sql).result())

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
@app.route("/api/menu-engineering", methods=["POST"])
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
@app.route("/api/customer-loyalty", methods=["POST"])
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

            # Tier × DOW
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
@app.route("/api/guest-export", methods=["GET"])
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
@app.route("/api/kpi-benchmarks", methods=["POST"])
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
        WHERE processing_date BETWEEN '{sd}' AND '{ed}'
          AND (voided IS NULL OR voided = 'false')
        """
        rev_row = list(bq_client.query(rev_q).result())[0]
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
        WHERE processing_date BETWEEN '{sd}' AND '{ed}'
          AND voided = 'true'
        """
        void_row = list(bq_client.query(void_q).result())[0]
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
            WHERE transaction_date BETWEEN '{sd}' AND '{ed}'
              AND transaction_type = 'debit'
            GROUP BY category ORDER BY total DESC
            """
            for row in bq_client.query(exp_q).result():
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
              AND DATE(CAST(paid_date AS DATETIME)) BETWEEN '{sd}' AND '{ed}'
          ) WHERE _rn = 1
        ),
        cards AS (
          SELECT
            CONCAT(last_4_card_digits, '-', COALESCE(card_type, '')) AS ckey,
            COUNT(DISTINCT DATE(CAST(paid_date AS DATETIME))) AS visit_days,
            ROUND(SUM(amount), 2) AS total_spend,
            DATE_DIFF(DATE '{ed}', MAX(DATE(CAST(paid_date AS DATETIME))), DAY) AS recency
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
        guest_row = list(bq_client.query(guest_q).result())[0]
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

        return jsonify({
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
        })

    except Exception as e:
        logging.exception("kpi-benchmarks API error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/budget", methods=["POST"])
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

        # --- Q1: Revenue for selected month ---
        rev_q = f"""
        SELECT
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS total_tips,
            COALESCE(SUM(gratuity), 0) AS total_gratuity,
            COUNT(DISTINCT order_id) AS order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        rev_row = list(bq_client.query(rev_q).result())[0]
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
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        rev_cat_row = list(bq_client.query(rev_cat_q).result())[0]
        food_rev = round(float(rev_cat_row.food_rev or 0), 2)
        liquor_rev = round(float(rev_cat_row.liquor_rev or 0), 2)

        # --- Q1c: Hookah revenue from bank deposits (not Toast POS) ---
        hookah_bank_q = f"""
        SELECT COALESCE(SUM(amount), 0) AS hookah_rev
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND LOWER(category) LIKE '%hookah sales%'
            AND amount > 0
        """
        hookah_row = list(bq_client.query(hookah_bank_q).result())[0]
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND transaction_type = 'debit'
        GROUP BY category
        ORDER BY total DESC
        """
        exp_rows = list(bq_client.query(exp_q).result())
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
            AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY vendor_normalized, category
        ORDER BY total DESC
        LIMIT 50
        """
        vendor_rows = list(bq_client.query(vendor_q).result())
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
            AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY abs_amount DESC
        LIMIT 500
        """
        txn_rows = list(bq_client.query(txn_q).result())
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
        WHERE processing_date BETWEEN '{hist_start}' AND '{hist_end}'
            AND (voided IS NULL OR voided = 'false')
        GROUP BY month ORDER BY month
        """
        exp_hist_q = f"""
        SELECT
            LEFT(CAST(transaction_date AS STRING), 7) AS month,
            category,
            ROUND(SUM(abs_amount), 2) AS total
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN '{hist_start}' AND '{hist_end}'
            AND transaction_type = 'debit'
        GROUP BY month, category
        ORDER BY month
        """
        rev_hist_rows = list(bq_client.query(rev_hist_q).result())
        exp_hist_rows = list(bq_client.query(exp_hist_q).result())

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


@app.route("/api/budget-drilldown", methods=["POST"])
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
            AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND {kw_where}
        ORDER BY abs_amount DESC
        """
        rows = list(bq_client.query(q).result())
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
@app.route("/api/event-roi", methods=["POST"])
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
        WHERE {bd} BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
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
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND transaction_type = 'debit'
            AND ({ops_cat_likes})
        GROUP BY month, category
        ORDER BY month
        """

        rev_rows = list(bq_client.query(q_revenue).result())
        direct_rows = list(bq_client.query(q_direct).result())
        shared_rows = list(bq_client.query(q_shared).result())
        all_vendor_rows = list(bq_client.query(q_all_vendors).result())
        labor_rows = list(bq_client.query(q_labor).result())
        ops_labor_rows = list(bq_client.query(q_ops_labor).result())

        # --- Step 1: Build revenue by event by month ---
        dow_to_event = {cfg["dow_num"]: key for key, cfg in RECURRING_EVENTS.items()}

        # {event_key: {month: {net_sales, gratuity, grat_retained, adjusted_revenue, num_nights, txn_count}}}
        rev_by_event_month: Dict[str, Dict[str, Dict]] = {}
        for row in rev_rows:
            dow = int(row.dow_num)
            event_key = dow_to_event.get(dow)
            if not event_key:
                continue  # Monday (dark) or unexpected DOW
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
            """Return list of event keys matched (empty if unattributed)."""
            v_lower = (vendor or "").lower()
            for keyword, ev_key in EVENT_VENDOR_MAP.items():
                if keyword.lower() in v_lower:
                    if isinstance(ev_key, list):
                        return ev_key
                    return [ev_key]
            return []

        # {event_key: {month: amount}}
        direct_by_event_month: Dict[str, Dict[str, float]] = {}
        # {event_key: {vendor: {amount, txns, category}}}
        direct_vendors_by_event: Dict[str, Dict[str, Dict]] = {}
        # unattributed accumulator
        unattributed_accum: Dict[str, Dict] = {}

        for row in direct_rows:
            vendor = row.vendor or "Unknown"
            month = row.month
            amount = float(row.total_amount or 0)
            txns = int(row.txn_count or 0)
            matched_events = _match_vendor(vendor)

            if matched_events:
                # Split evenly across matched events
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

        # Build unattributed list from Q4 (aggregated across months)
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

        # All months across revenue + expenses
        all_months = sorted(set(
            m for ev in rev_by_event_month.values() for m in ev
        ) | set(shared_by_month.keys()) | set(
            m for ev in direct_by_event_month.values() for m in ev
        ))

        # {event_key: {month: allocated_amount}}
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
        # Bank labor debits include tip/grat pass-through which is NOT a house cost.
        # True Labor = Gross Labor - Tips - (Gratuity × 65%)
        # Then subtract fixed (mgmt + 1099), allocate variable remainder by DOW%.
        labor_by_month: Dict[str, float] = {}
        for row in labor_rows:
            labor_by_month[row.month] = float(row.total_labor or 0)

        # Compute tips + grat pass-through per month from revenue data
        passthrough_by_month: Dict[str, float] = {}
        for ek in RECURRING_EVENTS:
            for month, mdata in rev_by_event_month.get(ek, {}).items():
                tips = mdata.get("tips", 0)
                grat = mdata.get("gratuity", 0)
                grat_passthrough = round(grat * (1 - GRAT_RETAIN_PCT), 2)
                passthrough_by_month[month] = passthrough_by_month.get(month, 0) + tips + grat_passthrough

        # Include labor months in all_months
        all_months = sorted(set(all_months) | set(labor_by_month.keys()))

        # {event_key: {month: labor_amount}}
        labor_by_event_month: Dict[str, Dict[str, float]] = {}
        total_gross_labor = 0.0
        total_passthrough = 0.0
        total_true_labor = 0.0
        total_fixed_labor = 0.0
        total_variable_labor = 0.0

        for month in all_months:
            gross = labor_by_month.get(month, 0)
            total_gross_labor += gross
            # Strip out tip/grat pass-through (not a house cost)
            pt = passthrough_by_month.get(month, 0)
            total_passthrough += pt
            true_labor = max(gross - pt, 0)
            total_true_labor += true_labor
            # Subtract fixed component (mgmt + 1099)
            fixed = FIXED_LABOR_MONTHLY
            total_fixed_labor += fixed
            variable = max(true_labor - fixed, 0)
            total_variable_labor += variable
            # Allocate variable labor by DOW percentages
            for ek, pct in LABOR_DOW_PCT.items():
                alloc = round(variable * pct, 2)
                if ek not in labor_by_event_month:
                    labor_by_event_month[ek] = {}
                labor_by_event_month[ek][month] = alloc

        # --- Step 3c: Allocate operational labor (security + contract staffing) by DOW ---
        # These are vendor payments with no tip/grat — allocate directly by DOW%.
        ops_labor_by_month: Dict[str, float] = {}
        for row in ops_labor_rows:
            m = row.month
            ops_labor_by_month[m] = ops_labor_by_month.get(m, 0) + float(row.total_amount or 0)

        all_months = sorted(set(all_months) | set(ops_labor_by_month.keys()))

        # {event_key: {month: ops_labor_amount}}
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

            # Direct vendor detail
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

        # Sort by revenue descending
        events_list.sort(key=lambda e: e["revenue"]["adjusted_revenue"], reverse=True)

        # Compute revenue share pct for each event
        for ev in events_list:
            ev["revenue"]["revenue_share_pct"] = round(
                ev["revenue"]["adjusted_revenue"] / total_event_revenue * 100, 1
            ) if total_event_revenue > 0 else 0

        # Summary
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

        # Unattributed warning
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

        # Labor allocation insight
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

        # Best and worst ROI events (only if they have costs)
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

        # Revenue dominance
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

        # Cost efficiency — lowest cost per dollar of revenue
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

        # Sort insights: critical → warning → info → good
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


@app.route("/api/menu-mix", methods=["POST"])
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

        # Dedup CTE: take latest processing_date per item_selection_id
        dedup_cte = f"""deduped AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY item_selection_id ORDER BY processing_date DESC
            ) AS _rn
            FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
            WHERE order_date IS NOT NULL AND order_date != ''
          ) WHERE _rn = 1
        )"""

        # --- Q1: KPIs with void stats ---
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

        # --- Q2: Top 20 items ---
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

        # --- Q3: Category x Service cross-tab ---
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

        # --- Q4: DOW x Hour cross-tab ---
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

        # Execute all queries
        kpi_rows = list(bq_client.query(q_kpis, job_config=job_config).result())
        top_items_rows = list(bq_client.query(q_top_items, job_config=job_config).result())
        cat_svc_rows = list(bq_client.query(q_cat_svc, job_config=job_config).result())
        dow_hour_rows = list(bq_client.query(q_dow_hour, job_config=job_config).result())

        # --- Assemble KPIs ---
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

        # Void rate = void_rev / (total_revenue + void_rev) * 100
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

        # --- Assemble Top Items ---
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

        # --- Pivot Q3: categories + service periods ---
        cat_map: dict = {}  # category -> {items, revenue, checks}
        svc_map: dict = {}  # service -> {items, revenue, checks}
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

        # --- Pivot Q4: DOW + hourly ---
        dow_map: dict = {}   # dow_name -> {items, revenue, checks, dow_num, dates}
        hour_map: dict = {}  # hour -> {items, revenue, checks, dates}
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


@app.route("/api/cash-recon", methods=["POST"])
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

        # --- Query 1: POS monthly breakdown from PaymentDetails ---
        # Uses paid_date (actual payment time) not processing_date (SFTP business day).
        # Bank settlements key off calendar payment date, so paid_date aligns better.
        # Includes AUTHORIZED + CAPTURE_IN_PROGRESS: analysis found 5,334 unique
        # non-CAPTURED credit transactions ($485K) that WERE settled by Citizens
        # but had no CAPTURED counterpart in the SFTP export.
        # Excludes DENIED (2,900 txns/$982K - declined cards, no money collected),
        # VOIDED (548 txns/$85K - cancelled before payment), and other terminal statuses.
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
        WHERE DATE(CAST(paid_date AS DATETIME)) BETWEEN '{start_date}' AND '{end_date}'
            AND status IN ('CAPTURED', 'AUTHORIZED', 'CAPTURE_IN_PROGRESS')
        GROUP BY month, status, pay_type
        ORDER BY month, pay_type, status
        """
        pos_rows = list(bq_client.query(pos_query).result())

        # --- Query 2: Bank deposit breakdown ---
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
            WHERE CAST(transaction_date AS DATE) BETWEEN '{start_date}' AND '{end_date}'
                AND (description LIKE '%Citizens%' OR description LIKE '%TOAST%'
                     OR description LIKE '%Toast%' OR description LIKE '%Counter Credit%'
                     OR (description LIKE 'Online Banking transfer from CHK 9121%' AND amount > 0))
            GROUP BY month, deposit_type
            HAVING deposit_type IS NOT NULL
            ORDER BY month, deposit_type
            """
            bank_rows = list(bq_client.query(bank_query).result())

        # --- Assemble per-month data ---
        # Collect all months
        all_months: set[str] = set()
        pos_data: dict[str, dict] = {}  # month -> aggregated POS data
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

            # Status breakdown (credit only for recon purposes)
            if pay_type == "Credit":
                if status not in d["status_breakdown"]:
                    d["status_breakdown"][status] = {"count": 0, "amount": 0.0}
                d["status_breakdown"][status]["count"] += count
                d["status_breakdown"][status]["amount"] += amount

        bank_data: dict[str, dict] = {}  # month -> bank deposit types
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

        # Build sorted month list
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

            # Cash recon: counter credits + inter-account transfers into operating account
            total_cash_in = round(bd_m["counter_credit"] + bd_m["interaccount_in"], 2)
            cash_gap = round(pd_m["cash_collected"] - total_cash_in, 2)
            cum_cash_gap = round(cum_cash_gap + cash_gap, 2)

            # Status badge
            abs_pct = abs(card_diff_pct)
            card_status = "OK" if abs_pct <= 5 else ("WATCH" if abs_pct <= 10 else "HIGH")

            total_pos_credit_net += credit_net
            total_pos_cash += pd_m["cash_collected"]
            total_bank_card_net += bank_card_net
            total_bank_cash += total_cash_in

            # Alerts
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


@app.route("/profit-summary", methods=["POST"])
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
    if not data or "start_date" not in data or "end_date" not in data:
        return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400

    start_date = data["start_date"]
    end_date = data["end_date"]

    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        # --- Revenue from Toast (with gratuity breakdown) ---
        # Uses centralized LOV3 Business Assumptions (see top of file)

        revenue_query = f"""
        SELECT
            COALESCE(SUM(amount), 0) as net_sales,
            COALESCE(SUM(tax), 0) as total_tax,
            COALESCE(SUM(tip), 0) as total_tips,
            COALESCE(SUM(gratuity), 0) as total_gratuity,
            COALESCE(SUM(total), 0) as gross_revenue,
            COUNT(DISTINCT order_id) as order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        rev_row = list(bq_client.query(revenue_query).result())[0]
        net_sales = float(rev_row.net_sales or 0)
        gross_revenue = float(rev_row.gross_revenue or 0)
        total_tips = float(rev_row.total_tips or 0)
        total_gratuity = float(rev_row.total_gratuity or 0)

        # Gratuity split
        grat_retained = round(total_gratuity * GRAT_RETAIN_PCT, 2)
        grat_to_staff = round(total_gratuity * GRAT_PASSTHROUGH_PCT, 2)
        total_pass_through = round(total_tips + grat_to_staff, 2)

        # Adjusted revenue includes LOV3's retained gratuity share
        adjusted_net_revenue = round(net_sales + grat_retained, 2)

        # --- Cash reconciliation ---
        # Toast PaymentDetails shows cash collected at the register.
        # Compare to cash deposits hitting the bank account.
        cash_query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN payment_type = 'Cash' OR payment_type LIKE '%CASH%'
                         THEN total ELSE 0 END), 0) as cash_collected,
            COUNTIF(payment_type = 'Cash' OR payment_type LIKE '%CASH%') as cash_txn_count
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        cash_row = list(bq_client.query(cash_query).result())[0]
        cash_collected = float(cash_row.cash_collected or 0)
        cash_txn_count = int(cash_row.cash_txn_count or 0)

        # Cash drawer activity from CashEntries
        drawer_query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN action = 'CASH_COLLECTED' THEN amount ELSE 0 END), 0) as drawer_collected,
            COALESCE(SUM(CASE WHEN action = 'PAY_OUT' THEN ABS(amount) ELSE 0 END), 0) as payouts,
            COALESCE(SUM(CASE WHEN action = 'CLOSE_OUT_OVERAGE' THEN amount ELSE 0 END), 0) as overages,
            COALESCE(SUM(CASE WHEN action = 'CLOSE_OUT_SHORTAGE' THEN ABS(amount) ELSE 0 END), 0) as shortages,
            COUNTIF(action = 'NO_SALE') as no_sale_count,
            COUNTIF(action = 'CLOSE_OUT_EXACT') as exact_closeouts
        FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        drawer_row = list(bq_client.query(drawer_query).result())[0]
        drawer_collected = float(drawer_row.drawer_collected or 0)
        drawer_payouts = float(drawer_row.payouts or 0)
        drawer_overages = float(drawer_row.overages or 0)
        drawer_shortages = float(drawer_row.shortages or 0)
        no_sale_count = int(drawer_row.no_sale_count or 0)
        exact_closeouts = int(drawer_row.exact_closeouts or 0)

        # --- Expenses from Bank ---
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
            WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
                AND transaction_type = 'debit'
            GROUP BY category
            ORDER BY total DESC
            """
            expense_rows = list(bq_client.query(expense_query).result())
            for row in expense_rows:
                expenses_by_category[row.category] = float(row.total or 0)
            # Exclude revenue-classified items from expense total
            total_expenses = sum(
                v for k, v in expenses_by_category.items()
                if "revenue" not in k.lower()
            )

            # Cash deposits hitting the bank account
            # BofA shows these as "Counter Credit" (physical cash deposits)
            # and inter-account transfers from cash account
            deposit_query = f"""
            SELECT
                COALESCE(SUM(abs_amount), 0) as total_deposits
            FROM `{bank_table}`
            WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
                AND transaction_type = 'credit'
                AND (LOWER(category) LIKE '%cash deposit%'
                     OR LOWER(category) LIKE '%cash account transfer%'
                     OR LOWER(description) LIKE '%counter credit%')
            """
            dep_row = list(bq_client.query(deposit_query).result())[0]
            cash_deposited = float(dep_row.total_deposits or 0)

        # --- Derived metrics ---
        # Sum categories by prefix to handle hierarchical names like
        # "2. Cost of Goods Sold/Food COGS" or flat names like "COGS/Food"
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

        # Reconcile labor: subtract tip & gratuity pass-through
        # Bank labor debits include pass-through money paid to staff
        labor_true = round(labor_gross - total_pass_through, 2)
        if labor_true < 0:
            labor_true = 0.0  # Guard against data timing mismatches

        # Adjusted expenses: replace gross labor with true labor
        adjusted_expenses = round(total_expenses - total_pass_through, 2)
        if adjusted_expenses < 0:
            adjusted_expenses = total_expenses

        # All percentages use adjusted_net_revenue as denominator
        rev = adjusted_net_revenue if adjusted_net_revenue > 0 else 1
        prime_cost = round(total_cogs + labor_true, 2)
        net_profit = round(adjusted_net_revenue - adjusted_expenses, 2)

        # --- Cash reconciliation ---
        undeposited_cash = round(cash_collected - cash_deposited, 2)
        # Cash-adjusted profit: undeposited cash offsets expenses already
        # paid with cash (e.g., payouts, vendor payments) that hit the bank
        # as debits but were funded by cash on hand, not bank balance.
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


@app.route("/comprehensive-analysis", methods=["POST"])
def comprehensive_analysis():
    """
    Full financial analysis combining Toast POS + BofA bank data.

    All key business assumptions are baked in:
    - Business day = 4AM to 3:59AM (nightlife venue)
    - Gratuity split: 35% house / 65% staff; tips 100% staff
    - True labor = gross labor - tip/gratuity pass-through
    - Unreconciled cash = Toast cash collected - bank deposits
    - Post-audit category hierarchy for expense classification

    Request body:
    {
        "start_date": "2025-01-01",
        "end_date": "2026-01-31"
    }
    """
    data = request.get_json()
    if not data or "start_date" not in data or "end_date" not in data:
        return jsonify({"error": "start_date and end_date required (YYYY-MM-DD)"}), 400

    start_date = data["start_date"]
    end_date = data["end_date"]

    try:
        report = WeeklyReportGenerator()

        # 1. Revenue by business day (4AM cutoff)
        revenue_by_dow = report.query_revenue_by_business_day(start_date, end_date)

        # 2. Monthly P&L
        monthly_pnl = report.query_monthly_pnl(start_date, end_date)

        # 3. Hourly revenue profile
        hourly_profile = report.query_hourly_revenue_profile(start_date, end_date)

        # 4. Aggregate P&L totals
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


@app.route("/sync-check-register", methods=["POST"])
def sync_check_register():
    """Manually sync the Google Sheet check register into BigQuery.

    Pulls the latest rows from the configured Google Sheet and MERGEs
    them into the CheckRegister BigQuery table.
    """
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


@app.route("/upload-check-register", methods=["POST"])
def upload_check_register():
    """Upload a check register CSV (fallback if Google Sheet sync fails).

    Accepts multipart file upload with columns: check_number, payee,
    and optionally category, amount, memo.
    """
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


@app.route("/api/reconcile-checks", methods=["POST"])
def reconcile_checks():
    """Re-categorize uncategorized Check transactions using the current register.

    Syncs the register from Google Sheets, then re-runs _categorize() for
    every BankTransactions_raw row where category = 'Uncategorized' and
    description matches 'Check XXXX'.  This fixes checks that were uploaded
    before the register entry existed.
    """
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        # 1. Sync register from Google Sheet
        register_sync = CheckRegisterSync(bq_client, DATASET_ID)
        synced = register_sync.sync_from_sheet()
        check_lookup = register_sync.get_lookup()

        # 2. Load keyword rules
        cat_manager = BankCategoryManager(bq_client, DATASET_ID)
        rules = cat_manager.list_rules()

        # 3. Build a temporary parser just for its _categorize method
        parser = BofACSVParser(rules, check_register=check_lookup)

        # 4. Fetch uncategorized Check rows
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

        # 5. Re-categorize each row
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

        # 6. Batch-update via parameterized queries
        reconciled = 0
        for u in updates:
            uq = f"""
                UPDATE `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
                SET category = @category,
                    category_source = @source,
                    vendor_normalized = @vendor
                WHERE transaction_date = @txn_date
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


if __name__ == "__main__":
    # Local development
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)
