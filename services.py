"""
Business logic service classes for the Toast ETL pipeline.

Extracted from main.py — BofA parsing, BigQuery loading, SFTP,
schema validation, alerting, and category management.
"""

import io
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import paramiko
import pandas as pd
from google.cloud import bigquery, secretmanager
from google.cloud.exceptions import NotFound
import google.auth
from googleapiclient.discovery import build as google_build

from config import (
    PROJECT_ID, DATASET_ID, DEFAULT_CATEGORY_RULES,
    CHECK_REGISTER_SHEET_ID, CHECK_REGISTER_SHEET_NAME,
)
from models import PipelineResult, PipelineRunSummary

logger = logging.getLogger(__name__)


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

        # Parse dates - store as datetime64 for BigQuery DATE compatibility
        # BofA uses both MM/DD/YYYY and M/D/YY formats across different exports
        df["transaction_date"] = pd.to_datetime(
            df["date_raw"].str.strip(), format="mixed", dayfirst=False
        ).dt.normalize()  # strips time component, keeps datetime64 dtype

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

            if col in ('processing_date', 'transaction_date'):
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
        WHERE processing_date = PARSE_DATE('%Y-%m-%d', @processing_date)
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("processing_date", "STRING", processing_date),
        ])

        query_job = self.client.query(delete_sql, job_config=job_config)
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

        status_emoji = "\u2705" if summary.status == "success" else "\u274c"

        message = f"""
{status_emoji} *Pipeline Run Complete*
\u2022 Run ID: `{summary.run_id}`
\u2022 Date Processed: {summary.processing_date}
\u2022 Status: {summary.status.upper()}
\u2022 Duration: {duration:.1f}s
\u2022 Files: {summary.files_processed} processed, {summary.files_failed} failed
\u2022 Total Rows: {summary.total_rows:,}
"""

        if summary.errors:
            message += "\n*Errors:*\n" + "\n".join([f"\u2022 {e}" for e in summary.errors[:5]])

        self.send_slack_alert(message, is_error=summary.status != "success")
