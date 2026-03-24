"""
Teller Bank Sync — Automated daily bank transaction ingestion.

Pulls posted transactions from Teller API (Bank of America) and loads
into BigQuery BankTransactions_raw using existing categorization rules.
Runs daily at 7:30 AM CST via Cloud Scheduler, after Toast ETL completes.

Ensures no gaps by pulling from the day after the latest transaction in
BigQuery, with a configurable lookback window for late-posting corrections.
"""

import io
import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from google.cloud import bigquery

from config import PROJECT_ID, DATASET_ID
from services import BofACSVParser, BankCategoryManager, CheckRegisterSync, BigQueryLoader, SecretManager

logger = logging.getLogger(__name__)

# Teller API configuration
TELLER_API_BASE = "https://api.teller.io"
TELLER_ACCOUNT_ID = "acc_pq7gc19lq41vgjs4ie000"

# Lookback: re-pull this many days before the latest BQ date to catch
# late-posting corrections and pending→posted transitions.
LOOKBACK_DAYS = 3


class TellerSync:
    """Syncs bank transactions from Teller API to BigQuery."""

    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT_ID)
        self.sm = SecretManager(PROJECT_ID)

    def _get_teller_creds(self) -> Tuple[str, str, str]:
        """Retrieve Teller API token, cert, and key from Secret Manager."""
        token = self.sm.get_secret("teller-api-token")
        cert = self.sm.get_secret("teller-certificate")
        key = self.sm.get_secret("teller-private-key")
        return token, cert, key

    def _write_temp_certs(self, cert_pem: str, key_pem: str) -> Tuple[str, str]:
        """Write cert/key to temp files for requests library."""
        import tempfile
        cert_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
        cert_file.write(cert_pem)
        cert_file.close()

        key_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
        key_file.write(key_pem)
        key_file.close()

        return cert_file.name, key_file.name

    def _get_latest_bq_date(self) -> Optional[str]:
        """Find the latest transaction_date in BigQuery."""
        sql = f"""
        SELECT MAX(CAST(transaction_date AS STRING)) AS latest
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        """
        rows = list(self.bq.query(sql).result())
        if rows and rows[0].latest:
            return rows[0].latest
        return None

    def _pull_transactions(
        self, token: str, cert_path: str, key_path: str,
        from_date: str = None, count: int = 500
    ) -> List[Dict]:
        """Pull transactions from Teller API.

        Args:
            token: Teller API token
            cert_path: Path to client certificate PEM
            key_path: Path to private key PEM
            from_date: Only return transactions on or after this date (YYYY-MM-DD)
            count: Max transactions to return

        Returns:
            List of posted transaction dicts
        """
        url = f"{TELLER_API_BASE}/accounts/{TELLER_ACCOUNT_ID}/transactions"
        params = {"count": count}

        resp = requests.get(
            url,
            params=params,
            auth=(token, ""),
            cert=(cert_path, key_path),
            timeout=30,
        )
        resp.raise_for_status()
        all_txns = resp.json()

        if isinstance(all_txns, dict) and "error" in all_txns:
            raise RuntimeError(f"Teller API error: {all_txns['error']}")

        # Filter to posted only (skip pending — amounts may change)
        posted = [t for t in all_txns if t.get("status") == "posted"]

        # Filter by date if specified
        if from_date:
            posted = [t for t in posted if t["date"] >= from_date]

        logger.info(f"Teller: pulled {len(posted)} posted transactions"
                    f" (of {len(all_txns)} total, from_date={from_date})")
        return posted

    def _transform_transactions(self, txns: List[Dict]) -> pd.DataFrame:
        """Convert Teller transactions to BankTransactions_raw format."""
        if not txns:
            return pd.DataFrame()

        rows = []
        for t in txns:
            amount = float(t["amount"])
            rows.append({
                "transaction_date": t["date"],
                "description": t["description"].split("\n")[0].strip(),  # first line only
                "amount": amount,
                "running_balance": None,
                "transaction_type": "debit" if amount < 0 else "credit",
                "abs_amount": abs(amount),
                "source_file": "teller_sync",
                "upload_date": date.today().isoformat(),
                "upload_batch_id": f"teller_{date.today().isoformat()}",
            })

        df = pd.DataFrame(rows)
        # Convert to datetime64 for BigQuery DATE compatibility
        df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.normalize()
        return df

    def _categorize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply existing categorization rules to transactions."""
        if df.empty:
            return df

        # Get rules from BigQuery
        cat_manager = BankCategoryManager(self.bq, DATASET_ID)
        rules = cat_manager.get_rules()

        # Get check register
        check_register = None
        try:
            register_sync = CheckRegisterSync(self.bq, DATASET_ID)
            register_sync.sync_from_sheet()
            check_register = register_sync.get_register_dict()
        except Exception as e:
            logger.warning(f"Check register sync skipped: {e}")

        # Run categorization
        parser = BofACSVParser(rules, check_register=check_register)
        categories = []
        sources = []
        vendors = []

        for _, row in df.iterrows():
            cat, src, vendor = parser._categorize(row["description"])
            categories.append(cat)
            sources.append(src)
            vendors.append(vendor)

        df["category"] = categories
        df["category_source"] = sources
        df["vendor_normalized"] = vendors
        return df

    def sync(self, force_from_date: str = None) -> Dict[str, Any]:
        """Run the full sync: pull → categorize → load.

        Args:
            force_from_date: Override the auto-detected start date (YYYY-MM-DD).
                           Use to backfill gaps.

        Returns:
            Summary dict with counts and status.
        """
        start_time = datetime.now()

        try:
            # Determine start date
            if force_from_date:
                from_date = force_from_date
            else:
                latest = self._get_latest_bq_date()
                if latest:
                    # Lookback N days to catch late corrections
                    dt = datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=LOOKBACK_DAYS)
                    from_date = dt.strftime("%Y-%m-%d")
                else:
                    # No data in BQ — pull everything
                    from_date = None

            logger.info(f"Teller sync starting from {from_date or 'all history'}")

            # Get credentials
            token, cert_pem, key_pem = self._get_teller_creds()
            cert_path, key_path = self._write_temp_certs(cert_pem, key_pem)

            # Pull transactions
            txns = self._pull_transactions(token, cert_path, key_path, from_date)

            if not txns:
                return {
                    "status": "success",
                    "message": "No new transactions to sync",
                    "transactions_pulled": 0,
                    "rows_loaded": 0,
                    "from_date": from_date,
                    "duration_seconds": (datetime.now() - start_time).total_seconds(),
                }

            # Transform to DataFrame
            df = self._transform_transactions(txns)

            # Categorize
            df = self._categorize(df)

            # Load to BigQuery via MERGE (same dedup as CSV upload)
            loader = BigQueryLoader(self.bq, DATASET_ID)
            table_name = "BankTransactions_raw"

            if loader.table_exists(table_name):
                # MERGE into existing table
                temp_table = f"{table_name}_teller_staging"
                loader.create_table_from_df(df, temp_table)

                target_ref = loader.get_table_ref(table_name)
                temp_ref = loader.get_table_ref(temp_table)

                merge_sql = f"""
                MERGE `{target_ref}` T
                USING `{temp_ref}` S
                ON T.transaction_date = S.transaction_date
                   AND T.description = S.description
                   AND ROUND(T.amount, 2) = ROUND(S.amount, 2)
                WHEN MATCHED AND T.category_source != 'manual' THEN
                    UPDATE SET
                        T.category = S.category,
                        T.category_source = S.category_source,
                        T.vendor_normalized = S.vendor_normalized,
                        T.upload_date = S.upload_date,
                        T.upload_batch_id = S.upload_batch_id,
                        T.source_file = S.source_file
                WHEN NOT MATCHED THEN
                    INSERT (transaction_date, description, amount, running_balance,
                            transaction_type, abs_amount, category, category_source,
                            vendor_normalized, source_file, upload_date, upload_batch_id)
                    VALUES (S.transaction_date, S.description, S.amount, S.running_balance,
                            S.transaction_type, S.abs_amount, S.category, S.category_source,
                            S.vendor_normalized, S.source_file, S.upload_date, S.upload_batch_id)
                """
                merge_job = self.bq.query(merge_sql)
                merge_job.result()
                rows_affected = merge_job.num_dml_affected_rows or 0

                # Clean up staging
                self.bq.delete_table(temp_ref, not_found_ok=True)
            else:
                loader.create_table_from_df(df, table_name)
                rows_affected = len(df)

            # Clean up temp cert files
            import os
            os.unlink(cert_path)
            os.unlink(key_path)

            duration = (datetime.now() - start_time).total_seconds()
            date_range = f"{df['transaction_date'].min()} to {df['transaction_date'].max()}"

            summary = {
                "status": "success",
                "transactions_pulled": len(txns),
                "rows_loaded": rows_affected,
                "from_date": from_date,
                "date_range": date_range,
                "categorized": len(df[df["category_source"] != "uncategorized"]),
                "uncategorized": len(df[df["category_source"] == "uncategorized"]),
                "duration_seconds": round(duration, 1),
            }
            logger.info(f"Teller sync complete: {summary}")
            return summary

        except Exception as e:
            logger.error(f"Teller sync failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }
