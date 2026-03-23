"""
Main pipeline orchestrator for the Toast ETL workflow.

Extracted from main.py — connects to Toast SFTP, downloads daily CSVs,
transforms them, and loads into BigQuery.
"""

import io
import hashlib
import logging
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from google.cloud import bigquery

from config import PROJECT_ID, DATASET_ID, SFTP_HOST, SFTP_PORT, SFTP_USER, ALERT_WEBHOOK_URL, ALERT_EMAIL, FILE_CONFIGS
from models import PipelineResult, PipelineRunSummary
from services import SecretManager, ToastSFTPClient, SchemaValidator, DataTransformer, BigQueryLoader, AlertManager

logger = logging.getLogger(__name__)


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
