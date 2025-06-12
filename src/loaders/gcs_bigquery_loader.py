"""
GCS-BigQuery Loader for Toast ETL Pipeline

This module implements the legacy staging approach:
1. Upload cleaned CSV files to GCS bucket
2. Load from GCS to BigQuery using native CSV loading
3. Avoids PyArrow conversion issues entirely

Based on the successful legacy scripts that used GCS staging.
"""

import logging
import time
import os
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, SourceFormat
from google.cloud.exceptions import GoogleCloudError, NotFound
import pandas as pd
from datetime import datetime

from src.utils.logging_utils import get_logger
from src.utils.retry_utils import retry_with_backoff
from src.config.settings import settings

logger = get_logger(__name__)

class GCSBigQueryLoader:
    """
    Handles BigQuery loading via GCS staging to avoid PyArrow conversion issues
    """
    
    def __init__(self, project_id: str = None, dataset_id: str = None, bucket_name: str = None):
        """
        Initialize GCS-BigQuery loader
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID  
            bucket_name: GCS bucket name for staging
        """
        self.project_id = project_id or settings.gcp_project_id
        self.dataset_id = dataset_id or settings.bigquery_dataset
        self.bucket_name = bucket_name or "toast-raw-data"
        
        # Initialize clients
        self.storage_client = storage.Client(project=self.project_id)
        self.bigquery_client = bigquery.Client(project=self.project_id)
        self.dataset_ref = self.bigquery_client.dataset(self.dataset_id)
        
        # Get bucket reference
        self.bucket = self.storage_client.bucket(self.bucket_name)
        
        # Table schemas for BigQuery loading
        self.table_schemas = self._get_table_schemas()
        
        logger.info(f"GCS-BigQuery loader initialized for project: {self.project_id}, dataset: {self.dataset_id}, bucket: {self.bucket_name}")
    
    def _get_table_schemas(self) -> Dict[str, List[Dict]]:
        """
        Define BigQuery table schemas for all Toast data sources.
        Updated to include closure indicator fields for business closure detection.
        
        Returns:
            Dictionary mapping table names to their schema fields
        """
        return {
            'all_items_report': [
                {"name": "master_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "item_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "parent_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "menu_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "menu_group", "type": "STRING", "mode": "NULLABLE"},
                {"name": "subgroup", "type": "STRING", "mode": "NULLABLE"},
                {"name": "menu_item", "type": "STRING", "mode": "NULLABLE"},
                {"name": "tags", "type": "STRING", "mode": "NULLABLE"},
                {"name": "avg_price", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "item_qty_incl_voids", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "percent_ttl_qty_incl_voids", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "gross_amount_incl_voids", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "percent_ttl_amt_incl_voids", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "item_qty", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "gross_amount", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "void_qty", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "void_amount", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "discount_amount", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "net_amount", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "num_orders", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "percent_ttl_num_orders", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "percent_qty_group", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "percent_qty_menu", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "percent_qty_all", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "percent_net_amt_group", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "percent_net_amt_menu", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "percent_net_amt_all", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
                # Business closure fields
                {"name": "closure_indicator", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "closure_reason", "type": "STRING", "mode": "NULLABLE"}
            ],
            'check_details': [
                {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "customer", "type": "STRING", "mode": "NULLABLE"},
                {"name": "customer_phone", "type": "STRING", "mode": "NULLABLE"},
                {"name": "customer_email", "type": "STRING", "mode": "NULLABLE"},
                {"name": "location_code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "opened_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "opened_time", "type": "TIME", "mode": "NULLABLE"},
                {"name": "item_description", "type": "STRING", "mode": "NULLABLE"},
                {"name": "server", "type": "STRING", "mode": "NULLABLE"},
                {"name": "tax", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "tender", "type": "STRING", "mode": "NULLABLE"},
                {"name": "check_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "check_number", "type": "STRING", "mode": "NULLABLE"},
                {"name": "total", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "customer_family", "type": "STRING", "mode": "NULLABLE"},
                {"name": "table_size", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "discount", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "reason_of_discount", "type": "STRING", "mode": "NULLABLE"},
                {"name": "link", "type": "STRING", "mode": "NULLABLE"},
                {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
                # Business closure fields
                {"name": "closure_indicator", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "closure_reason", "type": "STRING", "mode": "NULLABLE"}
            ],
            'cash_entries': [
                {"name": "location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "entry_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "created_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "action", "type": "STRING", "mode": "NULLABLE"},
                {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "cash_drawer", "type": "STRING", "mode": "NULLABLE"},
                {"name": "payout_reason", "type": "STRING", "mode": "NULLABLE"},
                {"name": "no_sale_reason", "type": "STRING", "mode": "NULLABLE"},
                {"name": "comment", "type": "STRING", "mode": "NULLABLE"},
                {"name": "employee", "type": "STRING", "mode": "NULLABLE"},
                {"name": "employee_2", "type": "STRING", "mode": "NULLABLE"},
                {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
                # Business closure fields
                {"name": "closure_indicator", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "closure_reason", "type": "STRING", "mode": "NULLABLE"}
            ],
            'item_selection_details': [
                {"name": "location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "order_number", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sent_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "order_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "check_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "server", "type": "STRING", "mode": "NULLABLE"},
                {"name": "table", "type": "STRING", "mode": "NULLABLE"},
                {"name": "dining_area", "type": "STRING", "mode": "NULLABLE"},
                {"name": "service", "type": "STRING", "mode": "NULLABLE"},
                {"name": "dining_option", "type": "STRING", "mode": "NULLABLE"},
                {"name": "item_selection_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "item_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "master_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sku", "type": "STRING", "mode": "NULLABLE"},
                {"name": "plu", "type": "STRING", "mode": "NULLABLE"},
                {"name": "menu_item", "type": "STRING", "mode": "NULLABLE"},
                {"name": "menu_subgroup", "type": "STRING", "mode": "NULLABLE"},
                {"name": "menu_group", "type": "STRING", "mode": "NULLABLE"},
                {"name": "menu", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sales_category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "gross_price", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "void", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "deferred", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "tax_exempt", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
                # Business closure fields
                {"name": "closure_indicator", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "closure_reason", "type": "STRING", "mode": "NULLABLE"}
            ],
            'kitchen_timings': [
                {"name": "location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "check_opened", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "fired_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "fulfilled_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "fulfillment_time", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "check_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "item_selection_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "menu_item", "type": "STRING", "mode": "NULLABLE"},
                {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
                # Business closure fields
                {"name": "closure_indicator", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "closure_reason", "type": "STRING", "mode": "NULLABLE"}
            ],
            'order_details': [
                {"name": "location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "opened", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "paid", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "closed", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "voided", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "check_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "check_number", "type": "STRING", "mode": "NULLABLE"},
                {"name": "server", "type": "STRING", "mode": "NULLABLE"},
                {"name": "table", "type": "STRING", "mode": "NULLABLE"},
                {"name": "dining_area", "type": "STRING", "mode": "NULLABLE"},
                {"name": "service", "type": "STRING", "mode": "NULLABLE"},
                {"name": "dining_option", "type": "STRING", "mode": "NULLABLE"},
                {"name": "duration_opened_to_paid", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "total", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "processing_date", "type": "STRING", "mode": "NULLABLE"},
                {"name": "loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
                # Business closure fields
                {"name": "closure_indicator", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "closure_reason", "type": "STRING", "mode": "NULLABLE"}
            ],
            'payment_details': [
                {"name": "location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "payment_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "paid_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "refund_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "order_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "void_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "check_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "payment_type", "type": "STRING", "mode": "NULLABLE"},
                {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"},
                {"name": "loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "source_file", "type": "STRING", "mode": "NULLABLE"},
                # Business closure fields
                {"name": "closure_indicator", "type": "BOOLEAN", "mode": "NULLABLE"},
                {"name": "closure_reason", "type": "STRING", "mode": "NULLABLE"}
            ]
        }
    
    def _convert_schema_to_bigquery_fields(self, schema_definition: List[Dict]) -> List[bigquery.SchemaField]:
        """
        Convert schema definition to BigQuery SchemaField objects
        
        Args:
            schema_definition: List of field definitions
            
        Returns:
            List of BigQuery SchemaField objects
        """
        fields = []
        for field_def in schema_definition:
            field = bigquery.SchemaField(
                name=field_def['name'],
                field_type=field_def['type'],
                mode=field_def.get('mode', 'NULLABLE')
            )
            fields.append(field)
        
        return fields
    
    @retry_with_backoff(max_attempts=3)
    def ensure_dataset_exists(self) -> None:
        """
        Ensure the BigQuery dataset exists
        """
        try:
            self.bigquery_client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            dataset = self.bigquery_client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {self.dataset_id}")
    
    @retry_with_backoff(max_attempts=3)
    def ensure_table_exists(self, table_name: str) -> None:
        """
        Ensure the BigQuery table exists with proper schema
        
        Args:
            table_name: Name of the table to create
        """
        try:
            table_ref = self.dataset_ref.table(table_name)
            self.bigquery_client.get_table(table_ref)
            logger.info(f"Table {table_name} already exists")
        except NotFound:
            if table_name not in self.table_schemas:
                raise ValueError(f"No schema defined for table: {table_name}")
            
            schema_fields = self._convert_schema_to_bigquery_fields(self.table_schemas[table_name])
            table = bigquery.Table(table_ref, schema=schema_fields)
            table = self.bigquery_client.create_table(table)
            logger.info(f"Created table {table_name}")
    
    def upload_csv_to_gcs(self, csv_file_path: str, gcs_path: str) -> str:
        """
        Upload CSV file to GCS after adding missing loaded_at field
        
        Args:
            csv_file_path: Local path to CSV file
            gcs_path: GCS path within bucket
            
        Returns:
            GCS URI of uploaded file
        """
        try:
            # Read CSV and add loaded_at timestamp field if missing
            df = pd.read_csv(csv_file_path)
            
            # Add loaded_at field if it doesn't exist
            if 'loaded_at' not in df.columns:
                df['loaded_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"Added loaded_at timestamp field to {os.path.basename(csv_file_path)}")
            
            # Create temporary file with updated data
            temp_file = csv_file_path + '.tmp'
            df.to_csv(temp_file, index=False)
            
            # Upload to GCS
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(temp_file)
            
            # Clean up temporary file
            os.remove(temp_file)
            
            gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
            logger.info(f"Uploaded CSV to GCS: {gcs_uri}")
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Failed to upload {csv_file_path} to GCS: {e}")
            raise
    
    @retry_with_backoff(max_attempts=3)
    def load_from_gcs_to_bigquery(self, gcs_uri: str, table_name: str, source_file: str) -> Dict[str, Any]:
        """
        Load data from GCS to BigQuery using native CSV loading with schema autodetection
        
        Args:
            gcs_uri: GCS URI of the file to load
            table_name: Target BigQuery table name
            source_file: Original source file name for auditing
            
        Returns:
            Dictionary with load job results
        """
        start_time = time.time()
        
        # Ensure dataset and table exist
        self.ensure_dataset_exists()
        
        # Check if table exists, if not create with autodetected schema
        table_ref = self.dataset_ref.table(table_name)
        try:
            table = self.bigquery_client.get_table(table_ref)
            logger.info(f"Table {table_name} already exists")
        except NotFound:
            # Table doesn't exist - will be created with autodetected schema
            logger.info(f"Table {table_name} will be created with autodetected schema")
        
        # Configure load job with autodetection
        logger.info(f"Loading data from {gcs_uri} to {table_name}")
        
        job_config = LoadJobConfig(
            source_format=SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            write_disposition=WriteDisposition.WRITE_TRUNCATE,  # Replace existing data
            allow_quoted_newlines=True,
            allow_jagged_rows=False,
            ignore_unknown_values=True,  # More permissive
            autodetect=True,  # Let BigQuery detect schema automatically
            create_disposition='CREATE_IF_NEEDED'  # Create table if it doesn't exist
        )
        
        # Start load job
        job_id = f"toast_etl_load_{table_name}_{int(time.time())}"
        load_job = self.bigquery_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config,
            job_id=job_id
        )
        
        try:
            # Wait for job completion
            result = load_job.result()  # Waits for job to complete
            
            # Get final statistics
            duration = time.time() - start_time
            rows_loaded = result.output_rows if result.output_rows else 0
            
            logger.info(f"Successfully loaded {rows_loaded:,} rows to {table_name} in {duration:.2f}s")
            
            return {
                'success': True,
                'job_id': job_id,
                'table_name': table_name,
                'source_file': source_file,
                'gcs_uri': gcs_uri,
                'rows_loaded': rows_loaded,
                'duration_seconds': duration,
                'bytes_processed': getattr(load_job, 'total_bytes_billed', 0) or 0
            }
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to load data from {gcs_uri} to {table_name}: {error_msg}")
            
            # Try to get more details from the job
            try:
                if load_job.errors:
                    error_details = "; ".join([err['message'] for err in load_job.errors])
                    error_msg = f"{error_msg} Details: {error_details}"
            except:
                pass
            
            return {
                'success': False,
                'job_id': job_id,
                'table_name': table_name,
                'source_file': source_file,
                'gcs_uri': gcs_uri,
                'error': error_msg,
                'duration_seconds': time.time() - start_time
            }
    
    def load_csv_file(self, csv_file_path: str, table_name: str, processing_date: str) -> Dict[str, Any]:
        """
        Complete workflow: Upload CSV to GCS and load to BigQuery
        
        Args:
            csv_file_path: Path to the CSV file to load
            table_name: Target BigQuery table name
            processing_date: Processing date for the data (YYYY-MM-DD format)
            
        Returns:
            Dictionary with load results
        """
        try:
            # Generate GCS path
            file_name = os.path.basename(csv_file_path)
            date_folder = processing_date.replace('-', '')  # Convert to YYYYMMDD
            gcs_path = f"cleaned/{date_folder}/{file_name}"
            
            # Step 1: Upload to GCS
            gcs_uri = self.upload_csv_to_gcs(csv_file_path, gcs_path)
            
            # Step 2: Load from GCS to BigQuery
            result = self.load_from_gcs_to_bigquery(gcs_uri, table_name, file_name)
            
            return result
            
        except Exception as e:
            logger.error(f"Error in complete load workflow for {csv_file_path}: {e}")
            return {
                'success': False,
                'table_name': table_name,
                'rows_loaded': 0,
                'total_rows': 0,
                'load_time_seconds': 0,
                'job_id': None,
                'source_file': os.path.basename(csv_file_path),
                'gcs_uri': None,
                'errors': str(e)
            }
    
    def load_csv_via_gcs(self, csv_file_path: str, table_name: str, source_file: str) -> Dict[str, Any]:
        """
        Load CSV file via GCS staging with partitioning fix
        This is the main method that combines upload and load operations
        
        Args:
            csv_file_path: Path to CSV file to load
            table_name: Target BigQuery table name
            source_file: Original source file name for auditing
            
        Returns:
            Dictionary with load results
        """
        try:
            # Extract date from file path for GCS organization
            processing_date = datetime.now().strftime('%Y%m%d')
            
            # Generate GCS path
            gcs_path = f"cleaned/{processing_date}/{os.path.basename(csv_file_path)}"
            
            logger.info(f"Starting GCS staging load: {csv_file_path} → {table_name}")
            
            # Step 1: Upload to GCS (with loaded_at field addition)
            gcs_uri = self.upload_csv_to_gcs(csv_file_path, gcs_path)
            
            # Step 2: Load from GCS to BigQuery  
            result = self.load_from_gcs_to_bigquery(gcs_uri, table_name, source_file)
            
            # Step 3: Clean up GCS file (optional)
            try:
                blob = self.bucket.blob(gcs_path)
                blob.delete()
                logger.info(f"Cleaned up GCS staging file: {gcs_path}")
            except Exception as e:
                logger.warning(f"Could not clean up GCS file {gcs_path}: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"GCS staging load failed for {csv_file_path}: {e}")
            return {
                'success': False,
                'error': str(e),
                'table_name': table_name,
                'source_file': source_file
            }

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a BigQuery table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information
        """
        try:
            table_ref = self.dataset_ref.table(table_name)
            table = self.bigquery_client.get_table(table_ref)
            
            return {
                'table_name': table_name,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created.isoformat() if table.created else None,
                'modified': table.modified.isoformat() if table.modified else None,
                'schema_fields': len(table.schema),
                'partitioned': table.time_partitioning is not None,
                'clustered': table.clustering_fields is not None
            }
        except NotFound:
            logger.warning(f"Table {table_name} not found")
            return {'table_name': table_name, 'exists': False}
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return {'table_name': table_name, 'error': str(e)} 