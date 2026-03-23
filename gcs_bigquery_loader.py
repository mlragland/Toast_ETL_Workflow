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

logger = logging.getLogger(__name__)

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
        self.project_id = project_id or "toast-analytics-444116"
        self.dataset_id = dataset_id or "toast_analytics"
        self.bucket_name = bucket_name or "toast-raw-data"
        
        # Initialize clients
        self.storage_client = storage.Client(project=self.project_id)
        self.bigquery_client = bigquery.Client(project=self.project_id)
        self.dataset_ref = self.bigquery_client.dataset(self.dataset_id)
        
        logger.info(f"GCS-BigQuery loader initialized for project: {self.project_id}, dataset: {self.dataset_id}, bucket: {self.bucket_name}")
    
    def upload_to_gcs(self, local_file_path: str, gcs_path: str) -> str:
        """
        Upload a file to Google Cloud Storage
        
        Args:
            local_file_path: Path to the local file
            gcs_path: Destination path in GCS bucket
            
        Returns:
            GCS URI of the uploaded file
        """
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)
            
            logger.info(f"Uploading {local_file_path} to gs://{self.bucket_name}/{gcs_path}")
            blob.upload_from_filename(local_file_path)
            
            gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
            logger.info(f"Successfully uploaded to {gcs_uri}")
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Error uploading {local_file_path} to GCS: {e}")
            raise
    
    def load_from_gcs_to_bigquery(self, gcs_uri: str, table_name: str, source_file: str) -> Dict[str, Any]:
        """
        Load data from GCS to BigQuery using native CSV loading
        
        Args:
            gcs_uri: GCS URI of the file to load
            table_name: Target BigQuery table name
            source_file: Original source file name for auditing
            
        Returns:
            Dictionary with load job results
        """
        start_time = time.time()
        
        # Configure load job
        table_ref = self.dataset_ref.table(table_name)
        job_config = LoadJobConfig(
            source_format=SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            write_disposition=WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
            allow_jagged_rows=False,
            ignore_unknown_values=False,
            autodetect=True  # Let BigQuery detect schema
        )
        
        try:
            logger.info(f"Loading data from {gcs_uri} to {table_name}")
            load_job = self.bigquery_client.load_table_from_uri(
                gcs_uri, table_ref, job_config=job_config
            )
            
            # Wait for job to complete
            load_job.result(timeout=300)  # 5 minute timeout
            
            # Get final table info
            table = self.bigquery_client.get_table(table_ref)
            
            load_time = time.time() - start_time
            
            result = {
                'success': True,
                'table_name': table_name,
                'rows_loaded': load_job.output_rows or 0,
                'total_rows': table.num_rows,
                'load_time_seconds': round(load_time, 2),
                'job_id': load_job.job_id,
                'source_file': source_file,
                'gcs_uri': gcs_uri,
                'errors': None
            }
            
            logger.info(f"Successfully loaded {load_job.output_rows or 0} rows to {table_name} in {load_time:.2f}s")
            return result
            
        except Exception as e:
            load_time = time.time() - start_time
            error_msg = f"Failed to load data from {gcs_uri} to {table_name}: {str(e)}"
            logger.error(error_msg)
            
            result = {
                'success': False,
                'table_name': table_name,
                'rows_loaded': 0,
                'total_rows': 0,
                'load_time_seconds': round(load_time, 2),
                'job_id': getattr(load_job, 'job_id', None) if 'load_job' in locals() else None,
                'source_file': source_file,
                'gcs_uri': gcs_uri,
                'errors': error_msg
            }
            
            raise GoogleCloudError(error_msg)
    
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
            gcs_uri = self.upload_to_gcs(csv_file_path, gcs_path)
            
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