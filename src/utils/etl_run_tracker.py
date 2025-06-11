"""
ETL Run Tracker - Toast ETL Pipeline
Tracks ETL execution metadata for dashboard monitoring and analytics.
"""

import os
import json
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

class ETLRunTracker:
    """Tracks ETL run metadata in BigQuery for dashboard monitoring."""
    
    def __init__(self):
        """Initialize the ETL run tracker."""
        self.project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
        self.dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
        self.client = bigquery.Client(project=self.project_id)
        
        # Current run state
        self.current_run = None
        
    def start_run(self, execution_date: str, source_type: str = 'manual') -> str:
        """
        Start tracking a new ETL run.
        
        Args:
            execution_date: Date being processed (YYYY-MM-DD format)
            source_type: Type of run ('scheduled', 'manual', 'backfill')
            
        Returns:
            Unique run ID
        """
        run_id = f"run_{int(time.time())}_{str(uuid.uuid4())[:8]}"
        
        self.current_run = {
            'run_id': run_id,
            'execution_date': execution_date,
            'started_at': datetime.utcnow(),
            'source_type': source_type,
            'status': 'running',
            'files_processed': 0,
            'records_processed': 0,
            'total_sales': 0.0,
            'files_detail': {}
        }
        
        try:
            # Insert initial run record
            self._insert_run_record()
            logger.info(f"Started tracking ETL run: {run_id}")
            
        except Exception as e:
            logger.error(f"Failed to start run tracking: {str(e)}")
            
        return run_id
    
    def update_run_progress(self, 
                          files_processed: Optional[int] = None,
                          records_processed: Optional[int] = None,
                          total_sales: Optional[float] = None,
                          files_detail: Optional[Dict[str, Any]] = None):
        """
        Update progress of the current run.
        
        Args:
            files_processed: Number of files processed
            records_processed: Total number of records processed
            total_sales: Total sales amount processed
            files_detail: Detailed file processing information
        """
        if not self.current_run:
            logger.warning("No active run to update")
            return
            
        if files_processed is not None:
            self.current_run['files_processed'] = files_processed
        if records_processed is not None:
            self.current_run['records_processed'] = records_processed
        if total_sales is not None:
            self.current_run['total_sales'] = total_sales
        if files_detail is not None:
            self.current_run['files_detail'].update(files_detail)
    
    def complete_run(self, status: str = 'success', error_message: Optional[str] = None):
        """
        Mark the current run as completed.
        
        Args:
            status: Final status ('success', 'failed')
            error_message: Error message if status is 'failed'
        """
        if not self.current_run:
            logger.warning("No active run to complete")
            return
        
        self.current_run['status'] = status
        self.current_run['completed_at'] = datetime.utcnow()
        self.current_run['error_message'] = error_message
        
        # Calculate execution time
        start_time = self.current_run['started_at']
        end_time = self.current_run['completed_at']
        execution_time = (end_time - start_time).total_seconds()
        self.current_run['execution_time_seconds'] = execution_time
        
        try:
            # Update run record with final status
            self._update_run_record()
            
            status_emoji = "✅" if status == 'success' else "❌"
            logger.info(f"{status_emoji} ETL run completed: {self.current_run['run_id']} "
                       f"({execution_time:.1f}s, {self.current_run['records_processed']} records)")
            
        except Exception as e:
            logger.error(f"Failed to complete run tracking: {str(e)}")
        
        finally:
            self.current_run = None
    
    def _insert_run_record(self):
        """Insert initial run record into BigQuery."""
        table_id = f"{self.project_id}.{self.dataset_id}.etl_runs"
        
        row = {
            'run_id': self.current_run['run_id'],
            'execution_date': self.current_run['execution_date'],
            'started_at': self.current_run['started_at'],
            'status': self.current_run['status'],
            'source_type': self.current_run['source_type'],
            'files_processed': self.current_run['files_processed'],
            'records_processed': self.current_run['records_processed'],
            'total_sales': self.current_run['total_sales'],
            'files_detail': json.dumps(self.current_run['files_detail'])
        }
        
        errors = self.client.insert_rows_json(table_id, [row])
        if errors:
            raise Exception(f"BigQuery insert failed: {errors}")
    
    def _update_run_record(self):
        """Update the run record with final status and metrics."""
        table_id = f"{self.project_id}.{self.dataset_id}.etl_runs"
        
        # Use UPDATE query to modify the existing record
        update_query = f"""
        UPDATE `{table_id}`
        SET 
            completed_at = @completed_at,
            status = @status,
            files_processed = @files_processed,
            records_processed = @records_processed,
            total_sales = @total_sales,
            execution_time_seconds = @execution_time_seconds,
            error_message = @error_message,
            files_detail = @files_detail
        WHERE run_id = @run_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", self.current_run['run_id']),
                bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", self.current_run['completed_at']),
                bigquery.ScalarQueryParameter("status", "STRING", self.current_run['status']),
                bigquery.ScalarQueryParameter("files_processed", "INTEGER", self.current_run['files_processed']),
                bigquery.ScalarQueryParameter("records_processed", "INTEGER", self.current_run['records_processed']),
                bigquery.ScalarQueryParameter("total_sales", "FLOAT", self.current_run['total_sales']),
                bigquery.ScalarQueryParameter("execution_time_seconds", "FLOAT", self.current_run['execution_time_seconds']),
                bigquery.ScalarQueryParameter("error_message", "STRING", self.current_run.get('error_message')),
                bigquery.ScalarQueryParameter("files_detail", "STRING", json.dumps(self.current_run['files_detail']))
            ]
        )
        
        query_job = self.client.query(update_query, job_config=job_config)
        query_job.result()  # Wait for completion
    
    def get_run_id(self) -> Optional[str]:
        """Get the current run ID."""
        return self.current_run['run_id'] if self.current_run else None


# Global tracker instance
run_tracker = ETLRunTracker() 