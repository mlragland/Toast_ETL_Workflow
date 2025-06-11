"""
BigQuery Loader for Toast ETL Pipeline

This module handles all BigQuery operations including:
- Table creation and schema management
- Data loading and validation
- Batch operations and optimization
- Error handling and retries
"""

import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, SourceFormat
from google.cloud.exceptions import GoogleCloudError, NotFound
from google.api_core import retry
import pandas as pd

from src.utils.logging_utils import get_logger
from src.utils.retry_utils import retry_with_backoff
from src.config.settings import settings

logger = get_logger(__name__)

class BigQueryLoader:
    """
    Handles all BigQuery operations for the Toast ETL pipeline
    """
    
    def __init__(self, project_id: str = None, dataset_id: str = None, location: str = None):
        """
        Initialize BigQuery loader
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            location: BigQuery location/region
        """
        self.project_id = project_id or settings.gcp_project_id
        self.dataset_id = dataset_id or settings.bigquery_dataset
        self.location = location or "US"  # Default BigQuery location
        
        # Initialize BigQuery client
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_ref = self.client.dataset(self.dataset_id)
        
        # Table schemas for each Toast data source
        self.table_schemas = self._get_table_schemas()
        
        logger.info(f"BigQuery loader initialized for project: {self.project_id}, dataset: {self.dataset_id}")
    
    def _get_table_schemas(self) -> Dict[str, List[bigquery.SchemaField]]:
        """
        Define BigQuery table schemas for all Toast data sources
        
        Returns:
            Dictionary mapping table names to their schema fields
        """
        schemas = {
            'all_items_report': [
                bigquery.SchemaField('GUID', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('Name', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('PLU', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Type', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Group', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Price', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('Cost', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('Visibility', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('ModifiedDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('CreatedDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('loaded_at', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('source_file', 'STRING', mode='REQUIRED'),
            ],
            'check_details': [
                bigquery.SchemaField('GUID', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('EntityType', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('ExternalId', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('CheckNumber', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('OpenedDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('ClosedDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('Server', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Table', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('GuestCount', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('TotalAmount', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('TipAmount', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('TaxAmount', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('loaded_at', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('source_file', 'STRING', mode='REQUIRED'),
            ],
            'cash_entries': [
                bigquery.SchemaField('GUID', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('CashDrawerGUID', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Type', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Amount', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('Date', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('Employee', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Reason', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('loaded_at', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('source_file', 'STRING', mode='REQUIRED'),
            ],
            'item_selection_details': [
                bigquery.SchemaField('GUID', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('ItemGUID', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('CheckGUID', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('MenuItemGUID', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Quantity', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('UnitPrice', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('TotalPrice', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('VoidDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('OrderedDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('loaded_at', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('source_file', 'STRING', mode='REQUIRED'),
            ],
            'kitchen_timings': [
                bigquery.SchemaField('GUID', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('CheckGUID', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('OrderGUID', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('SentDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('CompletedDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('Station', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('loaded_at', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('source_file', 'STRING', mode='REQUIRED'),
            ],
            'order_details': [
                bigquery.SchemaField('GUID', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('CheckGUID', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('OrderDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('OrderType', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Source', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('loaded_at', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('source_file', 'STRING', mode='REQUIRED'),
            ],
            'payment_details': [
                bigquery.SchemaField('GUID', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('CheckGUID', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('PaymentType', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('Amount', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('TipAmount', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('PaymentDate', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('loaded_at', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('source_file', 'STRING', mode='REQUIRED'),
            ]
        }
        return schemas
    
    @retry_with_backoff(max_attempts=3)
    def ensure_dataset_exists(self) -> None:
        """
        Ensure the BigQuery dataset exists, create if it doesn't
        """
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            logger.info(f"Creating dataset {self.dataset_id}")
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = self.location
            dataset.description = "Toast POS ETL Data Pipeline"
            
            # Set up partitioning and clustering for better performance
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {self.dataset_id}")
    
    @retry_with_backoff(max_attempts=3)
    def ensure_table_exists(self, table_name: str) -> None:
        """
        Ensure a BigQuery table exists with the correct schema
        
        Args:
            table_name: Name of the table to create/verify
        """
        if table_name not in self.table_schemas:
            raise ValueError(f"Unknown table: {table_name}")
        
        table_ref = self.dataset_ref.table(table_name)
        
        try:
            table = self.client.get_table(table_ref)
            logger.info(f"Table {table_name} already exists")
            
            # Verify schema matches
            existing_schema = {field.name: field for field in table.schema}
            expected_schema = {field.name: field for field in self.table_schemas[table_name]}
            
            if existing_schema != expected_schema:
                logger.warning(f"Schema mismatch for table {table_name}, considering migration")
                
        except NotFound:
            logger.info(f"Creating table {table_name}")
            table = bigquery.Table(table_ref, schema=self.table_schemas[table_name])
            
            # Set up partitioning on loaded_at field for better performance
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="loaded_at"
            )
            
            table = self.client.create_table(table, timeout=30)
            logger.info(f"Created table {table_name}")
    
    def validate_data_quality(self, df: pd.DataFrame, table_name: str) -> Tuple[bool, List[str]]:
        """
        Validate data quality before loading to BigQuery
        
        Args:
            df: DataFrame to validate
            table_name: Target table name
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check if DataFrame is empty
        if df.empty:
            errors.append(f"DataFrame is empty for table {table_name}")
            return False, errors
        
        # Get expected schema
        if table_name not in self.table_schemas:
            errors.append(f"Unknown table schema: {table_name}")
            return False, errors
        
        expected_fields = {field.name for field in self.table_schemas[table_name] 
                          if field.name not in ['loaded_at', 'source_file']}
        
        # Check for missing required columns
        df_columns = set(df.columns)
        missing_columns = expected_fields - df_columns
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check for completely null required columns
        required_fields = {field.name for field in self.table_schemas[table_name] 
                          if field.mode == 'REQUIRED' and field.name not in ['loaded_at', 'source_file']}
        
        for field in required_fields:
            if field in df.columns and df[field].isnull().all():
                errors.append(f"Required field {field} is completely null")
        
        # Business rule validations
        if table_name == 'check_details':
            # Check for negative amounts
            if 'TotalAmount' in df.columns:
                negative_amounts = df[df['TotalAmount'] < 0]
                if not negative_amounts.empty:
                    logger.warning(f"Found {len(negative_amounts)} records with negative TotalAmount")
        
        return len(errors) == 0, errors
    
    @retry_with_backoff(max_attempts=3)
    def load_dataframe(self, df: pd.DataFrame, table_name: str, source_file: str, 
                      write_disposition: str = 'WRITE_APPEND') -> Dict[str, Any]:
        """
        Load a DataFrame to BigQuery with comprehensive error handling
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            source_file: Source file name for auditing
            write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE, etc.)
            
        Returns:
            Dictionary with load job results
        """
        start_time = time.time()
        
        # Ensure dataset exists
        self.ensure_dataset_exists()
        
        # Add metadata columns
        df_copy = df.copy()
        df_copy['loaded_at'] = pd.Timestamp.now(tz='UTC')
        df_copy['source_file'] = source_file
        
        # Configure load job
        table_ref = self.dataset_ref.table(table_name)
        job_config = LoadJobConfig(
            write_disposition=getattr(WriteDisposition, write_disposition),
            create_disposition='CREATE_IF_NEEDED',
            autodetect=True,  # Let BigQuery infer schema for now
        )
        
        try:
            # Load the data
            logger.info(f"Loading {len(df_copy)} records to {table_name}")
            load_job = self.client.load_table_from_dataframe(
                df_copy, table_ref, job_config=job_config
            )
            
            # Wait for job to complete
            load_job.result(timeout=300)  # 5 minute timeout
            
            # Get final table info
            table = self.client.get_table(table_ref)
            
            load_time = time.time() - start_time
            
            result = {
                'success': True,
                'table_name': table_name,
                'rows_loaded': len(df_copy),
                'total_rows': table.num_rows,
                'load_time_seconds': round(load_time, 2),
                'job_id': load_job.job_id,
                'source_file': source_file,
                'errors': None
            }
            
            logger.info(f"Successfully loaded {len(df_copy)} rows to {table_name} in {load_time:.2f}s")
            return result
            
        except Exception as e:
            load_time = time.time() - start_time
            error_msg = f"Failed to load data to {table_name}: {str(e)}"
            logger.error(error_msg)
            
            result = {
                'success': False,
                'table_name': table_name,
                'rows_loaded': 0,
                'total_rows': 0,
                'load_time_seconds': round(load_time, 2),
                'job_id': getattr(load_job, 'job_id', None) if 'load_job' in locals() else None,
                'source_file': source_file,
                'errors': error_msg
            }
            
            raise GoogleCloudError(error_msg)
    
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
            table = self.client.get_table(table_ref)
            
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
    
    def run_query(self, query: str) -> pd.DataFrame:
        """
        Run a BigQuery SQL query and return results as DataFrame
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame with query results
        """
        try:
            logger.info(f"Executing query: {query[:100]}...")
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            logger.info(f"Query returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline statistics
        
        Returns:
            Dictionary with pipeline statistics
        """
        stats = {
            'dataset_id': self.dataset_id,
            'project_id': self.project_id,
            'tables': {}
        }
        
        for table_name in self.table_schemas.keys():
            stats['tables'][table_name] = self.get_table_info(table_name)
        
        return stats 