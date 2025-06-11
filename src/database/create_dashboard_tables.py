"""
Dashboard Database Setup - Toast ETL Pipeline
Creates necessary tables for dashboard functionality and ETL run tracking.
"""

import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_dashboard_tables():
    """Create dashboard tracking tables in BigQuery."""
    
    # Initialize BigQuery client
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
    
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    
    logger.info(f"Creating dashboard tables in {project_id}.{dataset_id}")
    
    # ETL Runs Tracking Table
    etl_runs_table_id = f"{project_id}.{dataset_id}.etl_runs"
    etl_runs_schema = [
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("execution_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # 'success', 'failed', 'running'
        bigquery.SchemaField("files_processed", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("records_processed", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("total_sales", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("execution_time_seconds", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("source_type", "STRING", mode="NULLABLE"),  # 'scheduled', 'manual', 'backfill'
        bigquery.SchemaField("files_detail", "JSON", mode="NULLABLE")  # Detailed file processing info
    ]
    
    create_table_if_not_exists(client, etl_runs_table_id, etl_runs_schema, "ETL Runs")
    
    # Backfill Jobs Tracking Table
    backfill_jobs_table_id = f"{project_id}.{dataset_id}.backfill_jobs"
    backfill_jobs_schema = [
        bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("requested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("date_range_start", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("date_range_end", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # 'queued', 'running', 'completed', 'failed'
        bigquery.SchemaField("progress_percentage", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("dates_processed", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("total_dates", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("records_added", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("requested_by", "STRING", mode="NULLABLE")
    ]
    
    create_table_if_not_exists(client, backfill_jobs_table_id, backfill_jobs_schema, "Backfill Jobs")
    
    logger.info("✅ Dashboard tables created successfully")


def create_table_if_not_exists(client, table_id, schema, table_name):
    """Create a BigQuery table if it doesn't exist."""
    try:
        client.get_table(table_id)
        logger.info(f"📋 {table_name} table already exists: {table_id}")
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        
        # Add partitioning for performance
        if 'etl_runs' in table_id:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="execution_date"
            )
        elif 'backfill_jobs' in table_id:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="requested_at"
            )
        
        table = client.create_table(table)
        logger.info(f"✅ Created {table_name} table: {table.table_id}")


def create_dashboard_views():
    """Create helpful views for dashboard queries."""
    
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
    client = bigquery.Client(project=project_id)
    
    # Daily Summary View
    daily_summary_view = f"""
    CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.daily_summary` AS
    SELECT 
        execution_date,
        COUNT(*) as runs_count,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
        AVG(execution_time_seconds) as avg_execution_time,
        SUM(records_processed) as total_records,
        SUM(total_sales) as total_sales,
        MAX(completed_at) as last_run_time
    FROM `{project_id}.{dataset_id}.etl_runs`
    WHERE status IN ('success', 'failed')
    GROUP BY execution_date
    ORDER BY execution_date DESC
    """
    
    # Recent Runs View  
    recent_runs_view = f"""
    CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.recent_runs` AS
    SELECT 
        run_id,
        execution_date,
        started_at,
        completed_at,
        status,
        files_processed,
        records_processed,
        total_sales,
        execution_time_seconds,
        source_type,
        error_message,
        DATETIME_DIFF(completed_at, started_at, SECOND) as duration_seconds
    FROM `{project_id}.{dataset_id}.etl_runs`
    ORDER BY started_at DESC
    LIMIT 100
    """
    
    # Execute view creation
    for view_name, view_sql in [
        ("Daily Summary", daily_summary_view),
        ("Recent Runs", recent_runs_view)
    ]:
        try:
            client.query(view_sql).result()
            logger.info(f"✅ Created {view_name} view")
        except Exception as e:
            logger.error(f"❌ Failed to create {view_name} view: {str(e)}")


if __name__ == "__main__":
    logger.info("🚀 Setting up Dashboard Database Tables")
    create_dashboard_tables()
    create_dashboard_views()
    logger.info("✅ Dashboard database setup complete") 