from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import subprocess

# Define DAG metadata
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'toast_etl_dag',
    default_args=default_args,
    description='ETL Pipeline for Toast Data',
    schedule_interval='0 3 * * *',  # Runs daily at 3 AM UTC
    catchup=False,
)

# Task 1: Extract, Transform & Upload CSVs
def run_etl_script():
    subprocess.run(["python3", "/home/airflow/gcs/dags/lov3_ETL_consolidated_pipeline.py"], check=True)

extract_transform_upload_task = PythonOperator(
    task_id='extract_transform_upload',
    python_callable=run_etl_script,
    dag=dag,
)

# Task 2: Load Data to BigQuery via Bash
load_bigquery_task = BashOperator(
    task_id='load_to_bigquery',
    bash_command='bash /home/airflow/gcs/scripts/load_toast_data.sh',
    dag=dag,
)

# DAG Workflow
extract_transform_upload_task >> load_bigquery_task
