# Main Files Summary

This document outlines the main scripts and configuration files in the `bigquery_toast_loader` repository and their primary responsibilities.

## 1. Extraction (SFTP → Local / GCS)
- **toast_etl.py**: Standalone Python/Paramiko script to download CSV exports via SFTP and upload to GCS.
- **main.py**: Flask-based Cloud Function HTTP trigger (shells out to `sftp`) for SFTP download, GCS upload, and Pub/Sub notification.
- **sftp-to-gcs/main.py**: Pub/Sub-triggered Cloud Function using Paramiko for SFTP → GCS.
- **python_etl_pipeline.py**, **lov3_ETL_consolidated_pipeline.py**, **lov3_etl_final_pipeline_20241218.py**, **extract_transform.py**: Variants of combined SFTP → transform → GCS → Pub/Sub pipelines (CLI and Cloud Function flavors).

## 2. Orchestration
- **toast_etl_dag.py**: Airflow DAG that runs the main ETL pipeline and invokes a BashOperator for BigQuery loading.
- **run_etl_pipeline.sh**: Shell wrapper chaining extraction, transform, upload, and `bq load` steps.

## 3. Transformation / Cleaning
- **transform_cleaning_pipeline.py** (and variant **transform_cleaning_pipeline1.py**): Core Pandas-based scripts to rename columns, format dates/times, fill nulls, and produce cleaned CSVs.

## 4. Staging to GCS
- **upload_to_gcs.py**, **upload_to_gcs_Manual.py**: Python scripts to upload cleaned CSVs to Cloud Storage, optionally logging record counts.

## 5. BigQuery Loading & Maintenance
- **load_toast_data.sh**, **bigquery_data_load_manual.sh**: Bash scripts looping through cleaned CSVs and loading into BigQuery with JSON schemas.
- **bigquery_data_deletion.py**: Python helper to delete existing rows for a specific processing date.
- **update_bigquery_tables.sh**: Bash script to swap in new BigQuery table schemas via rename workflows.

## 6. Containerization & Configuration
- **toast-etl-container/Dockerfile**: Docker recipe for containerized deployment of the Flask/Cloud Function pipeline.
- **docker-compose.yml**, **.env**, **requirements.txt**: Local development setup and dependency specifications.