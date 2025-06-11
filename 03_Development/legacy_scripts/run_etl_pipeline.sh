#!/bin/bash

# Variables
PROJECT_ID="toast-analytics-444116"         # Replace with your GCP project ID
DATASET="toast_raw"                         # BigQuery dataset for raw data
BUCKET_NAME="toast-raw-data"                # Name of the GCS bucket
DATE=$(date -d "yesterday" +"%Y%m%d")       # Date folder in YYYYMMDD format
LOG_FILE="/private/tmp/toast_raw_data/cleaned/etl_pipeline_${DATE}.log" # Path to log file

# Log start
echo "Starting ETL pipeline for date $DATE" | tee -a "$LOG_FILE"

# Step 1: Extract data from SFTP
echo "Step 1: Extracting data from SFTP..." | tee -a "$LOG_FILE"
python3 toast_sftp_extract.py --date "$DATE" 2>>"$LOG_FILE"
if [ $? -ne 0 ]; then
  echo "SFTP extraction failed. Exiting." | tee -a "$LOG_FILE"
  exit 1
fi

# Step 2: Transform and clean data
echo "Step 2: Transforming and cleaning data..." | tee -a "$LOG_FILE"
python3 transform_cleaning_pipeline.py --date "$DATE" 2>>"$LOG_FILE"
if [ $? -ne 0 ]; then
  echo "Data transformation failed. Exiting." | tee -a "$LOG_FILE"
  exit 1
fi

# Step 3: Upload transformed data to GCS
echo "Step 3: Uploading data to GCS..." | tee -a "$LOG_FILE"
python3 upload_to_gcs.py --date "$DATE" --bucket "$BUCKET_NAME" 2>>"$LOG_FILE"
if [ $? -ne 0 ]; then
  echo "GCS upload failed. Exiting." | tee -a "$LOG_FILE"
  exit 1
fi

# Step 4: Load data into BigQuery
echo "Step 4: Loading data into BigQuery..." | tee -a "$LOG_FILE"

FILES=("AllItemsReport_cleaned.csv" "CashEntries_cleaned.csv" "CheckDetails_cleaned.csv" "ItemSelectionDetails_cleaned.csv" "KitchenTimings_cleaned.csv" "OrderDetails_cleaned.csv" "PaymentDetails_cleaned.csv")
TABLES=("AllItemsReport_raw" "CashEntries_raw" "CheckDetails_raw" "ItemSelectionDetails_raw" "KitchenTimings_raw" "OrderDetails_raw" "PaymentDetails_raw")

for i in "${!FILES[@]}"; do
  FILE="${FILES[$i]}"
  TABLE="${TABLES[$i]}"
  GCS_FILE_PATH="gs://${BUCKET_NAME}/raw/${DATE}/${FILE}"
  SCHEMA_FILE="${TABLE}.json"

  echo "Checking if data for $DATE already exists in $TABLE..." | tee -a "$LOG_FILE"
  EXISTS=$(bq query --use_legacy_sql=false --format=json \
    "SELECT COUNT(*) AS cnt FROM \`${PROJECT_ID}.${DATASET}.${TABLE}\` WHERE processing_date='${DATE}'" \
    | jq -r '.[0].cnt')

  if [ "$EXISTS" -gt 0 ]; then
    echo "Data for $DATE already exists in $TABLE. Skipping load." | tee -a "$LOG_FILE"
    continue
  fi

  echo "Loading $FILE into $TABLE from $GCS_FILE_PATH..." | tee -a "$LOG_FILE"

  if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "Error: Schema file $SCHEMA_FILE not found. Skipping $FILE." | tee -a "$LOG_FILE"
    continue
  fi

  if ! gsutil -q stat "$GCS_FILE_PATH"; then
    echo "Error: File $GCS_FILE_PATH not found in GCS. Skipping $FILE." | tee -a "$LOG_FILE"
    continue
  fi

  bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    "${DATASET}.${TABLE}" \
    "${GCS_FILE_PATH}" \
    "${SCHEMA_FILE}" 2>>"$LOG_FILE"

  if [ $? -eq 0 ]; then
    echo "Successfully loaded $FILE into $TABLE." | tee -a "$LOG_FILE"
  else
    echo "Failed to load $FILE into $TABLE. Exiting." | tee -a "$LOG_FILE"
    exit 1
  fi
done

echo "ETL pipeline completed successfully for date $DATE." | tee -a "$LOG_FILE"
