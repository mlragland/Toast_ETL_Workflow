#!/bin/bash

# Retrieve the date from the environment variable
if [ -z "$DATE" ]; then
  echo "Error: DATE environment variable not set."
  exit 1
fi

# Variables
PROJECT_ID="toast-analytics-444116"         # Replace with your GCP project ID
DATASET="toast_raw"                         # BigQuery dataset for raw data
BUCKET_NAME="toast-raw-data"                # Name of the GCS bucket

#DATE=$(date -d "yesterday" +"%Y%m%d")       # Date folder in YYYYMMDD format
#DATE="20241222"       # Date folder in YYYYMMDD format

# File and table mappings (use two arrays for mapping)
FILES=("AllItemsReport_cleaned.csv" "CashEntries_cleaned.csv" "CheckDetails_cleaned.csv" "ItemSelectionDetails_cleaned.csv" "KitchenTimings_cleaned.csv" "OrderDetails_cleaned.csv" "PaymentDetails_cleaned.csv")
TABLES=("AllItemsReport_raw" "CashEntries_raw" "CheckDetails_raw" "ItemSelectionDetails_raw" "KitchenTimings_raw" "OrderDetails_raw" "PaymentDetails_raw")

# Set active project in gcloud (optional, but ensures correctness)
gcloud config set project "$PROJECT_ID" || { echo "Failed to set project. Ensure gcloud is authenticated."; exit 1; }

# Loop through the files and load them into BigQuery
for i in "${!FILES[@]}"; do
  FILE="${FILES[$i]}"                       # Current file name
  TABLE="${TABLES[$i]}"                     # Corresponding table name
  GCS_FILE_PATH="gs://${BUCKET_NAME}/raw/${DATE}/${FILE}"  # Full path to the file in GCS
  SCHEMA_FILE="${TABLE}.json"               # Schema file for the table

  echo "Loading $FILE into $DATASET.$TABLE from $GCS_FILE_PATH"

  # Validate if schema file exists
  if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "Error: Schema file $SCHEMA_FILE not found. Skipping $FILE."
    continue
  fi

  # Validate if file exists in GCS
  if ! gsutil -q stat "$GCS_FILE_PATH"; then
    echo "Error: File $GCS_FILE_PATH not found in GCS. Skipping $FILE."
    continue
  fi

  # Execute BigQuery load
  bq load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    "$DATASET.$TABLE" \
    "$GCS_FILE_PATH" \
    "$SCHEMA_FILE"

  # Check the exit status of the load command
  if [ $? -eq 0 ]; then
    echo "Successfully loaded $FILE into $DATASET.$TABLE."
  else
    echo "Failed to load $FILE into $DATASET.$TABLE. Exiting script."
    exit 1
  fi
done

echo "All files loaded successfully into BigQuery."

