#!/bin/bash
set -x  # Enable debugging for detailed output

# Variables
PROJECT_ID="toast-analytics-444116"
DATASET="toast_raw"
BUCKET_NAME="toast-raw-data"

TABLES=("AllItemsReport_raw" "CashEntries_raw" "CheckDetails_raw" "ItemSelectionDetails_raw" "KitchenTimings_raw" "OrderDetails_raw" "PaymentDetails_raw")
SCHEMA_FILES=("AllItemsReport_raw.json" "CashEntries_raw.json" "CheckDetails_raw.json" "ItemSelectionDetails_raw.json" "KitchenTimings_raw.json" "OrderDetails_raw.json" "PaymentDetails_raw.json")

TEMP_SUFFIX="_new"

# Function to create a new table
create_table() {
    local table_name=$1
    local schema_file=$2
    local temp_table="${table_name}${TEMP_SUFFIX}"

    echo "Creating new table: ${DATASET}.${temp_table} with schema from ${schema_file}..."
    bq mk \
        --project_id="${PROJECT_ID}" \
        --dataset_id="${DATASET}" \
        --table "${DATASET}.${temp_table}" \
        "${schema_file}"
}

# Function to delete an old table
delete_table() {
    local table_name=$1

    echo "Deleting old table: ${DATASET}.${table_name}..."
    bq rm -f -t "${DATASET}.${table_name}"
}

# Function to rename a table
rename_table() {
    local old_table=$1
    local new_table=$2

    echo "Renaming table: ${DATASET}.${old_table} to ${DATASET}.${new_table}..."
    bq cp -n "${DATASET}.${old_table}" "${DATASET}.${new_table}"
    bq rm -f -t "${DATASET}.${old_table}"
}

# Main script logic
echo "Starting BigQuery table update process..."

for i in "${!TABLES[@]}"; do
    table="${TABLES[$i]}"
    schema_file="${SCHEMA_FILES[$i]}"

    # Step 1: Create new table with the updated schema
    create_table "${table}" "${schema_file}"

    # Step 2: Delete the old table
    delete_table "${table}"

    # Step 3: Rename the new table to the original table name
    rename_table "${table}${TEMP_SUFFIX}" "${table}"
done

echo "BigQuery table update process completed successfully."
