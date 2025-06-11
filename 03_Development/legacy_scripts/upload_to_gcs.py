import os
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd

# Google Cloud Storage bucket name
BUCKET_NAME = "toast-raw-data"  # Replace with your GCS bucket name

# Get the current date in YYYYMMDD format for dynamic folder paths
date = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")  # Yesterday's date
parsed_date = datetime.strptime(date, "%Y%m%d")
date_formatted = parsed_date.strftime("%Y-%m-%d")

# Path to the folder containing cleaned CSV files
LOCAL_FOLDER = f"/private/tmp/toast_raw_data/cleaned/{date}"

# Path in the GCS bucket to store the files
GCS_FOLDER = f"raw/{date}"  # Desired folder path in your GCS bucket

# Path to log files
LOG_FILE = f"/private/tmp/toast_raw_data/logs/upload_log_{date}.csv"

# Upload files to GCS
def upload_to_gcs(local_file_path, bucket_name, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage.

    Parameters:
        local_file_path (str): Path to the local file to upload.
        bucket_name (str): Name of the GCS bucket.
        destination_blob_name (str): Destination path in the GCS bucket.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        print(f"Uploading {local_file_path} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(local_file_path)
        print(f"Successfully uploaded: {local_file_path}")
    except Exception as e:
        print(f"Error uploading {local_file_path}: {e}")
        raise

def upload_all_files(local_folder, bucket_name, gcs_folder):
    """
    Uploads all CSV files from a local folder to a GCS bucket.

    Parameters:
        local_folder (str): Path to the local folder containing files.
        bucket_name (str): Name of the GCS bucket.
        gcs_folder (str): Destination folder path in the GCS bucket.
    """
    if not os.path.exists(local_folder):
        print(f"Local folder does not exist: {local_folder}")
        return

    for file_name in os.listdir(local_folder):
        if file_name.endswith(".csv"):
            local_file_path = os.path.join(local_folder, file_name)
            destination_blob_name = f"{gcs_folder}/{file_name}"
            try:
                record_count = count_records(local_file_path)
                upload_to_gcs(local_file_path, bucket_name, destination_blob_name)
                log_upload(file_name, record_count)
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

def count_records(file_path):
    """
    Counts the number of records in a CSV file.

    Parameters:
        file_path (str): Path to the CSV file.

    Returns:
        int: Number of records in the file.
    """
    try:
        df = pd.read_csv(file_path)
        return len(df)
    except Exception as e:
        print(f"Error counting records in {file_path}: {e}")
        return 0

def log_upload(file_name, record_count):
    """
    Logs the upload process into a CSV file.

    Parameters:
        file_name (str): Name of the uploaded file.
        record_count (int): Number of records in the file.
    """
    try:
        log_entry = {"file_name": file_name, "record_count": record_count, "date_uploaded": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        if not os.path.exists(LOG_FILE):
            pd.DataFrame([log_entry]).to_csv(LOG_FILE, index=False)
        else:
            pd.DataFrame([log_entry]).to_csv(LOG_FILE, mode="a", index=False, header=False)
        print(f"Logged upload for {file_name}: {record_count} records.")
    except Exception as e:
        print(f"Error logging upload for {file_name}: {e}")

if __name__ == "__main__":
    # Ensure the local folder exists
    if os.path.exists(LOCAL_FOLDER):
        print(f"Uploading files from {LOCAL_FOLDER} to GCS...")
        upload_all_files(LOCAL_FOLDER, BUCKET_NAME, GCS_FOLDER)
    else:
        print(f"Error: Local folder not found: {LOCAL_FOLDER}")