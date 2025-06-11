import os
import subprocess
from datetime import datetime
from google.cloud import storage
import logging
from transform_csv import transform_csv  # Import the transformation function from your script

# Google Cloud Storage bucket name
BUCKET_NAME = "toast-raw-data"

# SFTP details
SFTP_USER = "LoveExportUser"
SFTP_SERVER = "s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com"
SFTP_PATH_TEMPLATE = "185129/{date}/*"
SSH_KEY_PATH = "~/.ssh/toast_ssh"

# Local staging directories
RAW_LOCAL_DIR = "/tmp/toast_raw_data/raw"
TRANSFORMED_LOCAL_DIR = "/tmp/toast_raw_data/transformed"

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

def download_from_sftp(date):
    """
    Downloads files from the SFTP server for a specific date.
    """
    try:
        logging.info(f"Starting SFTP download for date: {date}")
        sftp_path = SFTP_PATH_TEMPLATE.format(date=date)
        local_dir = os.path.join(RAW_LOCAL_DIR, date)

        # Ensure local raw staging directory exists
        os.makedirs(local_dir, exist_ok=True)

        # SFTP command
        sftp_command = f"sftp -i {SSH_KEY_PATH} -r '{SFTP_USER}@{SFTP_SERVER}:{sftp_path}' {local_dir}"
        logging.info(f"Executing: {sftp_command}")
        subprocess.check_call(sftp_command, shell=True)

        logging.info(f"Files downloaded to {local_dir}")
        return local_dir
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during SFTP download: {e}")
        return None

def transform_files(raw_dir, transformed_dir):
    """
    Transforms all files in the raw directory and saves them in the transformed directory.
    """
    try:
        logging.info(f"Starting transformation of files in {raw_dir}")
        os.makedirs(transformed_dir, exist_ok=True)

        for file_name in os.listdir(raw_dir):
            if file_name.endswith(".csv"):
                raw_file_path = os.path.join(raw_dir, file_name)
                transformed_file_path = os.path.join(transformed_dir, file_name.replace(".csv", "_cleaned.csv"))

                logging.info(f"Transforming file: {file_name}")
                transform_csv(raw_file_path, transformed_file_path)
                logging.info(f"Transformed file saved as: {transformed_file_path}")
    except Exception as e:
        logging.error(f"Error during file transformation: {e}")

def upload_to_gcs(local_dir, date):
    """
    Uploads files from the transformed staging area to GCS.
    """
    try:
        logging.info(f"Starting upload to GCS for date: {date}")
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(BUCKET_NAME)
        gcs_folder = f"raw/{date}"

        for root, _, files in os.walk(local_dir):
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                blob_name = f"{gcs_folder}/{file_name}"
                blob = bucket.blob(blob_name)

                logging.info(f"Uploading {local_file_path} to gs://{BUCKET_NAME}/{blob_name}")
                blob.upload_from_filename(local_file_path)

        logging.info(f"Upload to GCS completed for date: {date}")
    except Exception as e:
        logging.error(f"Error during GCS upload: {e}")

def main():
    # Current date in YYYYMMDD format
    date = datetime.now().strftime("%Y%m%d")

    # Step 1: Download files from SFTP
    raw_dir = download_from_sftp(date)
    if not raw_dir:
        logging.error("SFTP download failed. Exiting.")
        return

    # Step 2: Transform files
    transformed_dir = os.path.join(TRANSFORMED_LOCAL_DIR, date)
    transform_files(raw_dir, transformed_dir)

    # Step 3: Upload transformed files to GCS
    upload_to_gcs(transformed_dir, date)

    # Step 4: Cleanup local staging directories
    logging.info("Cleaning up local staging directories...")
    os.system(f"rm -rf {RAW_LOCAL_DIR} {TRANSFORMED_LOCAL_DIR}")
    logging.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
