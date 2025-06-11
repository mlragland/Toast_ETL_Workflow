import os
import subprocess
import logging
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Global Variables
SFTP_USER = "LoveExportUser"
SFTP_SERVER = "s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com"
SFTP_PATH_TEMPLATE = "185129/{date}/*"
SSH_KEY_PATH = "~/.ssh/toast_ssh"

RAW_LOCAL_DIR = "/tmp/toast_raw_data/raw"
CLEANED_LOCAL_DIR = "/tmp/toast_raw_data/cleaned"
LOG_DIR = "/tmp/toast_raw_data/logs"

BUCKET_NAME = "toast-raw-data"

BASH_SCRIPT_PATH = "./load_toast_data.sh"  # Path to Bash script

def get_date(delta_days=1):
    """
    Returns the date in YYYYMMDD format adjusted by delta_days.
    """
    return (datetime.now() - timedelta(days=delta_days)).strftime("%Y%m%d")

def download_from_sftp(date):
    """
    Downloads raw CSV files from the SFTP server for the specified date.
    """
    try:
        logging.info(f"Starting SFTP download for date: {date}")
        sftp_path = SFTP_PATH_TEMPLATE.format(date=date)
        local_dir = os.path.join(RAW_LOCAL_DIR, date)

        os.makedirs(local_dir, exist_ok=True)
        sftp_command = f"sftp -i {SSH_KEY_PATH} -r '{SFTP_USER}@{SFTP_SERVER}:{sftp_path}' {local_dir}"
        logging.info(f"Executing command: {sftp_command}")
        subprocess.check_call(sftp_command, shell=True)
        logging.info(f"Files downloaded successfully to: {local_dir}")
        return local_dir
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during SFTP download: {e}")
        return None

def upload_to_gcs(local_file_path, bucket_name, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        logging.info(f"Uploading {local_file_path} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(local_file_path)
        logging.info(f"Successfully uploaded: {local_file_path}")
    except Exception as e:
        logging.error(f"Error uploading {local_file_path}: {e}")

def upload_all_files(local_folder, bucket_name, gcs_folder):
    """
    Uploads all CSV files from a local folder to a GCS bucket.
    """
    if not os.path.exists(local_folder):
        logging.error(f"Local folder does not exist: {local_folder}")
        return

    for file_name in os.listdir(local_folder):
        if file_name.endswith(".csv"):
            local_file_path = os.path.join(local_folder, file_name)
            destination_blob_name = f"{gcs_folder}/{file_name}"
            upload_to_gcs(local_file_path, bucket_name, destination_blob_name)

def call_bash_script(date):
    """
    Calls the load_toast_data.sh Bash script and captures the output.
    """
    try:
        logging.info(f"Executing {BASH_SCRIPT_PATH} with DATE={date}")
        result = subprocess.run(
            ["bash", BASH_SCRIPT_PATH],
            env={**os.environ, "DATE": date},
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Bash Script Output:\n" + result.stdout)
        if result.stderr:
            logging.error("Bash Script Errors:\n" + result.stderr)
    except subprocess.CalledProcessError as e:
        logging.error(f"Bash script failed with return code {e.returncode}")
        logging.error(f"Output: {e.stdout}")
        logging.error(f"Error: {e.stderr}")

def transform_files(date):
    """
    Transforms raw CSV files into cleaned CSV files.
    """
    processing_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
    input_folder = os.path.join(RAW_LOCAL_DIR, date)
    output_folder = os.path.join(CLEANED_LOCAL_DIR, date)
    os.makedirs(output_folder, exist_ok=True)

    # Dummy transformation logic (replace with actual logic)
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".csv"):
            logging.info(f"Processing file: {file_name}")
            input_file = os.path.join(input_folder, file_name)
            output_file = os.path.join(output_folder, file_name.replace(".csv", "_cleaned.csv"))
            try:
                df = pd.read_csv(input_file)
                # Add your transformation logic here
                df["processing_date"] = processing_date
                df.to_csv(output_file, index=False)
                logging.info(f"File processed and saved to: {output_file}")
            except Exception as e:
                logging.error(f"Error processing file {file_name}: {e}")

def main():
    """
    Main ETL pipeline function.
    """
    date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")  #Yesterday

    # Step 1: Extract
    logging.info("Step 1: Extracting data from SFTP...")
    local_dir = download_from_sftp(date)
    if not local_dir:
        logging.error("Step 1 failed. Exiting.")
        return

    # Step 2: Transform
    logging.info("Step 2: Transforming and cleaning data...")
    transform_files(date)

    # Step 3: Stage to GCS
    logging.info("Step 3: Uploading data to GCS...")
    local_folder = os.path.join(CLEANED_LOCAL_DIR, date)
    gcs_folder = f"raw/{date}"
    upload_all_files(local_folder, BUCKET_NAME, gcs_folder)

    # Step 4: Load to BigQuery
    logging.info("Step 4: Loading data to BigQuery...")
    call_bash_script(date)

if __name__ == "__main__":
    main()
