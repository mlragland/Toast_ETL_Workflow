import os
import logging
import subprocess
from datetime import datetime, timedelta
from google.cloud import pubsub_v1, storage

# Setup logging
logging.basicConfig(level=logging.INFO)

# Define Variables
SFTP_USER = "LoveExportUser"
SFTP_SERVER = "s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com"
SFTP_PATH_TEMPLATE = "185129/{date}/*"
SSH_KEY_PATH = "~/.ssh/toast_ssh"
RAW_LOCAL_DIR = "/tmp/toast_raw_data/raw"
BUCKET_NAME = "toast-raw-data"
TOPIC_NAME = "etl_pipeline_notifications"

# Get date
date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

def download_from_sftp():
    """Downloads data from SFTP to local directory"""
    sftp_path = SFTP_PATH_TEMPLATE.format(date=date)
    local_dir = os.path.join(RAW_LOCAL_DIR, date)
    os.makedirs(local_dir, exist_ok=True)

    sftp_command = f"sftp -i {SSH_KEY_PATH} -r '{SFTP_USER}@{SFTP_SERVER}:{sftp_path}' {local_dir}"
    
    try:
        subprocess.check_call(sftp_command, shell=True)
        logging.info(f"Data downloaded to {local_dir}")
        return local_dir
    except subprocess.CalledProcessError as e:
        logging.error(f"SFTP download failed: {e}")
        return None

def upload_to_gcs(local_dir):
    """Uploads processed files to GCS"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    for filename in os.listdir(local_dir):
        if filename.endswith(".csv"):
            local_file_path = os.path.join(local_dir, filename)
            blob = bucket.blob(f"raw/{date}/{filename}")
            blob.upload_from_filename(local_file_path)
            logging.info(f"Uploaded {filename} to GCS")

def publish_message():
    """Publishes message to Pub/Sub"""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("GOOGLE_CLOUD_PROJECT"), TOPIC_NAME)
    message = f"ETL Process completed for {date}"
    publisher.publish(topic_path, message.encode("utf-8"))
    logging.info("Notification sent to Pub/Sub.")

def main(event, context):
    """Cloud Function Entry Point"""
    local_dir = download_from_sftp()
    if local_dir:
        upload_to_gcs(local_dir)
        publish_message()
    else:
        logging.error("ETL Process failed.")

