import os
import logging
import subprocess
from datetime import datetime, timedelta
from flask import Flask, request
from google.cloud import storage, pubsub_v1, secretmanager

# Initialize Flask App
app = Flask(__name__)

# Setup logging
logging.basicConfig(level=logging.INFO)

# Environment Variables
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "toast-analytics-444116")
BUCKET_NAME = "toast-raw-data"
TOPIC_NAME = "etl_pipeline_notifications"

# Date for SFTP Path
date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
SFTP_PATH = f"185129/{date}/"
RAW_LOCAL_DIR = f"/tmp/toast_raw_data/{date}"
os.makedirs(RAW_LOCAL_DIR, exist_ok=True)

logging.info(f"📂 Using SFTP path: {SFTP_PATH}")

# Load SSH Key from Secret Manager
def get_ssh_key():
    """Retrieve SSH key from Google Secret Manager."""
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{GOOGLE_CLOUD_PROJECT}/secrets/TOAST_SFTP_KEY/versions/latest"
    response = secret_client.access_secret_version(name=secret_name)
    return response.payload.data.decode("utf-8")

# Download from SFTP
def download_from_sftp():
    """Downloads data from SFTP to local directory."""
    ssh_key = get_ssh_key()
    ssh_key_path = "/tmp/toast_ssh"
    
    # Write SSH key to temp file
    with open(ssh_key_path, "w") as key_file:
        key_file.write(ssh_key)
    os.chmod(ssh_key_path, 0o600)

    sftp_command = f"sftp -i {ssh_key_path} -r 'LoveExportUser@s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com:{SFTP_PATH}' {RAW_LOCAL_DIR}"
    
    try:
        subprocess.check_call(sftp_command, shell=True)
        logging.info(f"✅ Data downloaded to {RAW_LOCAL_DIR}")
        return RAW_LOCAL_DIR
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ SFTP download failed: {e}")
        return None

# Upload to GCS
def upload_to_gcs(local_dir):
    """Uploads files to GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    for filename in os.listdir(local_dir):
        if filename.endswith(".csv"):
            local_file_path = os.path.join(local_dir, filename)
            blob = bucket.blob(f"raw/{date}/{filename}")
            blob.upload_from_filename(local_file_path)
            logging.info(f"✅ Uploaded {filename} to GCS")

# Publish Notification to Pub/Sub
def publish_message():
    """Publishes message to Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GOOGLE_CLOUD_PROJECT, TOPIC_NAME)
    message = f"ETL Process completed for {date}"
    publisher.publish(topic_path, message.encode("utf-8"))
    logging.info(f"📢 Notification sent to Pub/Sub.")

# Health Check Endpoint
@app.route("/healthz", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return "OK", 200

# Trigger ETL Function
@app.route("/", methods=["POST"])
def trigger_etl():
    """Cloud Function HTTP Trigger."""
    logging.info("🚀 ETL Function Started.")

    local_dir = download_from_sftp()
    if local_dir:
        upload_to_gcs(local_dir)
        publish_message()
        return "✅ ETL Process Completed Successfully!", 200
    else:
        logging.error("❌ ETL Process Failed!")
        return "❌ ETL Process Failed!", 500

# Run Flask without explicitly setting PORT
if __name__ == "__main__":
    app.run(host="0.0.0.0")
