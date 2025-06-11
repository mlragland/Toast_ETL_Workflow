import os
import paramiko
import logging
from google.cloud import storage
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO)

# Define Constants
SFTP_HOST = "s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com"
SFTP_USER = "LoveExportUser"
GCS_BUCKET = "toast-raw-data"
DATE = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
LOCAL_DIR = f"/tmp/toast_raw_data/{DATE}"
SFTP_REMOTE_PATH = f"185129/{DATE}/"

# Ensure Local Directory
os.makedirs(LOCAL_DIR, exist_ok=True)

def download_sftp():
    """Download files from SFTP to local storage."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    private_key = paramiko.RSAKey(filename="/home/airflow/gcs/secrets/toast_ssh")

    ssh.connect(SFTP_HOST, username=SFTP_USER, pkey=private_key)
    sftp = ssh.open_sftp()
    
    try:
        files = sftp.listdir(SFTP_REMOTE_PATH)
        for file in files:
            local_file = os.path.join(LOCAL_DIR, file)
            remote_file = f"{SFTP_REMOTE_PATH}/{file}"
            sftp.get(remote_file, local_file)
            logging.info(f"✅ Downloaded {file} to {local_file}")
    except Exception as e:
        logging.error(f"❌ SFTP Download Failed: {e}")
    finally:
        sftp.close()
        ssh.close()

def upload_to_gcs():
    """Uploads files to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    for file in os.listdir(LOCAL_DIR):
        local_path = os.path.join(LOCAL_DIR, file)
        blob = bucket.blob(f"raw/{DATE}/{file}")
        blob.upload_from_filename(local_path)
        logging.info(f"✅ Uploaded {file} to GCS.")

if __name__ == "__main__":
    download_sftp()
    upload_to_gcs()
