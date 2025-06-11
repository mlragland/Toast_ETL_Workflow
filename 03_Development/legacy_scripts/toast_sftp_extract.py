import os
import subprocess
import logging
from datetime import datetime,timedelta
# SFTP connection details
SFTP_USER = "LoveExportUser"
SFTP_SERVER = "s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com"
SFTP_PATH_TEMPLATE = "185129/{date}/*"
SSH_KEY_PATH = "~/.ssh/toast_ssh"

# Local directory to stage raw data
RAW_LOCAL_DIR = f"/tmp/toast_raw_data/raw"
#/private/tmp/toast_raw_data/raw/20241206 (files stored in private tmp direcory)


# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

def download_from_sftp(date):
    """
    Downloads raw CSV files from the SFTP server for the specified date.

    Parameters:
        date (str): The date folder on the SFTP server in YYYYMMDD format.

    Returns:
        str: The local directory where files were staged, or None if an error occurred.
    """
    try:
        logging.info(f"Starting SFTP download for date: {date}")
        
        # Remote SFTP path and local staging directory
        sftp_path = SFTP_PATH_TEMPLATE.format(date=date)
        local_dir = os.path.join(RAW_LOCAL_DIR, date)

        # Ensure the local staging directory exists
        os.makedirs(local_dir, exist_ok=True)

        # SFTP command
        sftp_command = f"sftp -i {SSH_KEY_PATH} -r '{SFTP_USER}@{SFTP_SERVER}:{sftp_path}' {local_dir}"
        logging.info(f"Executing command: {sftp_command}")
        subprocess.check_call(sftp_command, shell=True)

        logging.info(f"Files downloaded successfully to: {local_dir}")
        return local_dir
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during SFTP download: {e}")
        return None

def main():
    """
    Main function to extract data from the SFTP server and stage it locally.
    """
    # Get the current date in YYYYMMDD format
    #date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    date = "20241214"

    # Step 1: Download files from SFTP
    local_dir = download_from_sftp(date)
    if not local_dir:
        logging.error("SFTP download failed. Exiting.")
        return

    logging.info("SFTP extraction completed successfully.")

if __name__ == "__main__":
    main()
