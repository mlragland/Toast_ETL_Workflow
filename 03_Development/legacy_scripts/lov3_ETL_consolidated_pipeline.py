# Purpose:  This script loads data from Toast POS SFTP Server Loads 
# the data into a GCP Bucket for stagining and finaly loads the data
# into BigQuery for further analytics. 
# Google Cloud Console Project
# Project name: toast-analytics
# Project id: toast-analytics-444116
# PSUB Service Account: toast-analytics-444116@appspot.gserviceaccount.com
# Topic name: projects/toast-analytics-444116/topics/etl_pipeline_notifications
# Project: toast-analytics-444116
# Subscription Name: projects/toast-analytics-444116/subscriptions/etl_pipeline_notifications-sub
# Subscription ID: etl_pipeline_notifications-sub
# Service Account: sftp-google-cloud@toast-analytics-444116.iam.gserviceaccount.com


import os
import subprocess
import logging
from datetime import datetime,timedelta
import re
import pandas as pd
from google.cloud import storage
import subprocess
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
from google.cloud.logging_v2.handlers import CloudLoggingHandler

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],  # Local console logging
)

# Initialize Cloud Logging client and handler
client = cloud_logging.Client()
cloud_handler = CloudLoggingHandler(client)
cloud_handler.setLevel(logging.INFO)

# Add Cloud Logging to the logging setup
logging.getLogger().addHandler(cloud_handler)

# Example log to test the setup
logging.info("Cloud Logging has been initialized.")

date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")  #Yesterday
############################## Extract #########################################
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


######################## TRANSFORM ###########################################

# Define column mappings and date columns for each CSV file
FILE_CONFIG = {
    "AllItemsReport.csv": {
            "column_mapping": {
            "Master ID": "master_id",
            "Item ID": "item_id",
            "Parent ID": "parent_id",
            "Menu Name": "menu_name",
            "Menu Group": "menu_group",
            "Subgroup": "subgroup",
            "Menu Item": "menu_item",
            "Tags": "tags",
            "Avg Price": "avg_price",
            "Item Qty (incl voids)": "item_qty_incl_voids",
            "% of Ttl Qty (incl voids)": "percent_ttl_qty_incl_voids",
            "Gross Amount (incl voids)": "gross_amount_incl_voids",
            "% of Ttl Amt (incl voids)": "percent_ttl_amt_incl_voids",
            "Item Qty": "item_qty",
            "Gross Amount": "gross_amount",
            "Void Qty": "void_qty",
            "Void Amount": "void_amount",
            "Discount Amount": "discount_amount",
            "Net Amount": "net_amount",
            "# Orders": "num_orders",
            "% of Ttl # Orders": "percent_ttl_num_orders",
            "% Qty (Group)": "percent_qty_group",
            "% Qty (Menu)": "percent_qty_menu",
            "% Qty (All)": "percent_qty_all",
            "% Net Amt (Group)": "percent_net_amt_group",
            "% Net Amt (Menu)": "percent_net_amt_menu",
            "% Net Amt (All)": "percent_net_amt_all"
        },
        "date_columns": ["processing_date"]  # Date fields to be formatted
    },
    "CheckDetails.csv": {
        "column_mapping": {
            "Customer Id": "customer_id",
            "Customer": "customer",
            "Customer Phone": "customer_phone",
            "Customer Email": "customer_email",
            "Location Code": "location_code",
            "Opened Date": "opened_date",
            "Opened Time": "opened_time",
            "Item Description": "item_description",
            "Server": "server",
            "Tax": "tax",
            "Tender": "tender",
            "Check Id": "check_id",
            "Check #": "check_number",
            "Total": "total",
            "Customer Family": "customer_family",
            "Table Size": "table_size",
            "Discount": "discount",
            "Reason of Discount": "reason_of_discount",
            "Link": "link"
        },
        "date_columns": ["processing_date","opened_date"],  # Date fields to be formatted
        "time_columns": ["opened_time"]  # Date fields to be formatted  
    },
    "CashEntries.csv": {
        "column_mapping": {
            "Location": "location",
            "Entry Id": "entry_id",
            "Created Date": "created_date",
            "Action": "action",
            "Amount": "amount",
            "Cash Drawer": "cash_drawer",
            "Payout Reason": "payout_reason",
            "No Sale Reason": "no_sale_reason",
            "Comment": "comment",
            "Employee": "employee",
            "Employee 2": "employee_2"
        },
        "date_columns": ["processing_date"],  # Date fields to be formatted
        "datetime_columns": ["created_date"]  # Date fields to be formatted
    },
    "ItemSelectionDetails.csv": {
        "column_mapping": {
            "Location": "location",
            "Order Id": "order_id",
            "Order #": "order_number",
            "Sent Date": "sent_date",
            "Order Date": "order_date",
            "Check Id": "check_id",
            "Server": "server",
            "Table": "table",
            "Dining Area": "dining_area",
            "Service": "service",
            "Dining Option": "dining_option",
            "Item Selection Id": "item_selection_id",
            "Item Id": "item_id",
            "Master Id": "master_id",
            "SKU": "sku",
            "PLU": "plu",
            "Menu Item": "menu_item",
            "Menu Subgroup(s)": "menu_subgroup",
            "Menu Group": "menu_group",
            "Menu": "menu",
            "Sales Category": "sales_category",
            "Gross Price": "gross_price",
            "Discount": "discount",
            "Net Price": "net_price",
            "Qty": "quantity",
            "Tax": "tax",
            "Void?": "void",
            "Deferred": "deferred",
            "Tax Exempt": "tax_exempt",
            "Tax Inclusion Option": "tax_inclusion_option",
            "Dining Option Tax": "dining_option_tax",
            "Tab Name": "tab_name"
        },
        "date_columns": ["processing_date"],  # Date fields to be formatted
        "datetime_columns": ["sent_date","order_date"]  # Date fields to be formatted
    },
    "KitchenTimings.csv": {
        "column_mapping": {
            "Location": "location",
            "ID": "id",
            "Server": "server",
            "Check #": "check_number",
            "Table": "table",
            "Check Opened": "check_opened",
            "Station": "station",
            "Expediter Level": "expediter_level",
            "Fired Date": "fired_date",
            "Fulfilled Date": "fulfilled_date",
            "Fulfillment Time": "fulfillment_time",
            "Fulfilled By": "fulfilled_by"
        },
        "date_columns": ["processing_date"],  # Date fields to be formatted
        "datetime_columns": ["check_opened","fired_date","fulfilled_date"]  # Date fields to be formatteda
    },
    "OrderDetails.csv": {
        "column_mapping": {
            "Location": "location",
            "Order Id": "order_id",
            "Order #": "order_number",
            "Checks": "checks",
            "Opened": "opened",
            "# of Guests": "guest_count",
            "Tab Names": "tab_names",
            "Server": "server",
            "Table": "table",
            "Revenue Center": "revenue_center",
            "Dining Area": "dining_area",
            "Service": "service",
            "Dining Options": "dining_options",
            "Discount Amount": "discount_amount",
            "Amount": "amount",
            "Tax": "tax",
            "Tip": "tip",
            "Gratuity": "gratuity",
            "Total": "total",
            "Voided": "voided",
            "Paid": "paid",
            "Closed": "closed",
            "Duration (Opened to Paid)": "duration_opened_to_paid",
            "Order Source": "order_source"
        },
        "date_columns": ["processing_date"],  # Date fields to be formatted
        "datetime_columns": ["opened","paid","closed"],  # Date fields to be formatteda
        "time_columns": ["duration_opened_to_paid"]
    },
    "PaymentDetails.csv": {
        "column_mapping": {
            "Location": "location",
            "Payment Id": "payment_id",
            "Order Id": "order_id",
            "Order #": "order_number",
            "Paid Date": "paid_date",
            "Order Date": "order_date",
            "Check Id": "check_id",
            "Check #": "check_number",
            "Tab Name": "tab_name",
            "Server": "server",
            "Table": "table",
            "Dining Area": "dining_area",
            "Service": "service",
            "Dining Option": "dining_option",
            "House Acct #": "house_account_number",
            "Amount": "amount",
            "Tip": "tip",
            "Gratuity": "gratuity",
            "Total": "total",
            "Swiped Card Amount": "swiped_card_amount",
            "Keyed Card Amount": "keyed_card_amount",
            "Amount Tendered": "amount_tendered",
            "Refunded": "refunded",
            "Refund Date": "refund_date",
            "Refund Amount": "refund_amount",
            "Refund Tip Amount": "refund_tip_amount",
            "Void User": "void_user",
            "Void Approver": "void_approver",
            "Void Date": "void_date",
            "Status": "status",
            "Type": "type",
            "Cash Drawer": "cash_drawer",
            "Card Type": "card_type",
            "Other Type": "other_type",
            "Email": "email",
            "Phone": "phone",
            "Last 4 Card Digits": "last_4_card_digits",
            "V/MC/D Fees": "vmcd_fees",
            "Room Info": "room_info",
            "Receipt": "receipt",
            "Source": "source",
            "Last 4 Gift Card Digits": "last_4_gift_card_digits",
            "First 5 Gift Card Digits": "first_5_gift_card_digits"
        },
        "date_columns": ["processing_date"],  # Date fields to be formatted
        "datetime_columns": ["paid_date","refund_date","order_date","void_date"]  # Date fields to be formatteda
    }
}



def convert_to_minutes(time_str):
    """
    Converts a time string like 'X hours, Y minutes, Z seconds' into total minutes.

    Parameters:
        time_str (str): Input time string.
    Returns:
        str: Total minutes up to 1 decimal place as a string.
    """
    try:
        # Normalize the string to lowercase
        time_str = time_str.lower()
        
        # Define regex patterns for hours, minutes, and seconds
        hour_pattern = r"(\d+)\s*hour"
        minute_pattern = r"(\d+)\s*minute"
        second_pattern = r"(\d+)\s*second"
        
        # Find all matches in the string
        hours = re.findall(hour_pattern, time_str)
        minutes = re.findall(minute_pattern, time_str)
        seconds = re.findall(second_pattern, time_str)
        
        # Convert matches to integers, defaulting to 0 if not found
        hours = int(hours[0]) if hours else 0
        minutes = int(minutes[0]) if minutes else 0
        seconds = int(seconds[0]) if seconds else 0
        
        # Calculate total minutes
        total_minutes = hours * 60 + minutes + seconds / 60
        return f"{total_minutes:.1f}"  # Format to 1 decimal place
    except Exception as e:
        print(f"Error converting time: '{time_str}', Error: {e}")
        return "0.0"  # Return 0.0 minutes if an error occurs
    
    
def transform_csv(input_file, output_file, config, processing_date):
    """
    Transforms a CSV file by renaming columns, adding a processing date, formatting date/time columns, 
    and ensuring certain columns remain as strings without scientific notation.

    Parameters:
        input_file (str): Path to the raw input CSV file.
        output_file (str): Path to save the transformed CSV file.
        config (dict): Configuration dictionary for column mappings and date/time handling.
        processing_date (str): Processing date in YYYY-MM-DD format.
    """
    try:
        print(f"Processing file: {input_file}")
        # Read CSV, force columns to strings if it's AllItemsReport.csv
        if os.path.basename(input_file) == "AllItemsReport.csv":
            print("Reading AllItemsReport.csv with explicit string types...")
            df = pd.read_csv(input_file, dtype={"Master ID": str, "Item ID": str, "Parent ID": str})
        else:
            df = pd.read_csv(input_file)

        # Rename columns
        print("Renaming columns...")
        df.rename(columns=config["column_mapping"], inplace=True)

        # Format date columns
        if "date_columns" in config:
            for col in config["date_columns"]:
                if col in df.columns:
                    print(f"Formatting date column: {col}")
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime('%Y-%m-%d')

        # Format datetime columns
        if "datetime_columns" in config:
            for col in config["datetime_columns"]:
                if col in df.columns:
                    print(f"Formatting datetime column: {col}")
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S')

        # Ensure certain columns in AllItemsReport.csv remain as strings
        if os.path.basename(input_file) == "AllItemsReport.csv":
            columns_to_convert = ["master_id", "item_id", "parent_id"]
            for col in columns_to_convert:
                if col in df.columns:
                    print(f"Ensuring {col} remains as string...")
                    df[col] = df[col].apply(lambda x: f"{x}" if pd.notnull(x) else "")
                    
        # Format time columns
        if "time_columns" in config:
            for col in config["time_columns"]:
                if col in df.columns:
                    print(f"Formatting time column: {col}")
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%H:%M:%S')
                    
        # Special handling for KitchenTimings.csv
        if os.path.basename(input_file) == "KitchenTimings.csv":
            if "fulfillment_time" in df.columns:
                print("Converting fulfillment_time column to total minutes...")
                df["fulfillment_time"] = df["fulfillment_time"].apply(convert_to_minutes)

        # Add processing_date column
        df["processing_date"] = processing_date

        # Handle missing values
        print("Handling missing values...")
        df.fillna("", inplace=True)
        
        # Log number of records transformed
        print(f"Number of records processed: {len(df)}")

        # Save the transformed CSV
        df.to_csv(output_file, index=False)
        print(f"Transformed file saved as: {output_file}")

    except Exception as e:
        print(f"Error processing file {input_file}: {e}")


def process_files(file_list, input_folder, output_folder, processing_date):
    """
    Processes a list of files, transforming them based on predefined configurations.

    Parameters:
        file_list (list): List of file names to process.
        input_folder (str): Folder containing raw CSV files.
        output_folder (str): Folder to save transformed CSV files.
        processing_date (str): Processing date in YYYY-MM-DD format.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for file_name in file_list:
        if file_name in FILE_CONFIG:
            input_file = os.path.join(input_folder, file_name)
            output_file = os.path.join(output_folder, file_name.replace(".csv", "_cleaned.csv"))
            config = FILE_CONFIG[file_name]
            transform_csv(input_file, output_file, config, processing_date)
        else:
            print(f"No configuration defined for file: {file_name}. Skipping...")

################################## STAGE #######################################

# Google Cloud Storage bucket name
BUCKET_NAME = "toast-raw-data"  # Replace with your GCS bucket name

# Get the current date in YYYYMMDD format for dynamic folder paths
#date = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")  # Replace with dynamic calculation if needed
#date = "20241215"
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
            upload_to_gcs(local_file_path, bucket_name, destination_blob_name)
            
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
    except Exception as e:
        print(f"Error logging upload for {file_name}: {e}")


def send_notification(status, message):
    """
    Publishes a notification to a Pub/Sub topic.
    """
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("toast-analytics-444116", "etl_pipeline_notifications")

        # Publish the message
        message_data = f"{status}: {message}"
        publisher.publish(topic_path, message_data.encode("utf-8"))
        logging.info(f"Notification sent: {message_data}")
    except Exception as e:
        logging.error(f"Failed to send notification: {e}")

def main():
    """
    Main function to extract data from the SFTP server and stage it locally.
    """
    # Get the current date in YYYYMMDD format
    #date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    #date = "20241215"

    # Step 1: Download files from SFTP
    local_dir = download_from_sftp(date)
    if not local_dir:
        logging.error("SFTP download failed. Exiting.")
        return

    logging.info("SFTP extraction completed successfully.")

################## LOAD DATA ##################################################
def call_bash_script(date):
    """
    Calls the Bash script with the date set as an environment variable.

    Parameters:
        date (str): Date in YYYYMMDD format to set as an environment variable.
    """
    bash_script_path = "./load_toast_data.sh"  # Path to your Bash script

    try:
        print(f"Setting environment variable DATE={date}")
        os.environ["DATE"] = date  # Set the environment variable

        print(f"Executing {bash_script_path}...")
        result = subprocess.run(
            ["bash", bash_script_path],
            capture_output=True,
            text=True,
            check=True
        )
        print("Bash Script Output:")
        print(result.stdout)
        print("Bash Script Error (if any):")
        print(result.stderr)

    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing {bash_script_path}:")
        print(f"Return Code: {e.returncode}")
        print(f"Output: {e.stdout}")
        print(f"Error: {e.stderr}")

######################### MAIN LINE #############################################
if __name__ == "__main__":
    #STEP 1. Estract Data
    #main()
    """
    Main function to extract data from the SFTP server and stage it locally.
    """
    # Get the current date in YYYYMMDD format
    #date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    #date = "20241215"

    # Step 1: Download files from SFTP
    local_dir = download_from_sftp(date)
    if not local_dir:
        logging.error("SFTP download failed. Exiting.")
    else:
        logging.info("SFTP extraction completed successfully.")

    
    #Step 2. Process Files 
    processing_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")

    # Define input and output directories
    input_folder = f"/private/tmp/toast_raw_data/raw/{date}"
    output_folder = f"/private/tmp/toast_raw_data/cleaned/{date}"

    # List of files to process
    files_to_process = [
        "AllItemsReport.csv",
        "CheckDetails.csv",
        "CashEntries.csv",
        "ItemSelectionDetails.csv",
        "KitchenTimings.csv",
        "OrderDetails.csv",
        "PaymentDetails.csv"
    ]
    # Process files
    process_files(files_to_process, input_folder, output_folder, processing_date)
    
    # Step 3. LOAD TO CGS
    # Ensure the local folder exists
    if os.path.exists(LOCAL_FOLDER):
        # Upload files to GCS
        print(f"Uploading files from {LOCAL_FOLDER} to GCS...")
        upload_all_files(LOCAL_FOLDER, BUCKET_NAME, GCS_FOLDER)
    else:
        print(f"Error: Local folder not found: {LOCAL_FOLDER}")
        
    #Step 4. Load to BigQuery Tabble

    # Call the bash script with the date
    call_bash_script(date)
