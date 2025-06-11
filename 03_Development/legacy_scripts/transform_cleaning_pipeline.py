import os
import re
import pandas as pd
from datetime import datetime, timedelta
import logging

# Initialize logging
LOG_FILE = "/private/tmp/toast_raw_data/logs/etl_pipeline.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

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
            "% Net Amt (All)": "percent_net_amt_all",
        },
        "date_columns": ["processing_date"],
    },
    # Define configurations for other CSVs...
}

def convert_to_minutes(time_str):
    """
    Converts a time string like 'X hours, Y minutes, Z seconds' into total minutes.
    """
    try:
        time_str = time_str.lower()
        hour_pattern = r"(\d+)\s*hour"
        minute_pattern = r"(\d+)\s*minute"
        second_pattern = r"(\d+)\s*second"

        hours = re.findall(hour_pattern, time_str)
        minutes = re.findall(minute_pattern, time_str)
        seconds = re.findall(second_pattern, time_str)

        hours = int(hours[0]) if hours else 0
        minutes = int(minutes[0]) if minutes else 0
        seconds = int(seconds[0]) if seconds else 0

        total_minutes = hours * 60 + minutes + seconds / 60
        return f"{total_minutes:.1f}"
    except Exception as e:
        logging.error(f"Error converting time: '{time_str}', Error: {e}")
        return "0.0"

def transform_csv(input_file, output_file, config, processing_date):
    """
    Transforms a CSV file by renaming columns, adding a processing date, formatting date/time columns.
    """
    try:
        logging.info(f"Processing file: {input_file}")
        df = pd.read_csv(input_file)

        # Rename columns
        df.rename(columns=config["column_mapping"], inplace=True)

        # Format date columns
        if "date_columns" in config:
            for col in config["date_columns"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

        # Format datetime columns
        if "datetime_columns" in config:
            for col in config["datetime_columns"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

        # Format time columns
        if "time_columns" in config:
            for col in config["time_columns"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%H:%M:%S")

        # Special handling for KitchenTimings.csv
        if os.path.basename(input_file) == "KitchenTimings.csv" and "fulfillment_time" in df.columns:
            df["fulfillment_time"] = df["fulfillment_time"].apply(convert_to_minutes)

        # Add processing_date column
        df["processing_date"] = processing_date

        # Handle missing values
        df.fillna("", inplace=True)

        # Log number of records transformed
        logging.info(f"Records processed: {len(df)}")

        # Save the transformed CSV
        df.to_csv(output_file, index=False)
        logging.info(f"Transformed file saved: {output_file}")
    except Exception as e:
        logging.error(f"Error processing file {input_file}: {e}")

def process_files(file_list, input_folder, output_folder, processing_date):
    """
    Processes a list of files, transforming them based on predefined configurations.
    """
    os.makedirs(output_folder, exist_ok=True)

    for file_name in file_list:
        if file_name in FILE_CONFIG:
            input_file = os.path.join(input_folder, file_name)
            output_file = os.path.join(output_folder, file_name.replace(".csv", "_cleaned.csv"))
            transform_csv(input_file, output_file, FILE_CONFIG[file_name], processing_date)
        else:
            logging.warning(f"No configuration defined for file: {file_name}. Skipping.")

if __name__ == "__main__":
    date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    processing_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
    input_folder = f"/private/tmp/toast_raw_data/raw/{date}"
    output_folder = f"/private/tmp/toast_raw_data/cleaned/{date}"
    files_to_process = list(FILE_CONFIG.keys())

    try:
        process_files(files_to_process, input_folder, output_folder, processing_date)
        logging.info("Data transformation completed successfully.")
    except Exception as e:
        logging.error(f"Data transformation failed: {e}")
