import os
import re
import pandas as pd
from datetime import datetime, timedelta

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

if __name__ == "__main__":
    # Define the processing date
    #date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    date = "20241214"
    
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