import os
import pandas as pd
from datetime import datetime

# Define column mappings for each CSV file
COLUMN_MAPPINGS = {
    "AllItemsReport.csv": {
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
    "CheckDetails.csv": {
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
    "CashEntries.csv": {
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
    "ItemSelectionDetails.csv": {
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
    "KitchenTimings.csv": {
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
    "OrderDetails.csv": {
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
    "PaymentDetails.csv": {
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
    }
}

def transform_csv(input_file, output_file, column_mapping, date):
    """
    Transforms a CSV file by renaming its columns, adding a processing date, and handling missing values.

    Parameters:
        input_file (str): Path to the raw input CSV file.
        output_file (str): Path to save the transformed CSV file.
        column_mapping (dict): Dictionary mapping raw column names to cleaned column names.
        date (str): Date string to add as a column (YYYY-MM-DD).
    """
    try:
        print(f"Processing file: {input_file}")
        df = pd.read_csv(input_file)

        # Rename columns based on the mapping
        print("Renaming columns...")
        df.rename(columns=column_mapping, inplace=True)

        # Format the processing_date column if it exists
        if 'processing_date' in df.columns:
            print("Formatting 'processing_date' to 'YYYY-MM-DD'...")
            df['processing_date'] = pd.to_datetime(df['processing_date'], format='%Y%m%d').dt.dateetime(df['processing_date'], format='%Y%m%d').dt.date
        df['processing_date'] = date_formatted

        # Handle missing values
        print("Handling missing values...")
        df.fillna("", inplace=True)

        # Save the transformed CSV
        df.to_csv(output_file, index=False)
        print(f"Transformed file saved as: {output_file}")
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

def process_files(file_list, input_folder, output_folder, date):
    """
    Processes a list of files, transforming them based on predefined mappings.

    Parameters:
        file_list (list): List of file names to process.
        input_folder (str): Folder containing raw CSV files.
        output_folder (str): Folder to save transformed CSV files.
        date (str): Date string for processing (YYYYMMDD).
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for file_name in file_list:
        if file_name in COLUMN_MAPPINGS:
            input_file = os.path.join(input_folder, file_name)
            output_file = os.path.join(output_folder, file_name.replace(".csv", "_cleaned.csv"))
            column_mapping = COLUMN_MAPPINGS[file_name]
            transform_csv(input_file, output_file, column_mapping, date)
        else:
            print(f"No column mapping defined for file: {file_name}. Skipping...")

if __name__ == "__main__":
    # Get the current date in YYYYMMDD format
    #date = datetime.now().strftime("%Y-%m-%d")
    date = "20241206"
    parsed_date = datetime.strptime(date, "%Y%m%d")
    date_formatted = parsed_date.strftime("%Y-%m-%d")

    # Define paths
    input_folder = f"/private/tmp/toast_raw_data/raw/{date}"  # Raw data folder
    output_folder = f"/private/tmp/toast_raw_data/cleaned/{date}"  # Cleaned data folder

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
    process_files(files_to_process, input_folder, output_folder, date)
