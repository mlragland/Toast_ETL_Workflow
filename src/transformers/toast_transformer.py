"""
Toast Data Transformer

Comprehensive data transformation module for Toast POS CSV exports.
Handles column name sanitization, data type conversions, and special processing.
"""

import os
import re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class ToastDataTransformer:
    """
    Comprehensive transformer for Toast POS CSV data.
    
    Handles:
    - Column name sanitization (removes parentheses, slashes, special chars)
    - Data type conversions (dates, times, datetimes, floats, booleans)
    - Special processing (kitchen timing conversions)
    - Missing value handling
    - BigQuery compatibility
    """
    
    # File configurations with original column mappings and processing rules
    FILE_CONFIGS = {
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
            "string_columns": ["master_id", "item_id", "parent_id"],
            "date_columns": ["processing_date"]
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
            "date_columns": ["processing_date", "opened_date"],
            "time_columns": ["opened_time"]
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
            "date_columns": ["processing_date"],
            "datetime_columns": ["created_date"]
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
            "date_columns": ["processing_date"],
            "datetime_columns": ["sent_date", "order_date"],
            "boolean_columns": ["void", "deferred", "tax_exempt"]
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
            "date_columns": ["processing_date"],
            "datetime_columns": ["check_opened", "fired_date", "fulfilled_date"],
            "special_processing": {"fulfillment_time": "convert_to_minutes"}
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
            "date_columns": ["processing_date"],
            "datetime_columns": ["opened", "paid", "closed"],
            "boolean_columns": ["voided"],
            "special_processing": {"duration_opened_to_paid": "convert_to_minutes"}
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
            "date_columns": ["processing_date"],
            "datetime_columns": ["paid_date", "refund_date", "order_date", "void_date"]
        }
    }
    
    def __init__(self, processing_date: Optional[str] = None):
        """
        Initialize transformer with processing date.
        
        Args:
            processing_date: Date in YYYY-MM-DD format. Defaults to yesterday.
        """
        if processing_date is None:
            yesterday = datetime.now().date()
            self.processing_date = yesterday.strftime("%Y-%m-%d")
        else:
            self.processing_date = processing_date
            
        logger.info(f"Initialized ToastDataTransformer for date: {self.processing_date}")
    
    def sanitize_column_name(self, column_name: str) -> str:
        """
        Sanitize column names for BigQuery compatibility.
        
        The sanitization should match the existing mappings in FILE_CONFIGS.
        Since we already have predefined mappings, this method is mainly
        for validation purposes.
        
        Args:
            column_name: Original column name
            
        Returns:
            Sanitized column name
        """
        # For now, we rely on the predefined mappings in FILE_CONFIGS
        # This method is primarily for testing the concept
        sanitized = column_name.lower()
        
        # Replace spaces with underscores
        sanitized = sanitized.replace(' ', '_')
        
        # Remove parentheses but keep content
        sanitized = re.sub(r'\(([^)]*)\)', r'_\1', sanitized)
        
        # Replace slashes with underscores
        sanitized = sanitized.replace('/', '_')
        
        # Remove other special characters except underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '', sanitized)
        
        # Remove multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        return sanitized
    
    def convert_to_minutes(self, time_str: str) -> str:
        """
        Convert time string to total minutes.
        
        Parses formats like:
        - "02:25:02" (HH:MM:SS) → "145.0"
        - "2 hours, 15 minutes, 30 seconds" → "135.5"
        - "45 minutes" → "45.0"
        - "1 hour, 30 seconds" → "60.5"
        
        Args:
            time_str: Time string to convert
            
        Returns:
            Total minutes as string with 1 decimal place
        """
        if pd.isna(time_str) or time_str == "":
            return "0.0"
            
        try:
            time_str = str(time_str).strip()
            
            # Check if it's in HH:MM:SS format
            if re.match(r'^\d{1,2}:\d{2}:\d{2}$', time_str):
                parts = time_str.split(':')
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2])
                
                total_minutes = hours * 60 + minutes + seconds / 60
                return f"{total_minutes:.1f}"
            
            # Otherwise, try the legacy text format
            time_str_lower = time_str.lower()
            
            # Extract hours, minutes, seconds using regex
            hour_match = re.search(r'(\d+)\s*hour', time_str_lower)
            minute_match = re.search(r'(\d+)\s*minute', time_str_lower)
            second_match = re.search(r'(\d+)\s*second', time_str_lower)
            
            hours = int(hour_match.group(1)) if hour_match else 0
            minutes = int(minute_match.group(1)) if minute_match else 0
            seconds = int(second_match.group(1)) if second_match else 0
            
            total_minutes = hours * 60 + minutes + seconds / 60
            return f"{total_minutes:.1f}"
            
        except Exception as e:
            logger.warning(f"Error converting time '{time_str}': {e}")
            return "0.0"
    
    def transform_csv(self, input_file: str, output_file: str) -> bool:
        """
        Transform a CSV file according to Toast requirements.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output transformed CSV file
            
        Returns:
            True if transformation successful, False otherwise
        """
        file_name = os.path.basename(input_file)
        
        if file_name not in self.FILE_CONFIGS:
            logger.error(f"No configuration found for file: {file_name}")
            return False
            
        config = self.FILE_CONFIGS[file_name]
        
        try:
            logger.info(f"Transforming file: {file_name}")
            
            # Read CSV with special handling for AllItemsReport
            if file_name == "AllItemsReport.csv":
                df = pd.read_csv(input_file, dtype={
                    "Master ID": str, 
                    "Item ID": str, 
                    "Parent ID": str
                })
            else:
                df = pd.read_csv(input_file)
            
            logger.info(f"Loaded {len(df)} rows from {file_name}")
            
            # Step 1: Rename columns using mapping
            df.rename(columns=config["column_mapping"], inplace=True)
            logger.info(f"Renamed columns for {file_name}")
            
            # Step 2: Add processing date
            df["processing_date"] = self.processing_date
            
            # Step 3: Handle string columns (prevent scientific notation)
            if "string_columns" in config:
                for col in config["string_columns"]:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else "")
                        logger.debug(f"Converted {col} to string type")
            
            # Step 4: Format date columns
            if "date_columns" in config:
                for col in config["date_columns"]:
                    if col in df.columns and col != "processing_date":
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
                        logger.debug(f"Formatted date column: {col}")
            
            # Step 5: Format datetime columns
            if "datetime_columns" in config:
                for col in config["datetime_columns"]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
                        logger.debug(f"Formatted datetime column: {col}")
            
            # Step 6: Format time columns
            if "time_columns" in config:
                for col in config["time_columns"]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%H:%M:%S")
                        logger.debug(f"Formatted time column: {col}")
            
            # Step 7: Handle boolean columns
            if "boolean_columns" in config:
                for col in config["boolean_columns"]:
                    if col in df.columns:
                        df[col] = df[col].astype(bool, errors="ignore")
                        logger.debug(f"Converted {col} to boolean type")
            
            # Step 8: Special processing
            if "special_processing" in config:
                for col, method in config["special_processing"].items():
                    if col in df.columns and method == "convert_to_minutes":
                        df[col] = df[col].apply(self.convert_to_minutes)
                        logger.info(f"Applied special processing to {col}: {method}")
            
            # Step 9: Handle missing values
            df.fillna("", inplace=True)
            logger.debug("Filled missing values with empty strings")
            
            # Step 10: Save transformed file
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            df.to_csv(output_file, index=False)
            
            logger.info(f"Successfully transformed {file_name}: {len(df)} rows → {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error transforming {file_name}: {e}")
            return False
    
    def transform_files(self, input_folder: str, output_folder: str, file_list: Optional[List[str]] = None, 
                       enable_validation: bool = False) -> Tuple[Dict[str, bool], Dict[str, Any]]:
        """
        Transform multiple CSV files with optional advanced validation.
        
        Args:
            input_folder: Folder containing input CSV files
            output_folder: Folder to save transformed files
            file_list: List of files to process. If None, processes all configured files.
            enable_validation: Whether to perform advanced data quality validation
            
        Returns:
            Tuple of (success results, validation reports)
        """
        if file_list is None:
            file_list = list(self.FILE_CONFIGS.keys())
        
        results = {}
        validation_reports = {}
        
        logger.info(f"Starting transformation of {len(file_list)} files")
        logger.info(f"Input folder: {input_folder}")
        logger.info(f"Output folder: {output_folder}")
        logger.info(f"Advanced validation: {'enabled' if enable_validation else 'disabled'}")
        
        # Initialize validation if requested
        quality_checker = None
        if enable_validation:
            try:
                from ..validators.quality_checker import QualityChecker
                quality_checker = QualityChecker()
                logger.info("Quality checker initialized")
            except ImportError as e:
                logger.warning(f"Advanced validation not available: {e}")
                enable_validation = False
        
        for file_name in file_list:
            input_file = os.path.join(input_folder, file_name)
            output_file = os.path.join(output_folder, file_name.replace(".csv", "_cleaned.csv"))
            
            if not os.path.exists(input_file):
                logger.warning(f"Input file not found: {input_file}")
                results[file_name] = False
                continue
            
            # Perform advanced validation if enabled
            if enable_validation and quality_checker:
                try:
                    logger.info(f"Performing quality validation for {file_name}")
                    df = pd.read_csv(input_file)
                    
                    # Validate and potentially correct data
                    corrected_df, validation_report = quality_checker.validate_and_enforce(df, file_name)
                    validation_reports[file_name] = validation_report
                    
                    # Log validation results
                    severity = validation_report.get("severity", "UNKNOWN")
                    if severity == "CRITICAL":
                        logger.warning(f"Critical quality issues found in {file_name}")
                    elif severity == "WARNING":
                        logger.info(f"Quality warnings found in {file_name}")
                    else:
                        logger.info(f"Quality validation passed for {file_name}")
                    
                    # Save corrected data if corrections were applied
                    if validation_report.get("schema_corrections", {}).get("applied", False):
                        corrected_df.to_csv(input_file, index=False)
                        logger.info(f"Applied schema corrections to {file_name}")
                        
                except Exception as e:
                    logger.error(f"Quality validation failed for {file_name}: {e}")
                    validation_reports[file_name] = {"error": str(e), "severity": "ERROR"}
            
            # Perform transformation
            results[file_name] = self.transform_csv(input_file, output_file)
        
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Transformation complete: {successful}/{len(file_list)} files successful")
        
        return results, validation_reports
    
    def validate_transformed_data(self, file_path: str) -> Dict[str, Any]:
        """
        Validate transformed CSV data.
        
        Args:
            file_path: Path to transformed CSV file
            
        Returns:
            Validation results dictionary
        """
        try:
            df = pd.read_csv(file_path)
            
            validation = {
                "file_path": file_path,
                "row_count": len(df),
                "column_count": len(df.columns),
                "has_processing_date": "processing_date" in df.columns,
                "processing_date_value": df["processing_date"].iloc[0] if "processing_date" in df.columns and len(df) > 0 else None,
                "null_counts": df.isnull().sum().to_dict(),
                "column_names": list(df.columns),
                "bigquery_compatible": True
            }
            
            # Check for BigQuery-incompatible column names
            problematic_columns = []
            for col in df.columns:
                if any(char in col for char in "()/ "):
                    problematic_columns.append(col)
            
            if problematic_columns:
                validation["bigquery_compatible"] = False
                validation["problematic_columns"] = problematic_columns
            else:
                validation["bigquery_compatible"] = True
            
            return validation
            
        except Exception as e:
            return {
                "file_path": file_path,
                "error": str(e),
                "bigquery_compatible": False
            }