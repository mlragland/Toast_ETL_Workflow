"""
Schema Enforcer for Toast ETL Pipeline.

Validates data against BigQuery table schemas, enforces data type constraints,
and ensures schema compliance before loading to BigQuery.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SchemaEnforcer:
    """
    Enforces BigQuery schema compliance for Toast CSV data.
    
    Validates data types, checks constraints, and ensures data compatibility
    with BigQuery table schemas before loading.
    """
    
    # BigQuery schema definitions based on legacy JSON files
    BIGQUERY_SCHEMAS = {
        "AllItemsReport.csv": [
            {"name": "master_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "item_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "parent_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "menu_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "menu_group", "type": "STRING", "mode": "NULLABLE"},
            {"name": "subgroup", "type": "STRING", "mode": "NULLABLE"},
            {"name": "menu_item", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tags", "type": "STRING", "mode": "NULLABLE"},
            {"name": "avg_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "item_qty_incl_voids", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "percent_ttl_qty_incl_voids", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gross_amount_incl_voids", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "percent_ttl_amt_incl_voids", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "item_qty", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gross_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "void_qty", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "void_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "discount_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "net_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "num_orders", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "percent_ttl_num_orders", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "percent_qty_group", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "percent_qty_menu", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "percent_qty_all", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "percent_net_amt_group", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "percent_net_amt_menu", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "percent_net_amt_all", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"}
        ],
        "CheckDetails.csv": [
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "opened_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "opened_time", "type": "TIME", "mode": "NULLABLE"},
            {"name": "item_description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "server", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tax", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "check_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "check_number", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "total", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "customer_family", "type": "STRING", "mode": "NULLABLE"},
            {"name": "table_size", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "discount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "reason_of_discount", "type": "STRING", "mode": "NULLABLE"},
            {"name": "link", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"}
        ],
        "CashEntries.csv": [
            {"name": "location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "entry_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "created_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "action", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "cash_drawer", "type": "STRING", "mode": "NULLABLE"},
            {"name": "payout_reason", "type": "STRING", "mode": "NULLABLE"},
            {"name": "no_sale_reason", "type": "STRING", "mode": "NULLABLE"},
            {"name": "comment", "type": "STRING", "mode": "NULLABLE"},
            {"name": "employee", "type": "STRING", "mode": "NULLABLE"},
            {"name": "employee_2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"}
        ],
        "ItemSelectionDetails.csv": [
            {"name": "location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_number", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sent_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "check_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "server", "type": "STRING", "mode": "NULLABLE"},
            {"name": "table", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dining_area", "type": "STRING", "mode": "NULLABLE"},
            {"name": "service", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dining_option", "type": "STRING", "mode": "NULLABLE"},
            {"name": "item_selection_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "item_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "master_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sku", "type": "STRING", "mode": "NULLABLE"},
            {"name": "plu", "type": "STRING", "mode": "NULLABLE"},
            {"name": "menu_item", "type": "STRING", "mode": "NULLABLE"},
            {"name": "menu_subgroup", "type": "STRING", "mode": "NULLABLE"},
            {"name": "menu_group", "type": "STRING", "mode": "NULLABLE"},
            {"name": "menu", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sales_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gross_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "discount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "net_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "quantity", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tax", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "void", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "deferred", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "tax_exempt", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "tax_inclusion_option", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dining_option_tax", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tab_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"}
        ],
        "KitchenTimings.csv": [
            {"name": "location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "server", "type": "STRING", "mode": "NULLABLE"},
            {"name": "check_number", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "table", "type": "STRING", "mode": "NULLABLE"},
            {"name": "check_opened", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "station", "type": "STRING", "mode": "NULLABLE"},
            {"name": "expediter_level", "type": "STRING", "mode": "NULLABLE"},
            {"name": "fired_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "fulfilled_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "fulfillment_time", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "fulfilled_by", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"}
        ],
        "OrderDetails.csv": [
            {"name": "location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "checks", "type": "STRING", "mode": "NULLABLE"},
            {"name": "opened", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "guest_count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "tab_names", "type": "STRING", "mode": "NULLABLE"},
            {"name": "server", "type": "STRING", "mode": "NULLABLE"},
            {"name": "table", "type": "STRING", "mode": "NULLABLE"},
            {"name": "revenue_center", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dining_area", "type": "STRING", "mode": "NULLABLE"},
            {"name": "service", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dining_options", "type": "STRING", "mode": "NULLABLE"},
            {"name": "discount_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tax", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tip", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gratuity", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "total", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "voided", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "paid", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "closed", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "duration_opened_to_paid", "type": "TIME", "mode": "NULLABLE"},
            {"name": "order_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"}
        ],
        "PaymentDetails.csv": [
            {"name": "location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "payment_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "paid_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "check_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "check_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tab_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "server", "type": "STRING", "mode": "NULLABLE"},
            {"name": "table", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dining_area", "type": "STRING", "mode": "NULLABLE"},
            {"name": "service", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dining_option", "type": "STRING", "mode": "NULLABLE"},
            {"name": "house_account_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tip", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gratuity", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "total", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "swiped_card_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "keyed_card_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "amount_tendered", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "refunded", "type": "STRING", "mode": "NULLABLE"},
            {"name": "refund_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "refund_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "refund_tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "void_user", "type": "STRING", "mode": "NULLABLE"},
            {"name": "void_approver", "type": "STRING", "mode": "NULLABLE"},
            {"name": "void_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cash_drawer", "type": "STRING", "mode": "NULLABLE"},
            {"name": "card_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "other_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_4_card_digits", "type": "STRING", "mode": "NULLABLE"},
            {"name": "vmcd_fees", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "room_info", "type": "STRING", "mode": "NULLABLE"},
            {"name": "receipt", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_4_gift_card_digits", "type": "STRING", "mode": "NULLABLE"},
            {"name": "first_5_gift_card_digits", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_date", "type": "DATE", "mode": "NULLABLE"}
        ]
    }
    
    def __init__(self):
        """Initialize the Schema Enforcer."""
        self.validation_errors = []
        self.schema_warnings = []
    
    def get_schema_for_file(self, filename: str) -> Optional[List[Dict[str, str]]]:
        """
        Get BigQuery schema definition for a given file.
        
        Args:
            filename: Name of the CSV file
            
        Returns:
            Schema definition or None if not found
        """
        base_filename = Path(filename).name
        if base_filename.endswith("_cleaned.csv"):
            base_filename = base_filename.replace("_cleaned.csv", ".csv")
        
        return self.BIGQUERY_SCHEMAS.get(base_filename)
    
    def validate_schema_compliance(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Validate DataFrame compliance with BigQuery schema.
        
        Args:
            df: Pandas DataFrame to validate
            filename: Name of the source file
            
        Returns:
            Validation result dictionary
        """
        schema = self.get_schema_for_file(filename)
        if not schema:
            return {
                "valid": False,
                "error": f"No schema found for file: {filename}",
                "missing_columns": [],
                "type_mismatches": [],
                "recommendations": []
            }
        
        validation_result = {
            "valid": True,
            "missing_columns": [],
            "extra_columns": [],
            "type_mismatches": [],
            "null_violations": [],
            "recommendations": [],
            "row_count": len(df),
            "column_count": len(df.columns)
        }
        
        # Create schema lookup
        schema_dict = {field["name"]: field for field in schema}
        required_columns = set(schema_dict.keys())
        actual_columns = set(df.columns)
        
        # Check for missing columns
        missing_columns = required_columns - actual_columns
        if missing_columns:
            validation_result["missing_columns"] = list(missing_columns)
            validation_result["valid"] = False
        
        # Check for extra columns
        extra_columns = actual_columns - required_columns
        if extra_columns:
            validation_result["extra_columns"] = list(extra_columns)
            validation_result["recommendations"].append(
                f"Extra columns found: {extra_columns}. Consider removing or updating schema."
            )
        
        # Validate data types for existing columns
        for col in actual_columns.intersection(required_columns):
            schema_field = schema_dict[col]
            expected_type = schema_field["type"]
            mode = schema_field.get("mode", "NULLABLE")
            
            # Check data type compatibility
            type_valid = self._validate_column_type(df[col], expected_type, col)
            if not type_valid:
                validation_result["type_mismatches"].append({
                    "column": col,
                    "expected_type": expected_type,
                    "current_type": str(df[col].dtype),
                    "sample_values": df[col].dropna().head(3).tolist()
                })
                validation_result["valid"] = False
            
            # Check for null violations in required fields
            if mode == "REQUIRED" and df[col].isnull().any():
                null_count = df[col].isnull().sum()
                validation_result["null_violations"].append({
                    "column": col,
                    "null_count": null_count,
                    "total_rows": len(df)
                })
                validation_result["valid"] = False
        
        return validation_result
    
    def _validate_column_type(self, series: pd.Series, expected_type: str, column_name: str) -> bool:
        """
        Validate if a pandas Series matches the expected BigQuery type.
        
        Args:
            series: Pandas Series to validate
            expected_type: Expected BigQuery data type
            column_name: Name of the column for logging
            
        Returns:
            True if type is compatible, False otherwise
        """
        # Drop null values for type checking
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return True  # All nulls are acceptable for nullable fields
        
        try:
            if expected_type == "STRING":
                # Strings are flexible - most things can be converted
                return True
                
            elif expected_type == "INTEGER":
                # Check if values can be converted to integers
                if pd.api.types.is_integer_dtype(non_null_series):
                    return True
                # Try to convert floats to integers if they're whole numbers
                if pd.api.types.is_float_dtype(non_null_series):
                    return non_null_series.apply(lambda x: float(x).is_integer()).all()
                return False
                
            elif expected_type == "FLOAT":
                # Check if values are numeric
                return pd.api.types.is_numeric_dtype(non_null_series)
                
            elif expected_type == "BOOLEAN":
                # Check if values are boolean or can be converted
                if pd.api.types.is_bool_dtype(non_null_series):
                    return True
                # Check for common boolean representations
                unique_values = set(non_null_series.astype(str).str.lower().unique())
                valid_booleans = {'true', 'false', '1', '0', 'yes', 'no', 't', 'f'}
                return unique_values.issubset(valid_booleans)
                
            elif expected_type == "DATE":
                # Try to parse as date
                try:
                    pd.to_datetime(non_null_series, errors="raise")
                    return True
                except:
                    return False
                    
            elif expected_type == "DATETIME":
                # Try to parse as datetime
                try:
                    pd.to_datetime(non_null_series, errors="raise")
                    return True
                except:
                    return False
                    
            elif expected_type == "TIME":
                # Check for time format
                try:
                    # Basic time validation - look for HH:MM:SS pattern
                    import re
                    time_pattern = r'^\d{1,2}:\d{2}:\d{2}$'
                    return non_null_series.astype(str).str.match(time_pattern).all()
                except:
                    return False
                    
            else:
                logger.warning(f"Unknown BigQuery type: {expected_type} for column {column_name}")
                return True  # Be permissive for unknown types
                
        except Exception as e:
            logger.error(f"Error validating column {column_name} type {expected_type}: {e}")
            return False
    
    def enforce_schema_types(self, df: pd.DataFrame, filename: str) -> Tuple[pd.DataFrame, List[str]]:
        """
        Enforce BigQuery schema types on DataFrame.
        
        Args:
            df: Input DataFrame
            filename: Source filename for schema lookup
            
        Returns:
            Tuple of (corrected DataFrame, list of conversion warnings)
        """
        schema = self.get_schema_for_file(filename)
        if not schema:
            return df, [f"No schema found for {filename}"]
        
        warnings = []
        df_corrected = df.copy()
        
        schema_dict = {field["name"]: field for field in schema}
        
        for col in df_corrected.columns:
            if col in schema_dict:
                expected_type = schema_dict[col]["type"]
                
                try:
                    if expected_type == "INTEGER":
                        # Convert to integer, handling NaN appropriately
                        df_corrected[col] = pd.to_numeric(df_corrected[col], errors="coerce").astype("Int64")
                        
                    elif expected_type == "FLOAT":
                        # Convert to float
                        df_corrected[col] = pd.to_numeric(df_corrected[col], errors="coerce")
                        
                    elif expected_type == "BOOLEAN":
                        # Convert to boolean
                        df_corrected[col] = self._convert_to_boolean(df_corrected[col])
                        
                    elif expected_type == "DATE":
                        # Convert to date string format
                        df_corrected[col] = pd.to_datetime(df_corrected[col], errors="coerce").dt.strftime("%Y-%m-%d")
                        
                    elif expected_type == "DATETIME":
                        # Convert to datetime string format
                        df_corrected[col] = pd.to_datetime(df_corrected[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
                        
                    elif expected_type == "TIME":
                        # Ensure time format
                        df_corrected[col] = pd.to_datetime(df_corrected[col], errors="coerce").dt.strftime("%H:%M:%S")
                        
                    elif expected_type == "STRING":
                        # Convert to string, handle NaN
                        df_corrected[col] = df_corrected[col].astype(str).replace('nan', '')
                        
                except Exception as e:
                    warnings.append(f"Could not convert column {col} to {expected_type}: {e}")
        
        return df_corrected, warnings
    
    def _convert_to_boolean(self, series: pd.Series) -> pd.Series:
        """Convert series to boolean values."""
        # Map common boolean representations
        boolean_map = {
            'true': True, 'false': False,
            '1': True, '0': False,
            'yes': True, 'no': False,
            't': True, 'f': False,
            'y': True, 'n': False
        }
        
        # Convert to string and lowercase for mapping
        str_series = series.astype(str).str.lower()
        
        # Apply mapping
        result = str_series.map(boolean_map)
        
        # Handle cases where mapping didn't work
        result = result.fillna(series.astype(bool) if pd.api.types.is_bool_dtype(series) else False)
        
        return result
    
    def generate_schema_report(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Generate comprehensive schema validation report.
        
        Args:
            df: DataFrame to analyze
            filename: Source filename
            
        Returns:
            Detailed schema report
        """
        validation_result = self.validate_schema_compliance(df, filename)
        
        # Add additional statistics
        report = {
            **validation_result,
            "filename": filename,
            "validation_timestamp": datetime.now().isoformat(),
            "summary": {
                "total_issues": (
                    len(validation_result["missing_columns"]) +
                    len(validation_result["type_mismatches"]) +
                    len(validation_result["null_violations"])
                ),
                "critical_issues": len(validation_result["missing_columns"]) + len(validation_result["null_violations"]),
                "warnings": len(validation_result["extra_columns"]) + len(validation_result["type_mismatches"])
            }
        }
        
        # Add severity assessment
        if report["summary"]["critical_issues"] > 0:
            report["severity"] = "CRITICAL"
        elif report["summary"]["warnings"] > 0:
            report["severity"] = "WARNING"
        else:
            report["severity"] = "PASS"
        
        return report