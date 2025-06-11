"""
Data Validator for Toast ETL Pipeline.

Provides comprehensive data validation, business rule checks, and data quality
assessments for Toast POS data.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Union
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Comprehensive data validator for Toast POS CSV data.
    
    Provides business rule validation, data quality checks, referential integrity
    validation, and anomaly detection for the Toast ETL pipeline.
    """
    
    def __init__(self):
        """Initialize the Data Validator."""
        self.validation_errors = []
        self.validation_warnings = []
        self.business_rules = self._define_business_rules()
    
    def _define_business_rules(self) -> Dict[str, Dict[str, Any]]:
        """
        Define business rules for Toast POS data validation.
        
        Returns:
            Dictionary of business rules by file type
        """
        return {
            "AllItemsReport.csv": {
                "price_ranges": {
                    "avg_price": {"min": 0, "max": 1000, "warning_max": 100},
                    "net_amount": {"min": 0, "max": 50000, "warning_max": 10000}
                },
                "percentage_ranges": {
                    "percent_ttl_qty_incl_voids": {"min": 0, "max": 100},
                    "percent_ttl_amt_incl_voids": {"min": 0, "max": 100},
                    "percent_qty_group": {"min": 0, "max": 100},
                    "percent_net_amt_group": {"min": 0, "max": 100}
                },
                "quantity_ranges": {
                    "item_qty": {"min": 0, "max": 10000},
                    "void_qty": {"min": 0, "max": 1000}
                },
                "required_fields": ["master_id", "item_id", "menu_item"],
                "string_length_limits": {
                    "menu_item": 200,
                    "menu_name": 100,
                    "tags": 500
                }
            },
            "CheckDetails.csv": {
                "amount_ranges": {
                    "total": {"min": 0, "max": 5000, "warning_max": 1000},
                    "tax": {"min": 0, "max": 500},
                    "discount": {"min": 0, "max": 1000}
                },
                "required_fields": ["check_id", "opened_date"],
                "email_validation": ["customer_email"],
                "phone_validation": ["customer_phone"],
                "string_length_limits": {
                    "customer": 100,
                    "server": 50,
                    "item_description": 300
                }
            },
            "CashEntries.csv": {
                "amount_ranges": {
                    "amount": {"min": -5000, "max": 5000}  # Can be negative for payouts
                },
                "required_fields": ["entry_id", "action"],
                "valid_actions": ["Payout", "No Sale", "Cash Drop", "Cash In", "Manager Payout"],
                "string_length_limits": {
                    "comment": 500,
                    "payout_reason": 200
                }
            },
            "ItemSelectionDetails.csv": {
                "price_ranges": {
                    "gross_price": {"min": 0, "max": 1000},
                    "net_price": {"min": 0, "max": 1000},
                    "quantity": {"min": 0, "max": 100}
                },
                "required_fields": ["order_id", "item_selection_id", "menu_item"],
                "boolean_fields": ["void", "deferred", "tax_exempt"],
                "string_length_limits": {
                    "menu_item": 200,
                    "sku": 50,
                    "plu": 50
                }
            },
            "KitchenTimings.csv": {
                "timing_ranges": {
                    "fulfillment_time": {"min": 0, "max": 300}  # 0 to 5 hours in minutes
                },
                "required_fields": ["id", "check_number", "station"],
                "datetime_sequence_validation": {
                    "check_opened": "fired_date",
                    "fired_date": "fulfilled_date"
                },
                "string_length_limits": {
                    "station": 50,
                    "server": 50
                }
            },
            "OrderDetails.csv": {
                "amount_ranges": {
                    "amount": {"min": 0, "max": 5000},
                    "total": {"min": 0, "max": 5000},
                    "tax": {"min": 0, "max": 500},
                    "tip": {"min": 0, "max": 1000},
                    "gratuity": {"min": 0, "max": 1000}
                },
                "guest_count_range": {"min": 1, "max": 50},
                "required_fields": ["order_id", "location"],
                "boolean_fields": ["voided"],
                "datetime_sequence_validation": {
                    "opened": "paid",
                    "paid": "closed"
                },
                "string_length_limits": {
                    "server": 50,
                    "order_source": 100
                }
            },
            "PaymentDetails.csv": {
                "amount_ranges": {
                    "amount": {"min": 0, "max": 5000},
                    "total": {"min": 0, "max": 5000},
                    "tip": {"min": 0, "max": 1000},
                    "swiped_card_amount": {"min": 0, "max": 5000},
                    "keyed_card_amount": {"min": 0, "max": 5000},
                    "vmcd_fees": {"min": 0, "max": 100}
                },
                "required_fields": ["payment_id", "order_id"],
                "valid_statuses": ["Completed", "Voided", "Refunded", "Pending"],
                "card_digit_validation": {
                    "last_4_card_digits": 4,
                    "last_4_gift_card_digits": 4,
                    "first_5_gift_card_digits": 5
                },
                "string_length_limits": {
                    "card_type": 50,
                    "email": 200,
                    "phone": 20
                }
            }
        }
    
    def validate_business_rules(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Validate data against business rules.
        
        Args:
            df: DataFrame to validate
            filename: Source filename
            
        Returns:
            Validation results with errors and warnings
        """
        base_filename = Path(filename).name
        if base_filename.endswith("_cleaned.csv"):
            base_filename = base_filename.replace("_cleaned.csv", ".csv")
        
        rules = self.business_rules.get(base_filename, {})
        if not rules:
            return {
                "valid": True,
                "errors": [],
                "warnings": [],
                "info": f"No business rules defined for {filename}"
            }
        
        errors = []
        warnings = []
        
        # Validate required fields
        if "required_fields" in rules:
            missing_required = self._validate_required_fields(df, rules["required_fields"])
            errors.extend(missing_required)
        
        # Validate numeric ranges
        if "price_ranges" in rules:
            range_errors, range_warnings = self._validate_ranges(df, rules["price_ranges"], "price")
            errors.extend(range_errors)
            warnings.extend(range_warnings)
        
        if "amount_ranges" in rules:
            range_errors, range_warnings = self._validate_ranges(df, rules["amount_ranges"], "amount")
            errors.extend(range_errors)
            warnings.extend(range_warnings)
        
        if "quantity_ranges" in rules:
            range_errors, range_warnings = self._validate_ranges(df, rules["quantity_ranges"], "quantity")
            errors.extend(range_errors)
            warnings.extend(range_warnings)
        
        if "timing_ranges" in rules:
            range_errors, range_warnings = self._validate_ranges(df, rules["timing_ranges"], "timing")
            errors.extend(range_errors)
            warnings.extend(range_warnings)
        
        # Validate percentage ranges
        if "percentage_ranges" in rules:
            percentage_errors = self._validate_percentages(df, rules["percentage_ranges"])
            errors.extend(percentage_errors)
        
        # Validate email addresses
        if "email_validation" in rules:
            email_errors = self._validate_emails(df, rules["email_validation"])
            errors.extend(email_errors)
        
        # Validate phone numbers
        if "phone_validation" in rules:
            phone_errors = self._validate_phones(df, rules["phone_validation"])
            errors.extend(phone_errors)
        
        # Validate boolean fields
        if "boolean_fields" in rules:
            boolean_errors = self._validate_booleans(df, rules["boolean_fields"])
            errors.extend(boolean_errors)
        
        # Validate datetime sequences
        if "datetime_sequence_validation" in rules:
            sequence_errors = self._validate_datetime_sequences(df, rules["datetime_sequence_validation"])
            errors.extend(sequence_errors)
        
        # Validate string lengths
        if "string_length_limits" in rules:
            length_warnings = self._validate_string_lengths(df, rules["string_length_limits"])
            warnings.extend(length_warnings)
        
        # Validate categorical values
        if "valid_actions" in rules:
            categorical_errors = self._validate_categorical(df, "action", rules["valid_actions"])
            errors.extend(categorical_errors)
        
        if "valid_statuses" in rules:
            categorical_errors = self._validate_categorical(df, "status", rules["valid_statuses"])
            errors.extend(categorical_errors)
        
        # Validate guest count
        if "guest_count_range" in rules and "guest_count" in df.columns:
            guest_errors = self._validate_guest_count(df, rules["guest_count_range"])
            errors.extend(guest_errors)
        
        # Validate card digits
        if "card_digit_validation" in rules:
            card_errors = self._validate_card_digits(df, rules["card_digit_validation"])
            errors.extend(card_errors)
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "rows_validated": len(df)
        }
    
    def _validate_required_fields(self, df: pd.DataFrame, required_fields: List[str]) -> List[str]:
        """Validate that required fields are present and not null."""
        errors = []
        for field in required_fields:
            if field not in df.columns:
                errors.append(f"Required field '{field}' is missing")
            elif df[field].isnull().any():
                null_count = df[field].isnull().sum()
                errors.append(f"Required field '{field}' has {null_count} null values")
        return errors
    
    def _validate_ranges(self, df: pd.DataFrame, ranges: Dict[str, Dict[str, float]], 
                        range_type: str) -> Tuple[List[str], List[str]]:
        """Validate numeric ranges."""
        errors = []
        warnings = []
        
        for column, limits in ranges.items():
            if column not in df.columns:
                continue
            
            # Convert to numeric, coercing errors to NaN
            numeric_series = pd.to_numeric(df[column], errors='coerce')
            
            # Check minimum values
            if "min" in limits:
                min_violations = numeric_series < limits["min"]
                if min_violations.any():
                    violation_count = min_violations.sum()
                    errors.append(f"{column}: {violation_count} values below minimum {limits['min']}")
            
            # Check maximum values (errors)
            if "max" in limits:
                max_violations = numeric_series > limits["max"]
                if max_violations.any():
                    violation_count = max_violations.sum()
                    errors.append(f"{column}: {violation_count} values above maximum {limits['max']}")
            
            # Check warning thresholds
            if "warning_max" in limits:
                warning_violations = numeric_series > limits["warning_max"]
                if warning_violations.any():
                    violation_count = warning_violations.sum()
                    warnings.append(f"{column}: {violation_count} values above warning threshold {limits['warning_max']}")
        
        return errors, warnings
    
    def _validate_percentages(self, df: pd.DataFrame, percentage_ranges: Dict[str, Dict[str, float]]) -> List[str]:
        """Validate percentage values are within 0-100 range."""
        errors = []
        for column, limits in percentage_ranges.items():
            if column not in df.columns:
                continue
            
            numeric_series = pd.to_numeric(df[column], errors='coerce')
            
            # Check for values outside 0-100 range
            invalid_percentages = (numeric_series < 0) | (numeric_series > 100)
            if invalid_percentages.any():
                violation_count = invalid_percentages.sum()
                errors.append(f"{column}: {violation_count} values outside valid percentage range (0-100)")
        
        return errors
    
    def _validate_emails(self, df: pd.DataFrame, email_columns: List[str]) -> List[str]:
        """Validate email address format."""
        errors = []
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        for column in email_columns:
            if column not in df.columns:
                continue
            
            # Filter out empty/null values
            non_empty_emails = df[column].dropna()
            non_empty_emails = non_empty_emails[non_empty_emails != '']
            
            if len(non_empty_emails) > 0:
                invalid_emails = ~non_empty_emails.str.match(email_pattern, na=False)
                if invalid_emails.any():
                    violation_count = invalid_emails.sum()
                    errors.append(f"{column}: {violation_count} invalid email formats")
        
        return errors
    
    def _validate_phones(self, df: pd.DataFrame, phone_columns: List[str]) -> List[str]:
        """Validate phone number format."""
        errors = []
        
        for column in phone_columns:
            if column not in df.columns:
                continue
            
            # Filter out empty/null values
            non_empty_phones = df[column].dropna()
            non_empty_phones = non_empty_phones[non_empty_phones != '']
            
            if len(non_empty_phones) > 0:
                # Basic phone validation - should contain only digits, spaces, hyphens, parentheses, +
                phone_pattern = r'^[\d\s\-\(\)\+\.]+$'
                invalid_phones = ~non_empty_phones.astype(str).str.match(phone_pattern, na=False)
                if invalid_phones.any():
                    violation_count = invalid_phones.sum()
                    errors.append(f"{column}: {violation_count} invalid phone number formats")
        
        return errors
    
    def _validate_booleans(self, df: pd.DataFrame, boolean_columns: List[str]) -> List[str]:
        """Validate boolean field values."""
        errors = []
        valid_boolean_values = {'true', 'false', '1', '0', 'yes', 'no', 't', 'f', 'y', 'n'}
        
        for column in boolean_columns:
            if column not in df.columns:
                continue
            
            # Check if values are valid boolean representations
            string_values = df[column].astype(str).str.lower()
            non_null_values = string_values[string_values != 'nan']
            
            if len(non_null_values) > 0:
                invalid_booleans = ~non_null_values.isin(valid_boolean_values)
                if invalid_booleans.any():
                    violation_count = invalid_booleans.sum()
                    errors.append(f"{column}: {violation_count} invalid boolean values")
        
        return errors
    
    def _validate_datetime_sequences(self, df: pd.DataFrame, 
                                   sequence_rules: Dict[str, str]) -> List[str]:
        """Validate datetime field sequences (e.g., opened < paid < closed)."""
        errors = []
        
        for earlier_field, later_field in sequence_rules.items():
            if earlier_field not in df.columns or later_field not in df.columns:
                continue
            
            # Convert to datetime
            earlier_dates = pd.to_datetime(df[earlier_field], errors='coerce')
            later_dates = pd.to_datetime(df[later_field], errors='coerce')
            
            # Check where both dates exist and earlier is actually later
            both_exist = earlier_dates.notna() & later_dates.notna()
            sequence_violations = both_exist & (earlier_dates >= later_dates)
            
            if sequence_violations.any():
                violation_count = sequence_violations.sum()
                errors.append(f"Datetime sequence violation: {violation_count} records where {earlier_field} >= {later_field}")
        
        return errors
    
    def _validate_string_lengths(self, df: pd.DataFrame, 
                                length_limits: Dict[str, int]) -> List[str]:
        """Validate string field lengths."""
        warnings = []
        
        for column, max_length in length_limits.items():
            if column not in df.columns:
                continue
            
            # Check string lengths
            string_lengths = df[column].astype(str).str.len()
            long_strings = string_lengths > max_length
            
            if long_strings.any():
                violation_count = long_strings.sum()
                max_found = string_lengths.max()
                warnings.append(f"{column}: {violation_count} values exceed max length {max_length} (max found: {max_found})")
        
        return warnings
    
    def _validate_categorical(self, df: pd.DataFrame, column: str, 
                            valid_values: List[str]) -> List[str]:
        """Validate categorical field values."""
        errors = []
        
        if column not in df.columns:
            return errors
        
        # Check for invalid categorical values
        non_null_values = df[column].dropna()
        if len(non_null_values) > 0:
            invalid_values = ~non_null_values.isin(valid_values)
            if invalid_values.any():
                violation_count = invalid_values.sum()
                unique_invalid = non_null_values[invalid_values].unique()
                errors.append(f"{column}: {violation_count} invalid values. Valid: {valid_values}, Found: {list(unique_invalid)[:5]}")
        
        return errors
    
    def _validate_guest_count(self, df: pd.DataFrame, 
                            guest_range: Dict[str, int]) -> List[str]:
        """Validate guest count values."""
        errors = []
        
        guest_counts = pd.to_numeric(df["guest_count"], errors='coerce')
        
        # Check minimum
        if "min" in guest_range:
            min_violations = guest_counts < guest_range["min"]
            if min_violations.any():
                violation_count = min_violations.sum()
                errors.append(f"guest_count: {violation_count} values below minimum {guest_range['min']}")
        
        # Check maximum
        if "max" in guest_range:
            max_violations = guest_counts > guest_range["max"]
            if max_violations.any():
                violation_count = max_violations.sum()
                errors.append(f"guest_count: {violation_count} values above maximum {guest_range['max']}")
        
        return errors
    
    def _validate_card_digits(self, df: pd.DataFrame, 
                            digit_rules: Dict[str, int]) -> List[str]:
        """Validate card digit field lengths."""
        errors = []
        
        for column, expected_length in digit_rules.items():
            if column not in df.columns:
                continue
            
            # Filter non-empty values
            non_empty_values = df[column].dropna()
            non_empty_values = non_empty_values[non_empty_values != '']
            
            if len(non_empty_values) > 0:
                # Check if all values have expected length and are numeric
                string_values = non_empty_values.astype(str)
                length_violations = string_values.str.len() != expected_length
                non_numeric = ~string_values.str.isdigit()
                
                if length_violations.any():
                    violation_count = length_violations.sum()
                    errors.append(f"{column}: {violation_count} values don't have expected length {expected_length}")
                
                if non_numeric.any():
                    violation_count = non_numeric.sum()
                    errors.append(f"{column}: {violation_count} values contain non-numeric characters")
        
        return errors
    
    def detect_anomalies(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Detect data anomalies and outliers.
        
        Args:
            df: DataFrame to analyze
            filename: Source filename
            
        Returns:
            Anomaly detection results
        """
        anomalies = {
            "duplicates": self._detect_duplicates(df),
            "outliers": self._detect_outliers(df),
            "data_consistency": self._check_data_consistency(df, filename),
            "missing_data_patterns": self._analyze_missing_data(df)
        }
        
        return anomalies
    
    def _detect_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect duplicate records."""
        total_rows = len(df)
        duplicate_rows = df.duplicated().sum()
        
        # Find columns that might be unique identifiers
        potential_id_columns = [col for col in df.columns if 'id' in col.lower()]
        id_duplicates = {}
        
        for col in potential_id_columns:
            if col in df.columns:
                duplicate_count = df[col].duplicated().sum()
                if duplicate_count > 0:
                    id_duplicates[col] = duplicate_count
        
        return {
            "total_duplicate_rows": duplicate_rows,
            "duplicate_percentage": (duplicate_rows / total_rows * 100) if total_rows > 0 else 0,
            "id_column_duplicates": id_duplicates
        }
    
    def _detect_outliers(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect outliers in numeric columns using IQR method."""
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        outliers = {}
        
        for col in numeric_columns:
            # Calculate IQR
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            # Define outlier bounds
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Count outliers
            outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_count = outlier_mask.sum()
            
            if outlier_count > 0:
                outliers[col] = {
                    "count": outlier_count,
                    "percentage": (outlier_count / len(df) * 100),
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "min_outlier": df[col][outlier_mask].min(),
                    "max_outlier": df[col][outlier_mask].max()
                }
        
        return outliers
    
    def _check_data_consistency(self, df: pd.DataFrame, filename: str) -> List[str]:
        """Check for data consistency issues."""
        issues = []
        
        # Check for negative values in amount fields that should be positive
        amount_columns = [col for col in df.columns if 'amount' in col.lower() or 'price' in col.lower()]
        for col in amount_columns:
            if col in df.columns:
                negative_count = (pd.to_numeric(df[col], errors='coerce') < 0).sum()
                if negative_count > 0:
                    issues.append(f"{col}: {negative_count} negative values found")
        
        # Check for future dates
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        today = datetime.now().date()
        
        for col in date_columns:
            if col in df.columns:
                try:
                    dates = pd.to_datetime(df[col], errors='coerce')
                    future_dates = (dates.dt.date > today).sum()
                    if future_dates > 0:
                        issues.append(f"{col}: {future_dates} future dates found")
                except:
                    pass
        
        return issues
    
    def _analyze_missing_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze missing data patterns."""
        missing_stats = {}
        total_rows = len(df)
        
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                missing_stats[col] = {
                    "count": missing_count,
                    "percentage": (missing_count / total_rows * 100)
                }
        
        # Check for rows with excessive missing data
        missing_per_row = df.isnull().sum(axis=1)
        rows_with_high_missing = (missing_per_row > len(df.columns) * 0.5).sum()
        
        return {
            "column_missing_stats": missing_stats,
            "rows_with_high_missing": rows_with_high_missing,
            "total_missing_percentage": (df.isnull().sum().sum() / (total_rows * len(df.columns)) * 100)
        }