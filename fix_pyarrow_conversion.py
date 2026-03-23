#!/usr/bin/env python3
"""
Fix PyArrow Conversion Issues

This script patches the BigQuery loader to handle PyArrow conversion errors
by converting problematic columns to strings before loading.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any

def fix_dataframe_for_bigquery(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix DataFrame types to prevent PyArrow conversion errors.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with corrected types
    """
    df_fixed = df.copy()
    
    # Convert all object columns to string to avoid PyArrow issues
    for col in df_fixed.columns:
        if df_fixed[col].dtype == 'object':
            df_fixed[col] = df_fixed[col].astype(str)
            # Replace pandas NaN representations
            df_fixed[col] = df_fixed[col].replace(['nan', 'None', 'NaT'], '')
        
        # Handle numeric columns that might have conversion issues
        elif df_fixed[col].dtype in ['int64', 'float64']:
            # Check if column has NaN values
            if df_fixed[col].isnull().any():
                # Convert to nullable integer/float types
                if df_fixed[col].dtype == 'int64':
                    df_fixed[col] = df_fixed[col].astype('Int64')
                else:
                    df_fixed[col] = df_fixed[col].astype('Float64')
    
    # Handle specific problematic columns based on error patterns
    problematic_string_columns = [
        'master_id', 'item_id', 'parent_id', 'order_id', 'payment_id', 
        'check_number', 'order_number', 'entry_id', 'duration_opened_to_paid'
    ]
    
    for col in problematic_string_columns:
        if col in df_fixed.columns:
            # Force convert to string
            df_fixed[col] = df_fixed[col].astype(str)
            df_fixed[col] = df_fixed[col].replace(['nan', 'None', 'NaT'], '')
    
    return df_fixed

def patch_bigquery_loader():
    """
    Monkey patch the BigQuery loader to fix PyArrow issues.
    """
    import src.loaders.bigquery_loader as bq_module
    
    # Store original method
    original_load_dataframe = bq_module.BigQueryLoader.load_dataframe
    
    def patched_load_dataframe(self, df, table_name, source_file, write_disposition='WRITE_APPEND'):
        """Patched version that fixes DataFrame types before loading."""
        # Fix DataFrame types
        df_fixed = fix_dataframe_for_bigquery(df)
        
        # Call original method with fixed DataFrame
        return original_load_dataframe(self, df_fixed, table_name, source_file, write_disposition)
    
    # Apply the patch
    bq_module.BigQueryLoader.load_dataframe = patched_load_dataframe
    print("✅ BigQuery loader patched to fix PyArrow conversion issues")

if __name__ == "__main__":
    print("🔧 PyArrow Conversion Fix")
    print("This script patches the BigQuery loader to handle conversion errors.")
    print("Import this module before running the ETL pipeline.")
    patch_bigquery_loader() 