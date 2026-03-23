#!/usr/bin/env python3
"""
Toast ETL Pipeline Main Entry Point (with PyArrow Fix)

This is the main orchestrator for the Toast ETL pipeline with PyArrow conversion fixes applied.
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import Optional, List

# Apply PyArrow fix before importing other modules
import pandas as pd
import numpy as np

def fix_dataframe_for_bigquery(df: pd.DataFrame) -> pd.DataFrame:
    """Fix DataFrame types to prevent PyArrow conversion errors."""
    df_fixed = df.copy()
    
    # Convert all object columns to string to avoid PyArrow issues
    for col in df_fixed.columns:
        if df_fixed[col].dtype == 'object':
            df_fixed[col] = df_fixed[col].astype(str)
            df_fixed[col] = df_fixed[col].replace(['nan', 'None', 'NaT'], '')
        elif df_fixed[col].dtype in ['int64', 'float64']:
            if df_fixed[col].isnull().any():
                if df_fixed[col].dtype == 'int64':
                    df_fixed[col] = df_fixed[col].astype('Int64')
                else:
                    df_fixed[col] = df_fixed[col].astype('Float64')
    
    # Handle specific problematic columns
    problematic_string_columns = [
        'master_id', 'item_id', 'parent_id', 'order_id', 'payment_id', 
        'check_number', 'order_number', 'entry_id', 'duration_opened_to_paid'
    ]
    
    for col in problematic_string_columns:
        if col in df_fixed.columns:
            df_fixed[col] = df_fixed[col].astype(str)
            df_fixed[col] = df_fixed[col].replace(['nan', 'None', 'NaT'], '')
    
    return df_fixed

# Patch BigQuery loader before importing
def patch_bigquery_loader():
    """Monkey patch the BigQuery loader to fix PyArrow issues."""
    import src.loaders.bigquery_loader as bq_module
    
    original_load_dataframe = bq_module.BigQueryLoader.load_dataframe
    
    def patched_load_dataframe(self, df, table_name, source_file, write_disposition='WRITE_APPEND'):
        """Patched version that fixes DataFrame types before loading."""
        df_fixed = fix_dataframe_for_bigquery(df)
        return original_load_dataframe(self, df_fixed, table_name, source_file, write_disposition)
    
    bq_module.BigQueryLoader.load_dataframe = patched_load_dataframe
    print("✅ BigQuery loader patched to fix PyArrow conversion issues")

# Apply the patch
patch_bigquery_loader()

# Now import the rest of the modules
from src.config.settings import settings
from src.extractors.sftp_extractor import SFTPExtractor
from src.transformers.toast_transformer import ToastDataTransformer
from src.loaders.bigquery_loader import BigQueryLoader
from src.validators.schema_enforcer import SchemaEnforcer
from src.utils.logging_utils import setup_logging, get_logger

# Set up logging
setup_logging()
logger = get_logger(__name__)

class ToastETLPipeline:
    """Main ETL Pipeline orchestrator with PyArrow fixes"""
    
    def __init__(self):
        """Initialize the ETL pipeline components"""
        self.sftp_extractor = SFTPExtractor()
        self.transformer = ToastDataTransformer()
        self.bigquery_loader = BigQueryLoader()
        self.schema_enforcer = SchemaEnforcer()
        
    def run_pipeline(self, date: str, extract_only: bool = False, 
                    transform_only: bool = False, load_only: bool = False) -> bool:
        """
        Run the complete ETL pipeline for a specific date
        
        Args:
            date: Date in YYYYMMDD format
            extract_only: Only run extraction phase
            transform_only: Only run transformation phase  
            load_only: Only run loading phase
            
        Returns:
            True if pipeline completed successfully, False otherwise
        """
        logger.info("🍴 Toast ETL Pipeline Starting")
        logger.info(f"Processing date: {date}")
        logger.info(f"Environment: {settings.environment}")
        logger.info(f"Settings: GCP Project = {settings.gcp_project_id}")
        
        # Determine which phases to run
        run_extract = not transform_only and not load_only
        run_transform = not extract_only and not load_only
        run_load = not extract_only and not transform_only
        
        logger.info(f"Phases to run: Extract={run_extract}, Transform={run_transform}, Load={run_load}")
        logger.info("=" * 70)
        
        try:
            # Phase 1: Extraction
            if run_extract:
                logger.info("PHASE 1: EXTRACTION")
                logger.info("=" * 70)
                
                success = self.sftp_extractor.download_files_for_date(date)
                if not success:
                    logger.error("Extraction phase failed")
                    return False
                
                # Log file details
                raw_folder = f"/tmp/toast_raw_data/raw/{date}"
                if os.path.exists(raw_folder):
                    files = os.listdir(raw_folder)
                    logger.info(f"Downloaded {len(files)} files to {raw_folder}")
                    for file in sorted(files):
                        file_path = os.path.join(raw_folder, file)
                        size = os.path.getsize(file_path)
                        logger.info(f"  - {file} ({size} bytes)")
                
                logger.info("Extraction completed successfully")
            
            # Phase 2: Transformation
            if run_transform:
                logger.info("=" * 70)
                logger.info("PHASE 2: TRANSFORMATION")
                logger.info("=" * 70)
                
                input_folder = f"/tmp/toast_raw_data/raw/{date}"
                output_folder = f"/tmp/toast_raw_data/raw/cleaned/{date}"
                
                logger.info(f"Input directory: {input_folder}")
                logger.info(f"Output directory: {output_folder}")
                
                # Set processing date for transformer
                self.transformer.processing_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
                
                # Transform files
                results, _ = self.transformer.transform_files(input_folder, output_folder)
                
                successful = sum(1 for success in results.values() if success)
                failed = len(results) - successful
                
                logger.info(f"Transformation summary: {successful} successful, {failed} failed")
                
                if successful > 0:
                    logger.info("✅ Successfully transformed files:")
                    for file_name, success in results.items():
                        if success:
                            logger.info(f"  - {file_name}")
                
                if failed > 0:
                    logger.info("❌ Failed transformations:")
                    for file_name, success in results.items():
                        if not success:
                            logger.info(f"  - {file_name}")
                
                # Validate transformed data
                logger.info("Validating transformed data...")
                for file_name, success in results.items():
                    if success:
                        cleaned_file = os.path.join(output_folder, file_name.replace(".csv", "_cleaned.csv"))
                        if os.path.exists(cleaned_file):
                            validation_result = self.transformer.validate_transformed_data(cleaned_file)
                            rows = validation_result.get('row_count', 0)
                            compatible = validation_result.get('bigquery_compatible', False)
                            status = "BigQuery compatible" if compatible else "needs review"
                            logger.info(f"✅ {os.path.basename(cleaned_file)}: {rows} rows, {status}")
            
            # Phase 3: Loading
            if run_load:
                logger.info("=" * 70)
                logger.info("PHASE 3: LOADING")
                logger.info("=" * 70)
                
                cleaned_folder = f"/tmp/toast_raw_data/raw/cleaned/{date}"
                
                if not os.path.exists(cleaned_folder):
                    logger.error(f"Cleaned data folder not found: {cleaned_folder}")
                    return False
                
                logger.info(f"Using cleaned/transformed files from {cleaned_folder}")
                
                # Get list of CSV files to load
                csv_files = [f for f in os.listdir(cleaned_folder) if f.endswith('_cleaned.csv')]
                logger.info(f"Found {len(csv_files)} CSV files to load")
                
                successful_loads = 0
                failed_loads = []
                
                # Load each file
                for csv_file in csv_files:
                    file_path = os.path.join(cleaned_folder, csv_file)
                    
                    # Determine table name
                    original_name = csv_file.replace('_cleaned.csv', '.csv')
                    table_mappings = {
                        'AllItemsReport.csv': 'all_items_report',
                        'CheckDetails.csv': 'check_details',
                        'CashEntries.csv': 'cash_entries',
                        'ItemSelectionDetails.csv': 'item_selection_details',
                        'KitchenTimings.csv': 'kitchen_timings',
                        'OrderDetails.csv': 'order_details',
                        'PaymentDetails.csv': 'payment_details'
                    }
                    
                    table_name = table_mappings.get(original_name, original_name.lower().replace('.csv', ''))
                    
                    logger.info(f"Loading {csv_file} to BigQuery")
                    
                    try:
                        # Read the CSV file
                        df = pd.read_csv(file_path)
                        
                        # Load to BigQuery (with PyArrow fix applied)
                        result = self.bigquery_loader.load_dataframe(
                            df, table_name, csv_file, 'WRITE_APPEND'
                        )
                        
                        if result.get('success', False):
                            rows_loaded = result.get('rows_loaded', 0)
                            logger.info(f"Successfully loaded {rows_loaded} rows to {settings.gcp_project_id}.{settings.bigquery_dataset}.{table_name}")
                            successful_loads += 1
                        else:
                            error_msg = result.get('errors', 'Unknown error')
                            logger.error(f"Failed to load {csv_file}: {error_msg}")
                            failed_loads.append(f"{csv_file}: {error_msg}")
                            
                    except Exception as e:
                        error_msg = str(e)
                        logger.error(f"Failed to process {csv_file}: {error_msg}")
                        failed_loads.append(f"{csv_file}: {error_msg}")
                
                # Summary
                logger.info(f"Loading summary: {successful_loads} successful, {len(failed_loads)} failed")
                
                if failed_loads:
                    logger.error("Failed loads:")
                    for failure in failed_loads:
                        logger.error(f"  - {failure}")
                
                if successful_loads == 0:
                    logger.error("No files were successfully loaded")
                    return False
            
            # Cleanup
            logger.info("=" * 70)
            logger.info("CLEANUP")
            logger.info("=" * 70)
            
            # Clean up SFTP connections
            self.sftp_extractor = SFTPExtractor()  # Reinitialize to close connections
            logger.info("Cleanup completed successfully")
            
            logger.info("🎉 Toast ETL Pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {e}")
            return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Toast ETL Pipeline')
    parser.add_argument('--date', required=True, help='Date in YYYYMMDD format')
    parser.add_argument('--extract-only', action='store_true', help='Only run extraction phase')
    parser.add_argument('--transform-only', action='store_true', help='Only run transformation phase')
    parser.add_argument('--load-only', action='store_true', help='Only run loading phase')
    
    args = parser.parse_args()
    
    # Validate date format
    try:
        datetime.strptime(args.date, '%Y%m%d')
    except ValueError:
        logger.error(f"Invalid date format: {args.date}. Expected YYYYMMDD")
        sys.exit(1)
    
    # Initialize and run pipeline
    pipeline = ToastETLPipeline()
    success = pipeline.run_pipeline(
        args.date, 
        args.extract_only, 
        args.transform_only, 
        args.load_only
    )
    
    if not success:
        logger.error("Pipeline failed during loading phase")
        sys.exit(1)

if __name__ == "__main__":
    main() 