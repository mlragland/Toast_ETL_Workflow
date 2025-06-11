#!/usr/bin/env python3
"""
Main entry point for Toast ETL Pipeline.

This modernized ETL pipeline replaces the legacy monolithic script with a 
modular, scalable, and maintainable architecture.
"""

import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
import pandas as pd

from src.config.settings import settings
from src.utils.logging_utils import setup_logging
from src.extractors.sftp_extractor import SFTPExtractor
from src.loaders.bigquery_loader import BigQueryLoader


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Toast ETL Pipeline - Extract, Transform, Load data from Toast POS"
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="Date to process in YYYYMMDD format (default: yesterday)"
    )
    
    parser.add_argument(
        "--environment",
        type=str,
        choices=["development", "staging", "production"],
        default=settings.environment,
        help="Environment to run in"
    )
    
    parser.add_argument(
        "--extract-only",
        action="store_true",
        help="Only run the extraction phase"
    )
    
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Only run the transformation phase"
    )
    
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Only run the loading phase"
    )
    
    parser.add_argument(
        "--skip-cleanup",
        action="store_true",
        help="Skip cleaning up temporary files"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    return parser.parse_args()


def get_processing_date(date_str: Optional[str] = None) -> str:
    """
    Get the date to process.
    
    Args:
        date_str: Date string in YYYYMMDD format, or None for yesterday
        
    Returns:
        Date in YYYYMMDD format
    """
    if date_str:
        try:
            # Validate date format
            datetime.strptime(date_str, "%Y%m%d")
            return date_str
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD")
    
    # Default to yesterday
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d")


def run_extraction(date: str, logger) -> Optional[str]:
    """
    Run the data extraction phase.
    
    Args:
        date: Date in YYYYMMDD format
        logger: Logger instance
        
    Returns:
        Local directory path where files were downloaded, or None if failed
    """
    logger.info("=" * 60)
    logger.info("PHASE 1: EXTRACTION")
    logger.info("=" * 60)
    
    extractor = SFTPExtractor()
    
    try:
        local_dir = extractor.download_files(date)
        
        if local_dir:
            file_info = extractor.get_file_info(date)
            logger.info(f"Extraction completed successfully")
            logger.info(f"Downloaded {file_info['total_files']} files to {local_dir}")
            
            for file in file_info['files']:
                logger.info(f"  - {file['name']} ({file['size']} bytes)")
        else:
            logger.error("Extraction failed - no files downloaded")
            
        return local_dir
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return None


def run_transformation(date: str, input_dir: str, logger) -> bool:
    """
    Run the data transformation phase.
    
    Args:
        date: Date in YYYYMMDD format
        input_dir: Directory containing raw files
        logger: Logger instance
        
    Returns:
        True if transformation succeeded, False otherwise
    """
    logger.info("=" * 60)
    logger.info("PHASE 2: TRANSFORMATION")
    logger.info("=" * 60)
    
    # TODO: Implement transformation logic
    # This will use the CSVTransformer class we'll create next
    logger.info("Transformation phase - TO BE IMPLEMENTED")
    logger.info(f"Would transform files from: {input_dir}")
    
    return True


def run_loading(date: str, input_dir: str, logger) -> bool:
    """
    Run the data loading phase.
    
    Args:
        date: Date in YYYYMMDD format
        input_dir: Directory containing processed files
        logger: Logger instance
        
    Returns:
        True if loading succeeded, False otherwise
    """
    logger.info("=" * 60)
    logger.info("PHASE 3: LOADING")
    logger.info("=" * 60)
    
    try:
        # Initialize BigQuery loader
        bq_loader = BigQueryLoader()
        
        # Find all CSV files in the input directory
        input_path = Path(input_dir)
        csv_files = list(input_path.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {input_dir}")
            return True
        
        logger.info(f"Found {len(csv_files)} CSV files to load")
        
        # Load each CSV file to BigQuery
        results = []
        for csv_file in csv_files:
            logger.info(f"Loading {csv_file.name} to BigQuery")
            
            try:
                # Read CSV file
                df = pd.read_csv(csv_file)
                
                # Determine table name from filename
                table_name = csv_file.stem.lower().replace(' ', '_')
                
                # Map common Toast file patterns to table names
                table_mapping = {
                    'allitemsreport': 'all_items_report',
                    'checkdetails': 'check_details',
                    'cashentries': 'cash_entries',
                    'itemselectiondetails': 'item_selection_details',
                    'kitchentimings': 'kitchen_timings',
                    'orderdetails': 'order_details',
                    'paymentdetails': 'payment_details'
                }
                
                # Use mapped table name if available
                table_name = table_mapping.get(table_name, table_name)
                
                # Load to BigQuery
                result = bq_loader.load_dataframe(
                    df=df,
                    table_name=table_name,
                    source_file=csv_file.name
                )
                
                results.append(result)
                
                if result['success']:
                    logger.info(f"✅ Loaded {result['rows_loaded']} rows to {table_name}")
                else:
                    logger.error(f"❌ Failed to load {csv_file.name}: {result['errors']}")
                    
            except Exception as e:
                logger.error(f"Failed to process {csv_file.name}: {str(e)}")
                results.append({
                    'success': False,
                    'table_name': table_name if 'table_name' in locals() else 'unknown',
                    'source_file': csv_file.name,
                    'errors': str(e)
                })
        
        # Summary
        successful_loads = [r for r in results if r['success']]
        failed_loads = [r for r in results if not r['success']]
        
        logger.info(f"Loading summary: {len(successful_loads)} successful, {len(failed_loads)} failed")
        
        if successful_loads:
            total_rows = sum(r['rows_loaded'] for r in successful_loads)
            logger.info(f"Total rows loaded: {total_rows}")
        
        if failed_loads:
            logger.error("Failed loads:")
            for result in failed_loads:
                logger.error(f"  - {result['source_file']}: {result['errors']}")
        
        return len(failed_loads) == 0
        
    except Exception as e:
        logger.error(f"Loading phase failed: {e}")
        return False


def cleanup_temp_files(date: str, logger) -> None:
    """
    Clean up temporary files.
    
    Args:
        date: Date in YYYYMMDD format
        logger: Logger instance
    """
    logger.info("=" * 60)
    logger.info("CLEANUP")
    logger.info("=" * 60)
    
    try:
        extractor = SFTPExtractor()
        extractor.cleanup_old_files(days_to_keep=7)
        logger.info("Cleanup completed successfully")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")


def main():
    """Main entry point for the Toast ETL Pipeline."""
    args = parse_arguments()
    
    # Setup logging
    log_level = "DEBUG" if args.debug else "INFO"
    logger = setup_logging(
        name="toast_etl_main",
        level=getattr(sys.modules['logging'], log_level)
    )
    
    try:
        # Get processing date
        date = get_processing_date(args.date)
        
        logger.info("🍴 Toast ETL Pipeline Starting")
        logger.info(f"Processing date: {date}")
        logger.info(f"Environment: {args.environment}")
        logger.info(f"Settings: GCP Project = {settings.gcp_project_id}")
        
        # Determine which phases to run
        run_extract = not (args.transform_only or args.load_only)
        run_transform = not (args.extract_only or args.load_only)
        run_load = not (args.extract_only or args.transform_only)
        
        logger.info(f"Phases to run: Extract={run_extract}, Transform={run_transform}, Load={run_load}")
        
        local_dir = None
        
        # Phase 1: Extraction
        if run_extract:
            local_dir = run_extraction(date, logger)
            if not local_dir:
                logger.error("Pipeline failed during extraction phase")
                sys.exit(1)
        
        # Phase 2: Transformation
        if run_transform:
            if not local_dir:
                # If we're only running transform, assume files are already extracted
                local_dir = f"{settings.raw_local_dir}/{date}"
            
            success = run_transformation(date, local_dir, logger)
            if not success:
                logger.error("Pipeline failed during transformation phase")
                sys.exit(1)
        
        # Phase 3: Loading
        if run_load:
            if not local_dir:
                # If we're only running load, assume files are already processed
                local_dir = f"{settings.raw_local_dir}/{date}"
            
            success = run_loading(date, local_dir, logger)
            if not success:
                logger.error("Pipeline failed during loading phase")
                sys.exit(1)
        
        # Cleanup
        if not args.skip_cleanup:
            cleanup_temp_files(date, logger)
        
        logger.info("🎉 Toast ETL Pipeline completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main() 