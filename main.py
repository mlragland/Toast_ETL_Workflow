#!/usr/bin/env python3
"""
Main entry point for Toast ETL Pipeline.

This modernized ETL pipeline replaces the legacy monolithic script with a 
modular, scalable, and maintainable architecture.
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
import pandas as pd

from src.config.settings import settings
from src.utils.logging_utils import setup_logging
from src.extractors.sftp_extractor import SFTPExtractor
from src.transformers.toast_transformer import ToastDataTransformer
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
    
    parser.add_argument(
        "--enable-validation",
        action="store_true",
        help="Enable advanced data quality validation during transformation"
    )
    
    parser.add_argument(
        "--quality-report",
        action="store_true",
        help="Generate comprehensive quality report for all files"
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


def run_transformation(date: str, input_dir: str, logger, enable_validation: bool = False) -> Optional[str]:
    """
    Run the data transformation phase.
    
    Args:
        date: Date in YYYYMMDD format
        input_dir: Directory containing raw files
        logger: Logger instance
        enable_validation: Whether to perform advanced data quality validation
        
    Returns:
        Output directory path if transformation succeeded, None otherwise
    """
    logger.info("=" * 60)
    logger.info("PHASE 2: TRANSFORMATION")
    logger.info("=" * 60)
    
    try:
        # Convert date format for processing_date
        processing_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        
        # Initialize transformer with processing date
        transformer = ToastDataTransformer(processing_date=processing_date)
        
        # Define output directory
        output_dir = str(Path(input_dir).parent / "cleaned" / date)
        
        logger.info(f"Input directory: {input_dir}")
        logger.info(f"Output directory: {output_dir}")
        
        # Transform all CSV files with optional validation
        results, validation_reports = transformer.transform_files(
            input_dir, output_dir, enable_validation=enable_validation
        )
        
        # Log results
        successful = [file for file, success in results.items() if success]
        failed = [file for file, success in results.items() if not success]
        
        logger.info(f"Transformation summary: {len(successful)} successful, {len(failed)} failed")
        
        if successful:
            logger.info("✅ Successfully transformed files:")
            for file in successful:
                logger.info(f"  - {file}")
        
        if failed:
            logger.error("❌ Failed to transform files:")
            for file in failed:
                logger.error(f"  - {file}")
        
        # Log validation results if enabled
        if enable_validation and validation_reports:
            logger.info("Advanced validation results:")
            for file, report in validation_reports.items():
                if "error" in report:
                    logger.error(f"❌ {file}: Validation error - {report['error']}")
                else:
                    severity = report.get("severity", "UNKNOWN")
                    if severity == "CRITICAL":
                        logger.error(f"❌ {file}: Critical quality issues detected")
                    elif severity == "WARNING":
                        logger.warning(f"⚠️ {file}: Quality warnings detected")
                    else:
                        logger.info(f"✅ {file}: Quality validation passed")
        
        # Validate transformed files
        logger.info("Validating transformed data...")
        output_path = Path(output_dir)
        cleaned_files = list(output_path.glob("*_cleaned.csv"))
        
        validation_results = []
        for cleaned_file in cleaned_files:
            validation = transformer.validate_transformed_data(str(cleaned_file))
            validation_results.append(validation)
            
            if validation.get("bigquery_compatible", False):
                logger.info(f"✅ {cleaned_file.name}: {validation['row_count']} rows, BigQuery compatible")
            else:
                logger.error(f"❌ {cleaned_file.name}: BigQuery compatibility issues")
                if "problematic_columns" in validation:
                    logger.error(f"  Problematic columns: {validation['problematic_columns']}")
        
        # Return output directory if any transformations succeeded
        if successful:
            return output_dir
        else:
            logger.error("All transformations failed")
            return None
            
    except Exception as e:
        logger.error(f"Transformation phase failed: {e}")
        return None


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
        
        # Prefer cleaned files if they exist, otherwise use raw files
        cleaned_files = list(input_path.glob("*_cleaned.csv"))
        raw_files = list(input_path.glob("*.csv"))
        
        if cleaned_files:
            csv_files = cleaned_files
            logger.info(f"Using cleaned/transformed files from {input_dir}")
        else:
            csv_files = [f for f in raw_files if not f.name.endswith("_cleaned.csv")]
            logger.info(f"Using raw files from {input_dir}")
        
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
                    'allitemsreport_cleaned': 'all_items_report',
                    'checkdetails': 'check_details',
                    'checkdetails_cleaned': 'check_details',
                    'cashentries': 'cash_entries',
                    'cashentries_cleaned': 'cash_entries',
                    'itemselectiondetails': 'item_selection_details',
                    'itemselectiondetails_cleaned': 'item_selection_details',
                    'kitchentimings': 'kitchen_timings',
                    'kitchentimings_cleaned': 'kitchen_timings',
                    'orderdetails': 'order_details',
                    'orderdetails_cleaned': 'order_details',
                    'paymentdetails': 'payment_details',
                    'paymentdetails_cleaned': 'payment_details'
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


def generate_quality_report(date: str, data_dir: str, logger) -> None:
    """
    Generate comprehensive quality report for all files.
    
    Args:
        date: Date in YYYYMMDD format
        data_dir: Directory containing CSV files
        logger: Logger instance
    """
    logger.info("=" * 60)
    logger.info("PHASE 4: COMPREHENSIVE QUALITY REPORT")
    logger.info("=" * 60)
    
    try:
        from src.validators.quality_checker import QualityChecker
        
        quality_checker = QualityChecker()
        
        # Load all CSV files
        data_path = Path(data_dir)
        csv_files = list(data_path.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {data_dir}")
            return
        
        logger.info(f"Analyzing {len(csv_files)} CSV files for comprehensive quality assessment")
        
        # Load data into memory for cross-file analysis
        file_data_map = {}
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                file_data_map[csv_file.name] = df
                logger.info(f"Loaded {csv_file.name}: {len(df)} rows, {len(df.columns)} columns")
            except Exception as e:
                logger.error(f"Failed to load {csv_file.name}: {e}")
        
        if not file_data_map:
            logger.error("No files could be loaded for quality analysis")
            return
        
        # Perform comprehensive quality check
        logger.info("Performing comprehensive quality analysis...")
        quality_report = quality_checker.comprehensive_quality_check(file_data_map)
        
        # Log summary results
        overall_status = quality_report["overall_status"]
        logger.info(f"Overall Quality Status: {overall_status}")
        
        # Log file-specific results
        logger.info("\nFile-by-file Quality Results:")
        for filename, file_report in quality_report["file_reports"].items():
            severity = file_report["severity"]
            status_emoji = "✅" if severity == "PASS" else "⚠️" if severity == "WARNING" else "❌"
            logger.info(f"{status_emoji} {filename}: {severity} ({file_report['row_count']} rows)")
            
            if file_report.get("critical_errors"):
                logger.error(f"  Critical issues: {len(file_report['critical_errors'])}")
            if file_report.get("warnings"):
                logger.warning(f"  Warnings: {len(file_report['warnings'])}")
        
        # Log cross-file summary
        cross_file = quality_report["cross_file_summary"]
        logger.info(f"\nCross-file Analysis:")
        logger.info(f"Total files: {cross_file['total_files']}")
        logger.info(f"Total records: {cross_file['total_records']:,}")
        
        # Log referential integrity results
        ref_integrity = quality_report["referential_integrity"]
        if ref_integrity:
            logger.info("\nReferential Integrity:")
            for rel_name, result in ref_integrity.items():
                if result.get("valid", True):
                    logger.info(f"✅ {rel_name}: Valid")
                else:
                    logger.error(f"❌ {rel_name}: {result.get('violations', ['Issues found'])}")
        
        # Log recommendations
        recommendations = quality_report["recommendations"]
        if recommendations:
            logger.info("\nRecommendations:")
            for i, recommendation in enumerate(recommendations, 1):
                logger.info(f"{i}. {recommendation}")
        
        # Save detailed report to file
        report_file = Path(data_dir) / f"quality_report_{date}.json"
        import json
        with open(report_file, 'w') as f:
            json.dump(quality_report, f, indent=2, default=str)
        logger.info(f"Detailed quality report saved to: {report_file}")
        
    except ImportError:
        logger.error("Quality checker modules not available. Cannot generate comprehensive report.")
    except Exception as e:
        logger.error(f"Quality report generation failed: {e}")


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
        transformed_dir = None
        if run_transform:
            if not local_dir:
                # If we're only running transform, assume files are already extracted
                local_dir = f"{settings.raw_local_dir}/{date}"
            
            transformed_dir = run_transformation(date, local_dir, logger, args.enable_validation)
            if not transformed_dir:
                logger.error("Pipeline failed during transformation phase")
                sys.exit(1)
        
        # Phase 3: Loading
        if run_load:
            # Use transformed files if available, otherwise use raw files
            load_dir = transformed_dir if transformed_dir else local_dir
            
            if not load_dir:
                # If we're only running load, check for cleaned files first
                cleaned_dir = f"{settings.raw_local_dir}/cleaned/{date}"
                raw_dir = f"{settings.raw_local_dir}/{date}"
                
                # Prefer cleaned directory if it exists and has files
                if os.path.exists(cleaned_dir) and os.listdir(cleaned_dir):
                    load_dir = cleaned_dir
                    logger.info(f"Load-only mode: Using cleaned files from {cleaned_dir}")
                else:
                    load_dir = raw_dir
                    logger.info(f"Load-only mode: Using raw files from {raw_dir}")
            
            success = run_loading(date, load_dir, logger)
            if not success:
                logger.error("Pipeline failed during loading phase")
                sys.exit(1)
        
        # Phase 4: Comprehensive Quality Report (if requested)
        if args.quality_report:
            # Generate quality report on cleaned files if available, otherwise raw files
            report_dir = transformed_dir if transformed_dir else load_dir
            if report_dir and os.path.exists(report_dir):
                generate_quality_report(date, report_dir, logger)
            else:
                logger.warning("No data directory available for quality report")
        
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