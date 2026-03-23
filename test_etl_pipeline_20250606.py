#!/usr/bin/env python3
"""
Comprehensive ETL Pipeline Test for Date 2025-06-06

This test verifies that the complete ETL pipeline works correctly for all tables
on a specific date (2025-06-06). It tests:
1. Extraction from SFTP
2. Transformation of all CSV files
3. Loading to BigQuery
4. Data validation and quality checks
"""

import os
import sys
import tempfile
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.config.settings import settings
from src.config.file_config import FILE_CONFIG, get_supported_files
from src.utils.logging_utils import setup_logging
from src.extractors.sftp_extractor import SFTPExtractor
from src.transformers.toast_transformer import ToastDataTransformer
from src.loaders.bigquery_loader import BigQueryLoader
from google.cloud import bigquery


class ETLPipelineTest:
    """Comprehensive ETL Pipeline Test for 2025-06-06."""
    
    def __init__(self):
        self.test_date = "20250606"
        self.test_date_formatted = "2025-06-06"
        self.logger = setup_logging("etl_pipeline_test", level=logging.DEBUG)
        self.temp_dir = None
        self.extraction_dir = None
        self.transformation_dir = None
        self.test_results = {
            'extraction': {},
            'transformation': {},
            'loading': {},
            'validation': {}
        }
        
        # Expected files based on FILE_CONFIG
        self.expected_files = get_supported_files()
        
        # BigQuery client for validation
        self.bq_client = bigquery.Client(project=settings.gcp_project_id)
        
    def setup_test_environment(self):
        """Set up temporary directories for testing."""
        self.logger.info("Setting up test environment...")
        
        # Create temporary directory
        self.temp_dir = tempfile.mkdtemp(prefix="etl_test_20250606_")
        self.extraction_dir = os.path.join(self.temp_dir, "raw", self.test_date)
        self.transformation_dir = os.path.join(self.temp_dir, "cleaned", self.test_date)
        
        # Create directories
        os.makedirs(self.extraction_dir, exist_ok=True)
        os.makedirs(self.transformation_dir, exist_ok=True)
        
        self.logger.info(f"Test environment created at: {self.temp_dir}")
        self.logger.info(f"Extraction directory: {self.extraction_dir}")
        self.logger.info(f"Transformation directory: {self.transformation_dir}")
        
    def test_extraction(self) -> bool:
        """Test the extraction phase."""
        self.logger.info("=" * 60)
        self.logger.info("TESTING EXTRACTION PHASE")
        self.logger.info("=" * 60)
        
        try:
            extractor = SFTPExtractor()
            
            # Download files for the test date
            downloaded_dir = extractor.download_files(self.test_date)
            
            if not downloaded_dir:
                self.logger.error("❌ Extraction failed - no files downloaded")
                self.test_results['extraction']['success'] = False
                return False
            
            # Get file information
            file_info = extractor.get_file_info(self.test_date)
            
            # Copy files to our test directory for consistency
            if downloaded_dir != self.extraction_dir:
                for file_path in Path(downloaded_dir).glob("*.csv"):
                    shutil.copy2(file_path, self.extraction_dir)
            
            # Validate extracted files
            extracted_files = list(Path(self.extraction_dir).glob("*.csv"))
            extracted_filenames = [f.name for f in extracted_files]
            
            self.logger.info(f"✅ Extraction completed successfully")
            self.logger.info(f"Downloaded {len(extracted_files)} files:")
            
            for filename in extracted_filenames:
                file_path = os.path.join(self.extraction_dir, filename)
                file_size = os.path.getsize(file_path)
                self.logger.info(f"  - {filename} ({file_size:,} bytes)")
                
                # Check if file is expected
                if filename in self.expected_files:
                    self.test_results['extraction'][filename] = {
                        'found': True,
                        'size': file_size,
                        'status': 'success'
                    }
                else:
                    self.logger.warning(f"  ⚠️  Unexpected file: {filename}")
                    self.test_results['extraction'][filename] = {
                        'found': True,
                        'size': file_size,
                        'status': 'unexpected'
                    }
            
            # Check for missing expected files
            missing_files = set(self.expected_files) - set(extracted_filenames)
            if missing_files:
                self.logger.warning(f"⚠️  Missing expected files: {missing_files}")
                for filename in missing_files:
                    self.test_results['extraction'][filename] = {
                        'found': False,
                        'size': 0,
                        'status': 'missing'
                    }
            
            self.test_results['extraction']['success'] = True
            self.test_results['extraction']['total_files'] = len(extracted_files)
            self.test_results['extraction']['expected_files'] = len(self.expected_files)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Extraction failed with error: {e}")
            self.test_results['extraction']['success'] = False
            self.test_results['extraction']['error'] = str(e)
            return False
    
    def test_transformation(self) -> bool:
        """Test the transformation phase."""
        self.logger.info("=" * 60)
        self.logger.info("TESTING TRANSFORMATION PHASE")
        self.logger.info("=" * 60)
        
        try:
            # Initialize transformer
            transformer = ToastDataTransformer(processing_date=self.test_date_formatted)
            
            # Transform files with validation enabled
            results, validation_reports = transformer.transform_files(
                self.extraction_dir, 
                self.transformation_dir, 
                enable_validation=True
            )
            
            # Analyze results
            successful_files = [file for file, success in results.items() if success]
            failed_files = [file for file, success in results.items() if not success]
            
            self.logger.info(f"Transformation summary: {len(successful_files)} successful, {len(failed_files)} failed")
            
            if successful_files:
                self.logger.info("✅ Successfully transformed files:")
                for filename in successful_files:
                    # Check for both original name and _cleaned suffix
                    output_path = os.path.join(self.transformation_dir, filename)
                    cleaned_filename = filename.replace('.csv', '_cleaned.csv')
                    cleaned_path = os.path.join(self.transformation_dir, cleaned_filename)
                    
                    # Try to find the actual output file
                    actual_path = None
                    if os.path.exists(output_path):
                        actual_path = output_path
                    elif os.path.exists(cleaned_path):
                        actual_path = cleaned_path
                    
                    if actual_path:
                        # Get row count and file size
                        df = pd.read_csv(actual_path)
                        row_count = len(df)
                        file_size = os.path.getsize(actual_path)
                        
                        self.logger.info(f"  - {filename} → {os.path.basename(actual_path)} ({row_count:,} rows, {file_size:,} bytes)")
                        
                        self.test_results['transformation'][filename] = {
                            'success': True,
                            'row_count': row_count,
                            'file_size': file_size,
                            'columns': list(df.columns),
                            'output_file': os.path.basename(actual_path)
                        }
                        
                        # Add validation report if available
                        if filename in validation_reports:
                            self.test_results['transformation'][filename]['validation'] = validation_reports[filename]
                    else:
                        self.logger.warning(f"  ⚠️  Output file not found: {filename} (checked {filename} and {cleaned_filename})")
                        self.test_results['transformation'][filename] = {
                            'success': False,
                            'error': 'Output file not found'
                        }
            
            if failed_files:
                self.logger.error("❌ Failed to transform files:")
                for filename in failed_files:
                    self.logger.error(f"  - {filename}")
                    self.test_results['transformation'][filename] = {
                        'success': False,
                        'error': 'Transformation failed'
                    }
            
            self.test_results['transformation']['success'] = len(failed_files) == 0
            self.test_results['transformation']['total_files'] = len(results)
            self.test_results['transformation']['successful_files'] = len(successful_files)
            self.test_results['transformation']['failed_files'] = len(failed_files)
            
            return len(failed_files) == 0
            
        except Exception as e:
            self.logger.error(f"❌ Transformation failed with error: {e}")
            self.test_results['transformation']['success'] = False
            self.test_results['transformation']['error'] = str(e)
            return False
    
    def test_loading(self) -> bool:
        """Test the loading phase."""
        self.logger.info("=" * 60)
        self.logger.info("TESTING LOADING PHASE")
        self.logger.info("=" * 60)
        
        try:
            # Initialize loader
            loader = BigQueryLoader()
            
            # Get list of transformed files
            transformed_files = list(Path(self.transformation_dir).glob("*.csv"))
            
            if not transformed_files:
                self.logger.error("❌ No transformed files found for loading")
                self.test_results['loading']['success'] = False
                return False
            
            loading_results = {}
            
            for file_path in transformed_files:
                filename = file_path.name
                # Map cleaned filename back to original table name
                original_filename = filename.replace('_cleaned', '')
                table_name = original_filename.replace('.csv', '').lower()
                
                self.logger.info(f"Loading {filename} to table {table_name}...")
                
                try:
                    # Load file to BigQuery
                    df = pd.read_csv(file_path)
                    job_result = loader.load_dataframe(
                        df,
                        table_name,
                        str(file_path),
                        write_disposition='WRITE_APPEND'  # Append to existing data
                    )
                    
                    if job_result and job_result.get('success', False):
                        # Get row count from BigQuery
                        query = f"""
                        SELECT COUNT(*) as row_count 
                        FROM `{settings.gcp_project_id}.{settings.bigquery_dataset}.{table_name}`
                        WHERE processing_date = '{self.test_date_formatted}'
                        """
                        
                        query_job = self.bq_client.query(query)
                        results = list(query_job)
                        row_count = results[0].row_count if results else 0
                        
                        self.logger.info(f"  ✅ Successfully loaded {row_count:,} rows to {table_name}")
                        
                        loading_results[original_filename] = {
                            'success': True,
                            'table_name': table_name,
                            'rows_loaded': row_count,
                            'job_id': job_result.get('job_id', 'unknown')
                        }
                    else:
                        error_msg = f"Job failed: {job_result.get('error', 'Unknown error') if job_result else 'No job result'}"
                        self.logger.error(f"  ❌ Failed to load {filename}: {error_msg}")
                        loading_results[original_filename] = {
                            'success': False,
                            'error': error_msg
                        }
                        
                except Exception as e:
                    self.logger.error(f"  ❌ Failed to load {filename}: {e}")
                    loading_results[original_filename] = {
                        'success': False,
                        'error': str(e)
                    }
            
            # Summarize loading results
            successful_loads = [f for f, r in loading_results.items() if r['success']]
            failed_loads = [f for f, r in loading_results.items() if not r['success']]
            
            self.test_results['loading'] = {
                'success': len(failed_loads) == 0,
                'total_files': len(loading_results),
                'successful_loads': len(successful_loads),
                'failed_loads': len(failed_loads),
                'results': loading_results
            }
            
            return len(failed_loads) == 0
            
        except Exception as e:
            self.logger.error(f"❌ Loading failed with error: {e}")
            self.test_results['loading']['success'] = False
            self.test_results['loading']['error'] = str(e)
            return False
    
    def test_data_validation(self) -> bool:
        """Test data validation and quality checks."""
        self.logger.info("=" * 60)
        self.logger.info("TESTING DATA VALIDATION")
        self.logger.info("=" * 60)
        
        try:
            validation_results = {}
            
            # Check each table in BigQuery
            for filename in self.expected_files:
                table_name = filename.replace('.csv', '').lower()
                
                try:
                    # Basic data quality checks
                    query = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(DISTINCT processing_date) as unique_dates,
                        MIN(processing_date) as min_date,
                        MAX(processing_date) as max_date
                    FROM `{settings.gcp_project_id}.{settings.bigquery_dataset}.{table_name}`
                    WHERE processing_date = '{self.test_date_formatted}'
                    """
                    
                    query_job = self.bq_client.query(query)
                    results = list(query_job)
                    
                    if results:
                        result = results[0]
                        validation_results[table_name] = {
                            'exists': True,
                            'total_rows': result.total_rows,
                            'unique_dates': result.unique_dates,
                            'min_date': str(result.min_date) if result.min_date else None,
                            'max_date': str(result.max_date) if result.max_date else None,
                            'has_test_date_data': result.total_rows > 0
                        }
                        
                        self.logger.info(f"✅ {table_name}: {result.total_rows:,} rows for {self.test_date_formatted}")
                    else:
                        validation_results[table_name] = {
                            'exists': True,
                            'total_rows': 0,
                            'has_test_date_data': False
                        }
                        self.logger.warning(f"⚠️  {table_name}: No data found for {self.test_date_formatted}")
                        
                except Exception as e:
                    if "Not found" in str(e):
                        validation_results[table_name] = {
                            'exists': False,
                            'error': 'Table not found'
                        }
                        self.logger.error(f"❌ {table_name}: Table not found")
                    else:
                        validation_results[table_name] = {
                            'exists': True,
                            'error': str(e)
                        }
                        self.logger.error(f"❌ {table_name}: Validation error - {e}")
            
            # Calculate validation summary
            tables_with_data = sum(1 for r in validation_results.values() 
                                 if r.get('has_test_date_data', False))
            tables_existing = sum(1 for r in validation_results.values() 
                                if r.get('exists', False))
            
            self.test_results['validation'] = {
                'success': tables_with_data > 0,
                'tables_existing': tables_existing,
                'tables_with_data': tables_with_data,
                'total_expected': len(self.expected_files),
                'results': validation_results
            }
            
            self.logger.info(f"Validation summary: {tables_with_data}/{len(self.expected_files)} tables have data for {self.test_date_formatted}")
            
            return tables_with_data > 0
            
        except Exception as e:
            self.logger.error(f"❌ Data validation failed with error: {e}")
            self.test_results['validation']['success'] = False
            self.test_results['validation']['error'] = str(e)
            return False
    
    def generate_test_report(self):
        """Generate a comprehensive test report."""
        self.logger.info("=" * 60)
        self.logger.info("ETL PIPELINE TEST REPORT - 2025-06-06")
        self.logger.info("=" * 60)
        
        # Overall summary
        phases = ['extraction', 'transformation', 'loading', 'validation']
        successful_phases = sum(1 for phase in phases if self.test_results.get(phase, {}).get('success', False))
        
        self.logger.info(f"Overall Test Result: {successful_phases}/{len(phases)} phases successful")
        
        # Detailed results for each phase
        for phase in phases:
            phase_result = self.test_results.get(phase, {})
            success = phase_result.get('success', False)
            status = "✅ PASSED" if success else "❌ FAILED"
            
            self.logger.info(f"\n{phase.upper()}: {status}")
            
            if phase == 'extraction':
                total_files = phase_result.get('total_files', 0)
                expected_files = phase_result.get('expected_files', 0)
                self.logger.info(f"  Files extracted: {total_files}/{expected_files}")
                
            elif phase == 'transformation':
                successful = phase_result.get('successful_files', 0)
                failed = phase_result.get('failed_files', 0)
                self.logger.info(f"  Files transformed: {successful}/{successful + failed}")
                
            elif phase == 'loading':
                successful = phase_result.get('successful_loads', 0)
                failed = phase_result.get('failed_loads', 0)
                self.logger.info(f"  Files loaded: {successful}/{successful + failed}")
                
            elif phase == 'validation':
                tables_with_data = phase_result.get('tables_with_data', 0)
                total_expected = phase_result.get('total_expected', 0)
                self.logger.info(f"  Tables with data: {tables_with_data}/{total_expected}")
            
            if 'error' in phase_result:
                self.logger.info(f"  Error: {phase_result['error']}")
        
        # File-by-file summary
        self.logger.info(f"\nFILE-BY-FILE SUMMARY:")
        for filename in self.expected_files:
            extraction_status = "✅" if self.test_results.get('extraction', {}).get(filename, {}).get('found', False) else "❌"
            transformation_status = "✅" if self.test_results.get('transformation', {}).get(filename, {}).get('success', False) else "❌"
            
            table_name = filename.replace('.csv', '').lower()
            loading_status = "✅" if self.test_results.get('loading', {}).get('results', {}).get(filename, {}).get('success', False) else "❌"
            validation_status = "✅" if self.test_results.get('validation', {}).get('results', {}).get(table_name, {}).get('has_test_date_data', False) else "❌"
            
            self.logger.info(f"  {filename}: Extract{extraction_status} Transform{transformation_status} Load{loading_status} Validate{validation_status}")
        
        return successful_phases == len(phases)
    
    def cleanup(self):
        """Clean up test environment."""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up test directory: {self.temp_dir}")
            except Exception as e:
                self.logger.warning(f"Failed to clean up test directory: {e}")
    
    def run_full_test(self) -> bool:
        """Run the complete ETL pipeline test."""
        try:
            self.logger.info("🚀 Starting ETL Pipeline Test for 2025-06-06")
            self.logger.info(f"Expected files: {', '.join(self.expected_files)}")
            
            # Setup
            self.setup_test_environment()
            
            # Run test phases
            extraction_success = self.test_extraction()
            transformation_success = self.test_transformation() if extraction_success else False
            loading_success = self.test_loading() if transformation_success else False
            validation_success = self.test_data_validation() if loading_success else False
            
            # Generate report
            overall_success = self.generate_test_report()
            
            return overall_success
            
        except Exception as e:
            self.logger.error(f"❌ Test execution failed: {e}")
            return False
        finally:
            self.cleanup()


def main():
    """Main function to run the ETL pipeline test."""
    # Set environment variables if not already set
    os.environ.setdefault('PROJECT_ID', 'toast-analytics-444116')
    os.environ.setdefault('DATASET_ID', 'toast_analytics')
    os.environ.setdefault('ENVIRONMENT', 'development')
    
    # Run the test
    test = ETLPipelineTest()
    success = test.run_full_test()
    
    if success:
        print("\n🎉 ETL Pipeline Test PASSED for 2025-06-06!")
        sys.exit(0)
    else:
        print("\n💥 ETL Pipeline Test FAILED for 2025-06-06!")
        sys.exit(1)


if __name__ == "__main__":
    main() 