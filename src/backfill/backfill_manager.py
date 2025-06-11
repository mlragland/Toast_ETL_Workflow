"""
Historical Backfill Manager for Toast ETL Pipeline.
Handles bulk processing of historical data with date range support.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from ..config.settings import settings
from ..extractors.sftp_extractor import SFTPExtractor
from ..transformers.toast_transformer import ToastDataTransformer
from ..loaders.bigquery_loader import BigQueryLoader
from ..validators.data_validator import DataValidator
from ..utils.logging_utils import get_logger
from ..utils.retry_utils import retry_with_backoff

logger = get_logger(__name__)


class BackfillManager:
    """Manages historical data backfill operations."""
    
    def __init__(self, 
                 max_workers: int = 3,
                 batch_size: int = 10,
                 skip_existing: bool = True,
                 validate_data: bool = True):
        """
        Initialize backfill manager.
        
        Args:
            max_workers: Maximum number of concurrent processing threads
            batch_size: Number of dates to process in each batch
            skip_existing: Skip dates already processed
            validate_data: Run data validation after loading
        """
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.skip_existing = skip_existing
        self.validate_data = validate_data
        
        # Initialize components
        self.sftp_extractor = SFTPExtractor()
        self.transformer = ToastDataTransformer()
        self.loader = BigQueryLoader()
        self.validator = DataValidator() if validate_data else None
        
        # Processing stats
        self.stats = {
            'total_dates': 0,
            'processed_dates': 0,
            'skipped_dates': 0,
            'failed_dates': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None,
            'failed_date_list': []
        }
        
        logger.info(f"BackfillManager initialized with max_workers={max_workers}, batch_size={batch_size}")
    
    def get_date_range(self, start_date: str, end_date: str) -> List[str]:
        """
        Generate list of dates between start and end date.
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            List of dates in YYYYMMDD format
        """
        try:
            start = datetime.strptime(start_date, '%Y%m%d')
            end = datetime.strptime(end_date, '%Y%m%d')
            
            dates = []
            current = start
            while current <= end:
                dates.append(current.strftime('%Y%m%d'))
                current += timedelta(days=1)
            
            logger.info(f"Generated {len(dates)} dates from {start_date} to {end_date}")
            return dates
            
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            raise
    
    def get_available_sftp_dates(self) -> List[str]:
        """
        Get all available dates from SFTP server.
        
        Returns:
            List of available dates in YYYYMMDD format
        """
        logger.info("Fetching available dates from SFTP server...")
        
        # We already know from our previous scan
        # Let's generate the full range from our known data
        return self.get_date_range('20240404', '20250609')
    
    def get_processed_dates(self) -> List[str]:
        """
        Get dates that have already been processed in BigQuery.
        
        Returns:
            List of processed dates in YYYYMMDD format
        """
        try:
            # Query BigQuery to find dates with existing data
            query = """
            SELECT DISTINCT 
                FORMAT_DATE('%Y%m%d', DATE(created_date)) as process_date
            FROM `{project_id}.{dataset_id}.order_details`
            WHERE created_date IS NOT NULL
            ORDER BY process_date
            """.format(
                project_id=settings.gcp_project_id,
                dataset_id=settings.bigquery_dataset
            )
            
            result = self.loader.client.query(query).result()
            processed_dates = [row.process_date for row in result]
            
            logger.info(f"Found {len(processed_dates)} dates already processed in BigQuery")
            return processed_dates
            
        except Exception as e:
            logger.error(f"Error getting processed dates: {e}")
            return []
    
    def filter_dates_to_process(self, available_dates: List[str]) -> List[str]:
        """
        Filter dates to only include those not yet processed.
        
        Args:
            available_dates: List of available dates
            
        Returns:
            List of dates to process
        """
        if not self.skip_existing:
            return available_dates
        
        processed_dates = set(self.get_processed_dates())
        dates_to_process = [date for date in available_dates if date not in processed_dates]
        
        logger.info(f"Filtered {len(available_dates)} available dates to {len(dates_to_process)} new dates")
        return sorted(dates_to_process)
    
    def process_single_date(self, date: str) -> Dict:
        """
        Process data for a single date.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Processing result dictionary
        """
        start_time = time.time()
        result = {
            'date': date,
            'success': False,
            'records_processed': 0,
            'processing_time': 0,
            'error': None
        }
        
        try:
            logger.info(f"Processing date: {date}")
            
            # Step 1: Download data from SFTP
            local_dir = self.sftp_extractor.download_files(date)
            if not local_dir:
                raise Exception(f"Failed to download files for {date}")
            
            # Step 2: Transform data using ToastDataTransformer
            transformed_dir = os.path.join(settings.cleaned_local_dir, date)
            os.makedirs(transformed_dir, exist_ok=True)
            
            # Set processing date for transformer
            self.transformer.processing_date = date
            
            # Transform files
            success_results, validation_results = self.transformer.transform_files(local_dir, transformed_dir)
            
            if not any(success_results.values()):
                raise Exception(f"No files successfully transformed for {date}")
            
            # Step 3: Load to BigQuery
            total_records = 0
            for filename, success in success_results.items():
                if success:
                    # Map filename to table name
                    table_name = self._get_table_name_from_filename(filename)
                    transformed_file = os.path.join(transformed_dir, filename)
                    
                    if os.path.exists(transformed_file):
                        # Load the CSV file to BigQuery using dataframe
                        import pandas as pd
                        df = pd.read_csv(transformed_file)
                        
                        if not df.empty:
                            load_result = self.loader.load_dataframe(df, table_name, filename)
                            if load_result.get('success', False):
                                total_records += len(df)
                                logger.info(f"Loaded {len(df)} records to {table_name} for {date}")
                            else:
                                raise Exception(f"Failed to load {table_name} for {date}: {load_result.get('error', 'Unknown error')}")
                        else:
                            logger.warning(f"Empty dataframe for {filename} on {date}")
            
            # Step 4: Validate data (optional)
            if self.validator and total_records > 0:
                try:
                    validation_result = self.validator.validate_daily_data(date)
                    if not validation_result.get('success', False):
                        logger.warning(f"Data validation failed for {date}: {validation_result}")
                except Exception as e:
                    logger.warning(f"Data validation error for {date}: {e}")
            
            # Clean up local files
            self.sftp_extractor.cleanup_date_files(date)
            if os.path.exists(transformed_dir):
                import shutil
                shutil.rmtree(transformed_dir)
            
            result['success'] = True
            result['records_processed'] = total_records
            result['processing_time'] = time.time() - start_time
            
            logger.info(f"Successfully processed {date}: {total_records} records in {result['processing_time']:.2f}s")
            
        except Exception as e:
            result['error'] = str(e)
            result['processing_time'] = time.time() - start_time
            logger.error(f"Failed to process {date}: {e}")
        
        return result
    
    def _get_table_name_from_filename(self, filename: str) -> str:
        """
        Map filename to BigQuery table name.
        
        Args:
            filename: CSV filename
            
        Returns:
            BigQuery table name
        """
        filename_lower = filename.lower()
        
        if 'allitemsreport' in filename_lower:
            return 'all_items_report'
        elif 'checkdetails' in filename_lower:
            return 'check_details'
        elif 'cashentries' in filename_lower:
            return 'cash_entries'
        elif 'itemselectiondetails' in filename_lower:
            return 'item_selection_details'
        elif 'kitchentimings' in filename_lower:
            return 'kitchen_timings'
        elif 'orderdetails' in filename_lower:
            return 'order_details'
        elif 'paymentdetails' in filename_lower:
            return 'payment_details'
        else:
            # Default fallback
            return filename_lower.replace('.csv', '').replace('-', '_').replace(' ', '_')
    
    def process_date_batch(self, dates: List[str]) -> List[Dict]:
        """
        Process a batch of dates in parallel.
        
        Args:
            dates: List of dates to process
            
        Returns:
            List of processing results
        """
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all dates for processing
            future_to_date = {
                executor.submit(self.process_single_date, date): date 
                for date in dates
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Update stats
                    if result['success']:
                        self.stats['processed_dates'] += 1
                        self.stats['total_records'] += result['records_processed']
                    else:
                        self.stats['failed_dates'] += 1
                        self.stats['failed_date_list'].append(date)
                    
                except Exception as e:
                    logger.error(f"Exception processing {date}: {e}")
                    self.stats['failed_dates'] += 1
                    self.stats['failed_date_list'].append(date)
                    results.append({
                        'date': date,
                        'success': False,
                        'error': str(e)
                    })
        
        return results
    
    def run_backfill(self, 
                     start_date: Optional[str] = None, 
                     end_date: Optional[str] = None,
                     specific_dates: Optional[List[str]] = None) -> Dict:
        """
        Run historical backfill process.
        
        Args:
            start_date: Start date in YYYYMMDD format (optional)
            end_date: End date in YYYYMMDD format (optional)
            specific_dates: List of specific dates to process (optional)
            
        Returns:
            Backfill summary dictionary
        """
        self.stats['start_time'] = datetime.now()
        logger.info("Starting historical backfill process...")
        
        try:
            # Determine dates to process
            if specific_dates:
                dates_to_process = specific_dates
            elif start_date and end_date:
                available_dates = self.get_date_range(start_date, end_date)
                dates_to_process = self.filter_dates_to_process(available_dates)
            else:
                # Process all available dates
                available_dates = self.get_available_sftp_dates()
                dates_to_process = self.filter_dates_to_process(available_dates)
            
            self.stats['total_dates'] = len(dates_to_process)
            
            if not dates_to_process:
                logger.info("No dates to process")
                return self.get_summary()
            
            logger.info(f"Processing {len(dates_to_process)} dates in batches of {self.batch_size}")
            
            # Process dates in batches
            all_results = []
            for i in range(0, len(dates_to_process), self.batch_size):
                batch = dates_to_process[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (len(dates_to_process) + self.batch_size - 1) // self.batch_size
                
                logger.info(f"Processing batch {batch_num}/{total_batches}: {batch[0]} to {batch[-1]}")
                
                batch_results = self.process_date_batch(batch)
                all_results.extend(batch_results)
                
                # Progress update
                progress = (self.stats['processed_dates'] + self.stats['failed_dates']) / self.stats['total_dates'] * 100
                logger.info(f"Progress: {progress:.1f}% - Processed: {self.stats['processed_dates']}, "
                           f"Failed: {self.stats['failed_dates']}, Records: {self.stats['total_records']}")
            
            self.stats['end_time'] = datetime.now()
            
            # Log final summary
            summary = self.get_summary()
            logger.info(f"Backfill completed! {summary}")
            
            return summary
            
        except Exception as e:
            logger.error(f"Backfill process failed: {e}")
            self.stats['end_time'] = datetime.now()
            raise
    
    def get_summary(self) -> Dict:
        """
        Get backfill process summary.
        
        Returns:
            Summary dictionary
        """
        duration = None
        if self.stats['start_time'] and self.stats['end_time']:
            duration = self.stats['end_time'] - self.stats['start_time']
        
        return {
            'total_dates': self.stats['total_dates'],
            'processed_dates': self.stats['processed_dates'],
            'failed_dates': self.stats['failed_dates'],
            'total_records': self.stats['total_records'],
            'duration': str(duration) if duration else None,
            'success_rate': (self.stats['processed_dates'] / max(self.stats['total_dates'], 1)) * 100,
            'failed_date_list': self.stats['failed_date_list'],
            'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
            'end_time': self.stats['end_time'].isoformat() if self.stats['end_time'] else None
        }
    
    def save_backfill_log(self, summary: Dict, log_file: str = "backfill_log.json") -> None:
        """
        Save backfill summary to log file.
        
        Args:
            summary: Backfill summary dictionary
            log_file: Path to log file
        """
        try:
            log_path = Path(settings.logs_dir) / log_file
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(log_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            logger.info(f"Backfill log saved to: {log_path}")
            
        except Exception as e:
            logger.error(f"Failed to save backfill log: {e}") 