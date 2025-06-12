"""
Business Calendar for Toast ETL Pipeline.

Handles business closure detection and zero-record generation for consistent reporting.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class BusinessCalendar:
    """
    Manages business operating calendar and closure detection.
    
    Provides functionality to:
    - Detect business closure dates based on record counts
    - Generate zero records for closed dates
    - Maintain reporting consistency
    """
    
    def __init__(self, 
                 min_records_threshold: int = 10,
                 min_files_threshold: int = 4,
                 min_sales_threshold: float = 50.0):
        """
        Initialize the Business Calendar.
        
        Args:
            min_records_threshold: Minimum total records across all files to consider open
            min_files_threshold: Minimum number of files to consider open
            min_sales_threshold: Minimum total sales amount to consider open
        """
        self.min_records_threshold = min_records_threshold
        self.min_files_threshold = min_files_threshold
        self.min_sales_threshold = min_sales_threshold
        
        self.closure_reasons = {
            'low_activity': 'Business closed - minimal activity detected',
            'no_files': 'Business closed - no data files found',
            'no_sales': 'Business closed - no sales activity',
            'maintenance': 'Business closed for maintenance',
            'emergency': 'Business closed due to emergency'
        }
    
    def is_likely_closure_date(self, date: str, file_analysis: Dict) -> tuple[bool, str]:
        """
        Determine if a date is likely a business closure based on data volume.
        
        Args:
            date: Date in YYYYMMDD format
            file_analysis: Analysis of files for this date containing:
                - total_records: Total records across all files
                - files_found: Number of CSV files found
                - total_sales: Total sales amount (if available)
                - has_meaningful_data: Boolean indicating substantial data
            
        Returns:
            Tuple of (is_closure, reason)
        """
        total_records = file_analysis.get('total_records', 0)
        files_found = file_analysis.get('files_found', 0)
        total_sales = file_analysis.get('total_sales', 0.0)
        has_meaningful_data = file_analysis.get('has_meaningful_data', False)
        
        # Check if no files found at all
        if files_found == 0:
            logger.info(f"Date {date}: No files found - likely closure")
            return True, 'no_files'
        
        # Check if files exist but very few records
        if total_records < self.min_records_threshold:
            logger.info(f"Date {date}: Only {total_records} records found (< {self.min_records_threshold}) - likely closure")
            return True, 'low_activity'
        
        # Check if insufficient number of files (missing key data files)
        if files_found < self.min_files_threshold:
            logger.info(f"Date {date}: Only {files_found} files found (< {self.min_files_threshold}) - likely closure")
            return True, 'low_activity'
        
        # Check if sales data is available and extremely low
        if total_sales > 0 and total_sales < self.min_sales_threshold:
            logger.info(f"Date {date}: Only ${total_sales:.2f} in sales (< ${self.min_sales_threshold}) - likely closure")
            return True, 'no_sales'
        
        # Check the meaningful data flag
        if not has_meaningful_data:
            logger.info(f"Date {date}: No meaningful data detected - likely closure")
            return True, 'low_activity'
        
        # If we get here, it's likely a normal business day
        logger.info(f"Date {date}: {total_records} records, {files_found} files - normal business day")
        return False, ''
    
    def analyze_file_data(self, date: str, file_paths: Dict[str, str]) -> Dict:
        """
        Analyze actual file data to determine business activity level.
        
        Args:
            date: Date in YYYYMMDD format
            file_paths: Dictionary mapping filename to file path
            
        Returns:
            Analysis dictionary with metrics
        """
        analysis = {
            'date': date,
            'total_records': 0,
            'files_found': 0,
            'total_sales': 0.0,
            'has_meaningful_data': False,
            'file_details': {}
        }
        
        for filename, filepath in file_paths.items():
            try:
                if not filepath or not Path(filepath).exists():
                    continue
                
                df = pd.read_csv(filepath)
                record_count = len(df)
                analysis['total_records'] += record_count
                analysis['files_found'] += 1
                
                # Track per-file details
                analysis['file_details'][filename] = {
                    'records': record_count,
                    'size_bytes': Path(filepath).stat().st_size if Path(filepath).exists() else 0
                }
                
                # Extract sales data if available
                if 'total' in df.columns and not df['total'].empty:
                    try:
                        sales = pd.to_numeric(df['total'], errors='coerce').sum()
                        if not pd.isna(sales):
                            analysis['total_sales'] += sales
                    except:
                        pass
                
                # Check for meaningful data (more than just headers)
                if record_count > 1:  # More than just header row
                    analysis['has_meaningful_data'] = True
                    
            except Exception as e:
                logger.warning(f"Error analyzing file {filename}: {e}")
                continue
        
        # Final determination of meaningful data
        if (analysis['total_records'] >= self.min_records_threshold and 
            analysis['files_found'] >= self.min_files_threshold):
            analysis['has_meaningful_data'] = True
        
        logger.info(f"File analysis for {date}: {analysis['total_records']} records, "
                   f"{analysis['files_found']} files, ${analysis['total_sales']:.2f} sales")
        
        return analysis
    
    def generate_closure_records(self, date: str, closure_reason: str) -> Dict[str, pd.DataFrame]:
        """
        Generate zero records for business closure date.
        
        Args:
            date: Date in YYYYMMDD format
            closure_reason: Reason for closure
            
        Returns:
            Dictionary of DataFrames with zero records for each table
        """
        processing_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        closure_note = self.closure_reasons.get(closure_reason, 'Business closed')
        
        # Generate zero records for each table
        zero_records = {}
        
        # All Items Report - Single row indicating no items sold
        zero_records['all_items_report'] = pd.DataFrame({
            'master_id': ['CLOSURE_RECORD'],
            'item_id': ['CLOSURE'],
            'menu_item': [f'Business Closed - {closure_note}'],
            'item_qty': [0],
            'net_amount': [0.0],
            'processing_date': [processing_date],
            'closure_indicator': [True],
            'closure_reason': [closure_reason]
        })
        
        # Check Details - Single row indicating no checks
        zero_records['check_details'] = pd.DataFrame({
            'check_id': ['CLOSURE_RECORD'],
            'customer': [f'Business Closed'],
            'total': [0.0],
            'opened_date': [processing_date],
            'processing_date': [processing_date],
            'closure_indicator': [True],
            'closure_reason': [closure_reason]
        })
        
        # Cash Entries - Single entry indicating closure
        zero_records['cash_entries'] = pd.DataFrame({
            'entry_id': ['CLOSURE_RECORD'],
            'action': ['Business Closed'],
            'amount': [0.0],
            'created_date': [processing_date],
            'processing_date': [processing_date],
            'closure_indicator': [True],
            'closure_reason': [closure_reason]
        })
        
        # Item Selection Details - No items selected
        zero_records['item_selection_details'] = pd.DataFrame({
            'order_id': ['CLOSURE_RECORD'],
            'item_selection_id': ['CLOSURE'],
            'menu_item': [f'Business Closed - {closure_note}'],
            'quantity': [0],
            'net_price': [0.0],
            'processing_date': [processing_date],
            'closure_indicator': [True],
            'closure_reason': [closure_reason]
        })
        
        # Kitchen Timings - No kitchen activity
        zero_records['kitchen_timings'] = pd.DataFrame({
            'id': ['CLOSURE_RECORD'],
            'check_number': ['CLOSURE'],
            'station': ['Business Closed'],
            'fulfillment_time': [0],
            'processing_date': [processing_date],
            'closure_indicator': [True],
            'closure_reason': [closure_reason]
        })
        
        # Order Details - No orders
        zero_records['order_details'] = pd.DataFrame({
            'order_id': ['CLOSURE_RECORD'],
            'location': ['Business Closed'],
            'total': [0.0],
            'opened': [processing_date],
            'processing_date': [processing_date],
            'closure_indicator': [True],
            'closure_reason': [closure_reason]
        })
        
        # Payment Details - No payments
        zero_records['payment_details'] = pd.DataFrame({
            'payment_id': ['CLOSURE_RECORD'],
            'order_id': ['CLOSURE_RECORD'],
            'amount': [0.0],
            'processing_date': [processing_date],
            'closure_indicator': [True],
            'closure_reason': [closure_reason]
        })
        
        logger.info(f"Generated closure records for {date}: {closure_reason}")
        return zero_records
    
    def should_process_as_closure(self, date: str, file_analysis: Dict) -> tuple[bool, str, Dict]:
        """
        Determine if date should be processed as business closure.
        
        Args:
            date: Date in YYYYMMDD format
            file_analysis: File analysis results from analyze_file_data()
            
        Returns:
            Tuple of (should_process_as_closure, reason, closure_records)
        """
        is_closure, reason = self.is_likely_closure_date(date, file_analysis)
        
        if is_closure:
            closure_records = self.generate_closure_records(date, reason)
            return True, reason, closure_records
        
        return False, '', {}
    
    def get_business_metrics_query_filter(self) -> str:
        """
        Get SQL filter to exclude closure records from business metrics.
        
        Returns:
            SQL WHERE clause to filter out closure records
        """
        return "WHERE (closure_indicator IS NULL OR closure_indicator = FALSE)"
    
    def get_closure_summary_query(self, project_id: str, dataset_id: str) -> str:
        """
        Get query to summarize business closures.
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            
        Returns:
            SQL query for closure summary
        """
        return f"""
        SELECT 
            processing_date,
            closure_reason,
            COUNT(*) as closure_records
        FROM `{project_id}.{dataset_id}.order_details`
        WHERE closure_indicator = TRUE
        GROUP BY processing_date, closure_reason
        ORDER BY processing_date DESC
        """
    
    def update_thresholds(self, 
                         min_records: Optional[int] = None,
                         min_files: Optional[int] = None, 
                         min_sales: Optional[float] = None):
        """
        Update closure detection thresholds.
        
        Args:
            min_records: New minimum records threshold
            min_files: New minimum files threshold
            min_sales: New minimum sales threshold
        """
        if min_records is not None:
            self.min_records_threshold = min_records
            logger.info(f"Updated min_records_threshold to {min_records}")
        
        if min_files is not None:
            self.min_files_threshold = min_files
            logger.info(f"Updated min_files_threshold to {min_files}")
            
        if min_sales is not None:
            self.min_sales_threshold = min_sales
            logger.info(f"Updated min_sales_threshold to ${min_sales}")
    
    def get_threshold_summary(self) -> Dict:
        """
        Get current threshold configuration.
        
        Returns:
            Dictionary of current thresholds
        """
        return {
            'min_records_threshold': self.min_records_threshold,
            'min_files_threshold': self.min_files_threshold,
            'min_sales_threshold': self.min_sales_threshold,
            'closure_reasons': list(self.closure_reasons.keys())
        }