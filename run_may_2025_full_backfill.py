#!/usr/bin/env python3
"""
May 2025 Full Backfill Script

Runs the complete ETL pipeline for all 31 days of May 2025 with all 7 tables,
using the corrected schemas that match the legacy implementation.
"""

import os
import sys
import subprocess
import time
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('may_2025_full_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_etl_for_date(date_str):
    """
    Run ETL pipeline for a specific date.
    
    Args:
        date_str: Date in YYYYMMDD format
        
    Returns:
        Tuple of (success, results_dict)
    """
    logger.info(f"🔄 Processing date: {date_str}")
    
    try:
        # Set environment variables
        env = os.environ.copy()
        env['PROJECT_ID'] = 'toast-analytics-444116'
        env['DATASET_ID'] = 'toast_analytics'
        env['ENVIRONMENT'] = 'development'
        
        # Run main ETL pipeline
        cmd = [sys.executable, 'main.py', '--date', date_str]
        
        start_time = time.time()
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout per date
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            # Parse output for record counts
            output_lines = result.stdout.split('\n')
            
            # Look for loading success messages
            loaded_tables = {}
            for line in output_lines:
                if 'Successfully loaded' in line and 'rows to' in line:
                    # Extract table name and row count
                    parts = line.split()
                    if len(parts) >= 4:
                        try:
                            row_count = int(parts[2])
                            table_name = parts[-1].split('.')[-1]  # Get table name after last dot
                            loaded_tables[table_name] = row_count
                        except (ValueError, IndexError):
                            continue
            
            logger.info(f"✅ {date_str} completed in {duration:.1f}s")
            for table, count in loaded_tables.items():
                logger.info(f"   📊 {table}: {count} rows")
            
            return True, {
                'date': date_str,
                'duration': duration,
                'tables': loaded_tables,
                'total_rows': sum(loaded_tables.values())
            }
        else:
            logger.error(f"❌ {date_str} failed (exit code: {result.returncode})")
            logger.error(f"   Error: {result.stderr}")
            return False, {
                'date': date_str,
                'duration': duration,
                'error': result.stderr,
                'tables': {}
            }
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {date_str} timed out after 10 minutes")
        return False, {
            'date': date_str,
            'duration': 600,
            'error': 'Timeout',
            'tables': {}
        }
    except Exception as e:
        logger.error(f"💥 {date_str} failed with exception: {e}")
        return False, {
            'date': date_str,
            'duration': 0,
            'error': str(e),
            'tables': {}
        }

def generate_may_2025_dates():
    """Generate all dates in May 2025 in YYYYMMDD format."""
    dates = []
    for day in range(1, 32):  # May has 31 days
        date_str = f"2025050{day:02d}" if day < 10 else f"20250{day}"
        dates.append(date_str)
    return dates

def main():
    """Main backfill execution."""
    logger.info("🍴 Starting May 2025 Full Backfill")
    logger.info("=" * 60)
    logger.info("📅 Processing all 31 days of May 2025")
    logger.info("📊 All 7 tables with corrected schemas")
    logger.info("🔧 Schema fixes applied for BigQuery compatibility")
    logger.info("=" * 60)
    
    # Check if schema updates have been applied
    logger.info("⚠️  IMPORTANT: Ensure BigQuery schemas have been updated!")
    logger.info("   Run: python update_bigquery_schemas.py")
    
    response = input("Have you updated the BigQuery schemas? (y/N): ")
    if response.lower() != 'y':
        logger.warning("❌ Please update BigQuery schemas first")
        logger.info("   Run: python update_bigquery_schemas.py")
        return False
    
    # Generate date list
    dates = generate_may_2025_dates()
    logger.info(f"📋 Generated {len(dates)} dates to process")
    
    # Track results
    successful_dates = []
    failed_dates = []
    all_results = []
    total_rows_loaded = 0
    
    start_time = time.time()
    
    # Process each date
    for i, date_str in enumerate(dates, 1):
        logger.info(f"\n📅 Processing {i}/{len(dates)}: {date_str}")
        
        success, result = run_etl_for_date(date_str)
        all_results.append(result)
        
        if success:
            successful_dates.append(date_str)
            total_rows_loaded += result.get('total_rows', 0)
        else:
            failed_dates.append(date_str)
        
        # Progress update every 5 dates
        if i % 5 == 0:
            elapsed = time.time() - start_time
            avg_time = elapsed / i
            remaining = (len(dates) - i) * avg_time
            logger.info(f"📊 Progress: {i}/{len(dates)} ({i/len(dates)*100:.1f}%)")
            logger.info(f"⏱️  Estimated remaining: {remaining/60:.1f} minutes")
    
    # Final summary
    total_duration = time.time() - start_time
    
    logger.info("\n" + "=" * 60)
    logger.info("🎯 MAY 2025 BACKFILL SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Successful dates: {len(successful_dates)}/{len(dates)}")
    logger.info(f"❌ Failed dates: {len(failed_dates)}")
    logger.info(f"📊 Total rows loaded: {total_rows_loaded:,}")
    logger.info(f"⏱️  Total duration: {total_duration/60:.1f} minutes")
    logger.info(f"📈 Average per date: {total_duration/len(dates):.1f} seconds")
    
    if failed_dates:
        logger.info(f"\n❌ Failed dates:")
        for date in failed_dates:
            logger.info(f"   - {date}")
    
    # Table-wise summary
    table_totals = {}
    for result in all_results:
        for table, count in result.get('tables', {}).items():
            table_totals[table] = table_totals.get(table, 0) + count
    
    if table_totals:
        logger.info(f"\n📊 Table Summary:")
        for table, total in sorted(table_totals.items()):
            logger.info(f"   📋 {table}: {total:,} rows")
    
    # Success criteria
    success_rate = len(successful_dates) / len(dates)
    if success_rate >= 0.9:  # 90% success rate
        logger.info(f"\n🎉 Backfill completed successfully!")
        logger.info(f"   Success rate: {success_rate*100:.1f}%")
        logger.info(f"   May 2025 data is now available in BigQuery")
        return True
    else:
        logger.warning(f"\n⚠️  Backfill completed with issues")
        logger.info(f"   Success rate: {success_rate*100:.1f}%")
        logger.info(f"   Consider re-running failed dates")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 