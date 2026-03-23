#!/usr/bin/env python3
"""
Automated backfill script for Toast ETL data.
This script processes data for dates from 2025-06-10 to 2025-01-01 in reverse chronological order.
It runs extract and transform for each date, then calls process_gcs_staging.py to stage and load to BigQuery via GCS.
"""
import os
import sys
import subprocess
import logging
import json
from datetime import datetime, timedelta
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('backfill.log')
    ]
)
logger = logging.getLogger(__name__)

def verify_load(date_str):
    """Verify that data for the given date has been loaded into BigQuery."""
    tables = [
        'check_details',
        'all_items_report',
        'cash_entries',
        'item_selection_details',
        'kitchen_timings',
        'order_details',
        'payment_details'
    ]
    
    results = {}
    for table in tables:
        query = f"""
        SELECT COUNT(*) as count
        FROM `toast-analytics-444116.toast_analytics.{table}`
        WHERE processing_date = '{date_str}'
        """
        try:
            result = subprocess.run(
                ["bq", "query", "--nouse_legacy_sql", "--format=prettyjson", query],
                capture_output=True,
                text=True,
                check=True
            )
            # Parse the JSON output
            data = json.loads(result.stdout)
            count = int(data[0]['count'])
            results[table] = count
            logger.info(f"Found {count} records in {table} for {date_str}")
        except Exception as e:
            logger.error(f"Verification failed for {table} on {date_str}: {e}")
            results[table] = 0
    
    return results

def main():
    start_date = datetime(2024, 12, 31)
    end_date = datetime(2024, 4, 6)
    current_date = start_date
    
    # Track results
    failed_dates = defaultdict(list)
    successful_dates = []
    
    while current_date >= end_date:
        date_str = current_date.strftime("%Y%m%d")
        processing_date = current_date.strftime("%Y-%m-%d")
        logger.info(f"Processing date: {date_str}")
        
        # Step 1: Extract only
        extract_cmd = ["python", "main.py", "--date", date_str, "--extract-only"]
        try:
            subprocess.run(extract_cmd, check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Extraction failed for {date_str}: {e}")
            failed_dates[date_str].append("extraction")
            current_date -= timedelta(days=1)
            continue
            
        # Step 2: Transform only
        transform_cmd = ["python", "main.py", "--date", date_str, "--transform-only"]
        try:
            subprocess.run(transform_cmd, check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Transformation failed for {date_str}: {e}")
            failed_dates[date_str].append("transformation")
            current_date -= timedelta(days=1)
            continue
            
        # Step 3: Stage and load via GCS
        gcs_stage_cmd = ["python", "process_gcs_staging.py", "--date", date_str]
        try:
            subprocess.run(gcs_stage_cmd, check=True)
            # Verify the load
            table_results = verify_load(processing_date)
            
            # Check each table's results
            all_tables_loaded = True
            for table, count in table_results.items():
                if count == 0:
                    failed_dates[date_str].append(f"{table}_empty")
                    all_tables_loaded = False
            
            if all_tables_loaded:
                successful_dates.append(date_str)
                logger.info(f"✅ All tables for {date_str} verified successfully.")
            else:
                logger.warning(f"⚠️ Some tables failed verification for {date_str}")
                
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ GCS staging/load failed for {date_str}: {e}")
            failed_dates[date_str].append("gcs_staging")
            
        current_date -= timedelta(days=1)
    
    # Print final summary
    logger.info("\n" + "=" * 50)
    logger.info("🎉 Backfill Summary")
    logger.info("=" * 50)
    logger.info(f"✅ Successful dates: {len(successful_dates)}")
    logger.info(f"❌ Failed dates: {len(failed_dates)}")
    
    if failed_dates:
        logger.info("\nFailed dates and reasons:")
        for date, failures in failed_dates.items():
            logger.info(f"  • {date}: {', '.join(failures)}")
    
    # Save results to JSON file
    results = {
        'successful_dates': successful_dates,
        'failed_dates': dict(failed_dates),
        'total_dates': len(successful_dates) + len(failed_dates),
        'success_rate': len(successful_dates) / (len(successful_dates) + len(failed_dates)) * 100
    }
    
    with open('backfill_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info("\nResults saved to backfill_results.json")

if __name__ == "__main__":
    main() 