#!/usr/bin/env python3
"""
Process Toast ETL data for a given date using GCS staging approach to avoid PyArrow conversion issues.

Usage:
    python process_gcs_staging.py --date YYYYMMDD
    (defaults to yesterday if --date not provided)
"""
import os
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
import argparse

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.loaders.gcs_bigquery_loader import GCSBigQueryLoader
from src.utils.logging_utils import get_logger

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = get_logger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Process Toast ETL data for a given date using GCS staging.")
    parser.add_argument('--date', type=str, help='Date to process in YYYYMMDD format (default: yesterday)')
    args = parser.parse_args()
    if args.date:
        try:
            dt = datetime.strptime(args.date, "%Y%m%d")
        except ValueError:
            print(f"❌ Invalid date format: {args.date}. Use YYYYMMDD.")
            sys.exit(1)
    else:
        dt = datetime.now() - timedelta(days=1)
    date_str = dt.strftime("%Y%m%d")
    processing_date = dt.strftime("%Y-%m-%d")
    return date_str, processing_date

def main():
    date, processing_date = parse_args()
    print(f"🍴 Toast ETL - GCS Staging Solution for {date}")
    print("=" * 60)
    print("📋 Strategy: Upload cleaned CSV files to GCS → Load to BigQuery")
    print("🎯 Goal: Avoid PyArrow conversion issues")
    print("=" * 60)

    # Check if we have cleaned files available
    cleaned_dir = f"/tmp/toast_raw_data/raw/cleaned/{date}"
    if not os.path.exists(cleaned_dir):
        print(f"❌ Cleaned files directory not found: {cleaned_dir}")
        print("Please run the transformation step first")
        sys.exit(1)
    # Get list of cleaned files
    csv_files = [f for f in os.listdir(cleaned_dir) if f.endswith('_cleaned.csv')]
    if not csv_files:
        print(f"❌ No cleaned CSV files found in: {cleaned_dir}")
        sys.exit(1)
    print(f"📂 Found {len(csv_files)} cleaned files:")
    for file in sorted(csv_files):
        print(f"   • {file}")
    print("\n🔄 Initializing GCS-BigQuery loader...")
    # Initialize the GCS staging loader
    loader = GCSBigQueryLoader(
        project_id="toast-analytics-444116",
        dataset_id="toast_analytics",
        bucket_name="toast-raw-data"
    )
    # File to table mapping
    file_table_mapping = {
        "AllItemsReport_cleaned.csv": "all_items_report",
        "CheckDetails_cleaned.csv": "check_details",
        "CashEntries_cleaned.csv": "cash_entries",
        "ItemSelectionDetails_cleaned.csv": "item_selection_details",
        "KitchenTimings_cleaned.csv": "kitchen_timings",
        "OrderDetails_cleaned.csv": "order_details",
        "PaymentDetails_cleaned.csv": "payment_details"
    }
    print("\n🚀 Starting GCS staging loads...")
    print("=" * 60)
    results = {}
    total_records = 0
    for csv_file in sorted(csv_files):
        if csv_file not in file_table_mapping:
            print(f"⚠️  Skipping unknown file: {csv_file}")
            continue
        table_name = file_table_mapping[csv_file]
        csv_file_path = os.path.join(cleaned_dir, csv_file)
        print(f"\n📊 Processing: {csv_file} → {table_name}")
        try:
            # Load via GCS staging
            result = loader.load_csv_file(
                csv_file_path=csv_file_path,
                table_name=table_name,
                processing_date=processing_date
            )
            results[csv_file] = result
            if result.get('success', False):
                rows_loaded = result.get('rows_loaded', 0)
                total_records += rows_loaded
                print(f"   ✅ Success: {rows_loaded:,} rows loaded")
            else:
                error = result.get('error', 'Unknown error')
                print(f"   ❌ Failed: {error}")
        except Exception as e:
            print(f"   ❌ Exception: {e}")
            results[csv_file] = {'success': False, 'error': str(e)}
    # Summary
    print("\n" + "=" * 60)
    print("📊 GCS STAGING RESULTS SUMMARY")
    print("=" * 60)
    successful_loads = [f for f, r in results.items() if r.get('success', False)]
    failed_loads = [f for f, r in results.items() if not r.get('success', False)]
    print(f"✅ Successful loads: {len(successful_loads)}")
    print(f"❌ Failed loads: {len(failed_loads)}")
    print(f"📊 Total records loaded: {total_records:,}")
    if successful_loads:
        print(f"\n✅ Successfully loaded tables:")
        for file in successful_loads:
            table_name = file_table_mapping[file]
            rows = results[file].get('rows_loaded', 0)
            print(f"   • {table_name}: {rows:,} rows")
    if failed_loads:
        print(f"\n❌ Failed to load:")
        for file in failed_loads:
            table_name = file_table_mapping[file]
            error = results[file].get('error', 'Unknown error')
            print(f"   • {table_name}: {error}")
    # Overall status
    success_rate = len(successful_loads) / len(results) * 100 if results else 0
    if success_rate == 100:
        print(f"\n🎉 ALL LOADS SUCCESSFUL!")
        print(f"🔗 Data available in BigQuery: toast-analytics-444116.toast_analytics")
        print(f"📅 Processing date: {processing_date}")
    elif success_rate >= 75:
        print(f"\n✅ MOSTLY SUCCESSFUL ({success_rate:.1f}% success rate)")
        print(f"🔄 Consider retrying failed loads")
    else:
        print(f"\n⚠️  MIXED RESULTS ({success_rate:.1f}% success rate)")
        print(f"🔍 Review errors and configuration")
    return success_rate == 100

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 