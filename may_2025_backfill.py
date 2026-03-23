#!/usr/bin/env python3
"""
May 2025 Data Backfill for Toast ETL Pipeline
Simplified script to upload May 2025 data only
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Set environment variables
os.environ['PROJECT_ID'] = 'toast-analytics-444116'
os.environ['DATASET_ID'] = 'toast_analytics'

def run_may_2025_backfill():
    """Run backfill for May 2025 data only"""
    
    print("🍴 Toast ETL - May 2025 Data Backfill")
    print("=" * 50)
    print("📅 Target: May 1-31, 2025")
    print("🗄️  Database: toast-analytics-444116.toast_analytics")
    print("🎯 Goal: Upload May 2025 data only")
    print("=" * 50)
    
    # Generate May 2025 date range
    may_dates = []
    for day in range(1, 32):
        try:
            date_obj = datetime(2025, 5, day)
            date_str = date_obj.strftime('%Y%m%d')
            may_dates.append(date_str)
        except ValueError:
            # Skip invalid dates (like May 31st doesn't exist in some contexts)
            continue
    
    print(f"📊 Processing {len(may_dates)} dates in May 2025")
    print(f"📅 Range: {may_dates[0]} to {may_dates[-1]}")
    print("")
    
    # Try to import and use the existing ETL pipeline
    try:
        # Add the src directory to Python path
        src_path = Path(__file__).parent / 'src'
        sys.path.insert(0, str(src_path))
        
        # Import the main ETL function
        from main import main as run_etl
        
        successful_dates = []
        failed_dates = []
        total_records = 0
        
        for i, date_str in enumerate(may_dates, 1):
            print(f"🔄 Processing {date_str} ({i}/{len(may_dates)})...")
            
            try:
                # Set the date for ETL processing
                os.environ['ETL_DATE'] = date_str
                
                # Run ETL for this specific date
                result = run_etl()
                
                if result and result.get('success', False):
                    records = result.get('records_processed', 0)
                    total_records += records
                    successful_dates.append(date_str)
                    print(f"✅ {date_str}: {records} records processed")
                else:
                    failed_dates.append(date_str)
                    print(f"❌ {date_str}: Processing failed")
                    
            except Exception as e:
                failed_dates.append(date_str)
                print(f"❌ {date_str}: Error - {str(e)}")
        
        # Summary
        print("\n" + "=" * 50)
        print("🎉 May 2025 Backfill Complete!")
        print("=" * 50)
        print(f"✅ Successful dates: {len(successful_dates)}")
        print(f"❌ Failed dates: {len(failed_dates)}")
        print(f"📊 Total records: {total_records:,}")
        print(f"📈 Success rate: {len(successful_dates)/len(may_dates)*100:.1f}%")
        
        if failed_dates:
            print(f"\n⚠️  Failed dates:")
            for date in failed_dates:
                print(f"   • {date}")
        
        print("=" * 50)
        
    except ImportError as e:
        print(f"❌ Could not import ETL pipeline: {e}")
        print("💡 Trying alternative approach...")
        
        # Alternative: Use direct BigQuery approach
        try_direct_approach(may_dates)

def try_direct_approach(may_dates):
    """Try a more direct approach using existing pipeline components"""
    
    print("\n🔄 Trying direct ETL approach...")
    
    # This would be a simplified version that directly processes files
    # For now, let's just show what we would do
    
    print("📋 May 2025 dates to process:")
    for date in may_dates:
        # Convert YYYYMMDD to YYYY-MM-DD for display
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        print(f"   📅 {formatted_date}")
    
    print("\n💡 To complete the backfill:")
    print("1. Ensure SFTP credentials are configured")
    print("2. Run the ETL pipeline for each date")
    print("3. Monitor BigQuery for data ingestion")
    
    print("\n🚀 You can run individual dates using:")
    print("   python main.py --date YYYYMMDD")
    
    print("\n📊 After completion, check the dashboard at:")
    print("   http://localhost:3000 (React frontend)")
    print("   http://localhost:8080 (API backend)")

if __name__ == '__main__':
    run_may_2025_backfill() 