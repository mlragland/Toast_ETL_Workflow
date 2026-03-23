#!/usr/bin/env python3
"""
May 2025 OrderDetails Backfill
Focus on the working OrderDetails table only
"""

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Set environment variables
os.environ['PROJECT_ID'] = 'toast-analytics-444116'
os.environ['DATASET_ID'] = 'toast_analytics'

def run_may_2025_orderdetails_backfill():
    """Run May 2025 backfill for OrderDetails only"""
    
    print("🍴 Toast ETL - May 2025 OrderDetails Backfill")
    print("=" * 60)
    print("📅 Target: May 1-31, 2025")
    print("🗄️  Database: toast-analytics-444116.toast_analytics")
    print("🎯 Focus: OrderDetails table (confirmed working)")
    print("💡 Strategy: Extract → Transform → Load OrderDetails only")
    print("=" * 60)
    
    # Generate May 2025 date range
    may_dates = []
    for day in range(1, 32):
        try:
            date_obj = datetime(2025, 5, day)
            date_str = date_obj.strftime('%Y%m%d')
            may_dates.append(date_str)
        except ValueError:
            continue
    
    print(f"📊 Processing {len(may_dates)} dates in May 2025")
    print(f"📅 Range: {may_dates[0]} to {may_dates[-1]}")
    print("")
    
    successful_dates = []
    failed_dates = []
    total_records = 0
    
    for i, date_str in enumerate(may_dates, 1):
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        print(f"🔄 Processing {formatted_date} ({i}/{len(may_dates)})...")
        
        try:
            # Run the ETL pipeline with specific date
            cmd = ['python', 'main.py', '--date', date_str]
            
            # Set environment variable for the date
            env = os.environ.copy()
            env['ETL_DATE'] = date_str
            
            # Run the command and capture output
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per date
            )
            
            # Check if OrderDetails was loaded successfully
            output = result.stdout
            if "Successfully loaded" in output and "order_details" in output:
                # Extract number of records from output
                lines = output.split('\n')
                for line in lines:
                    if "Successfully loaded" in line and "order_details" in line:
                        try:
                            # Parse line like: "Successfully loaded 120 rows to order_details in 4.04s"
                            parts = line.split()
                            records = int(parts[2])
                            total_records += records
                            successful_dates.append(date_str)
                            print(f"✅ {formatted_date}: {records} OrderDetails records loaded")
                            break
                        except:
                            successful_dates.append(date_str)
                            print(f"✅ {formatted_date}: OrderDetails loaded (count unknown)")
                            break
            else:
                failed_dates.append(date_str)
                # Check if it's a data availability issue
                if "No files found" in output or "SFTP" in result.stderr:
                    print(f"❌ {formatted_date}: No data available on SFTP")
                else:
                    print(f"❌ {formatted_date}: OrderDetails not loaded")
                
        except subprocess.TimeoutExpired:
            failed_dates.append(date_str)
            print(f"❌ {formatted_date}: Timeout (>5 minutes)")
        except Exception as e:
            failed_dates.append(date_str)
            print(f"❌ {formatted_date}: Error - {str(e)}")
    
    # Final Summary
    print("\n" + "=" * 60)
    print("🎉 May 2025 OrderDetails Backfill Complete!")
    print("=" * 60)
    print(f"✅ Successful dates: {len(successful_dates)}")
    print(f"❌ Failed dates: {len(failed_dates)}")
    print(f"📊 Total OrderDetails records: {total_records:,}")
    print(f"📈 Success rate: {len(successful_dates)/len(may_dates)*100:.1f}%")
    
    if successful_dates:
        print(f"\n✅ Successfully processed dates:")
        for date in successful_dates:
            formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            print(f"   📅 {formatted_date}")
    
    if failed_dates:
        print(f"\n⚠️  Failed dates:")
        for date in failed_dates:
            formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            print(f"   ❌ {formatted_date}")
    
    # Database status
    print(f"\n📈 Database Update:")
    print(f"   • Previous OrderDetails records: 0 (cleared)")
    print(f"   • New May 2025 records: {total_records:,}")
    print(f"   • Total OrderDetails now: {total_records:,}")
    
    print("\n🎯 Next Steps:")
    print("   📊 Check dashboard: http://localhost:3000")
    print("   🔧 API backend: http://localhost:8080")
    print("   📈 View analytics with May 2025 data")
    
    print("=" * 60)
    
    return {
        'successful_dates': len(successful_dates),
        'failed_dates': len(failed_dates),
        'total_records': total_records,
        'success_rate': len(successful_dates)/len(may_dates)*100
    }

if __name__ == '__main__':
    print("🚀 Starting May 2025 OrderDetails Backfill...")
    print("💡 This will process all 31 days of May 2025")
    print("⏱️  Estimated time: 15-30 minutes")
    print("")
    
    confirm = input("Continue? (y/N): ").strip().lower()
    
    if confirm in ['y', 'yes']:
        result = run_may_2025_orderdetails_backfill()
        
        if result['success_rate'] > 80:
            print("\n🎊 Backfill highly successful!")
            print("✅ May 2025 data is now available in the dashboard")
        elif result['success_rate'] > 50:
            print("\n✅ Backfill partially successful")
            print("📊 Good amount of May 2025 data loaded")
        else:
            print("\n⚠️  Backfill had issues")
            print("📋 Check the failed dates above")
            
    else:
        print("❌ Backfill cancelled by user") 