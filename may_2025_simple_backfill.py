#!/usr/bin/env python3
"""
May 2025 Data Backfill - Simplified Version
Focus on OrderDetails table only (which is working)
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# Set environment variables
os.environ['PROJECT_ID'] = 'toast-analytics-444116'
os.environ['DATASET_ID'] = 'toast_analytics'

def run_may_2025_simple_backfill():
    """Run simplified backfill for May 2025 OrderDetails only"""
    
    print("🍴 Toast ETL - May 2025 Simple Backfill")
    print("=" * 50)
    print("📅 Target: May 1-31, 2025")
    print("🗄️  Database: toast-analytics-444116.toast_analytics")
    print("🎯 Focus: OrderDetails table only (working)")
    print("=" * 50)
    
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
        print(f"🔄 Processing {date_str} ({i}/{len(may_dates)})...")
        
        try:
            # Run the ETL pipeline with specific date
            cmd = [
                'python', 'main.py',
                '--date', date_str,
                '--extract-only', 'false',
                '--transform-only', 'false', 
                '--load-only', 'false'
            ]
            
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
            
            if result.returncode == 0:
                # Parse the output to find how many records were loaded
                output = result.stdout
                if "Successfully loaded" in output and "order_details" in output:
                    # Extract number of records from output
                    lines = output.split('\n')
                    for line in lines:
                        if "Successfully loaded" in line and "order_details" in line:
                            try:
                                records = int(line.split()[2])
                                total_records += records
                                successful_dates.append(date_str)
                                print(f"✅ {date_str}: {records} records loaded")
                                break
                            except:
                                successful_dates.append(date_str)
                                print(f"✅ {date_str}: Records loaded (count unknown)")
                                break
                else:
                    failed_dates.append(date_str)
                    print(f"❌ {date_str}: No order_details loaded")
            else:
                failed_dates.append(date_str)
                print(f"❌ {date_str}: ETL failed - {result.stderr[:100]}...")
                
        except subprocess.TimeoutExpired:
            failed_dates.append(date_str)
            print(f"❌ {date_str}: Timeout (>5 minutes)")
        except Exception as e:
            failed_dates.append(date_str)
            print(f"❌ {date_str}: Error - {str(e)}")
    
    # Summary
    print("\n" + "=" * 50)
    print("🎉 May 2025 Simple Backfill Complete!")
    print("=" * 50)
    print(f"✅ Successful dates: {len(successful_dates)}")
    print(f"❌ Failed dates: {len(failed_dates)}")
    print(f"📊 Total OrderDetails records: {total_records:,}")
    print(f"📈 Success rate: {len(successful_dates)/len(may_dates)*100:.1f}%")
    
    if successful_dates:
        print(f"\n✅ Successfully processed dates:")
        for date in successful_dates[:10]:  # Show first 10
            formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            print(f"   📅 {formatted_date}")
        if len(successful_dates) > 10:
            print(f"   ... and {len(successful_dates) - 10} more")
    
    if failed_dates:
        print(f"\n⚠️  Failed dates:")
        for date in failed_dates[:10]:  # Show first 10
            formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            print(f"   ❌ {formatted_date}")
        if len(failed_dates) > 10:
            print(f"   ... and {len(failed_dates) - 10} more")
    
    print("=" * 50)
    print("💡 Note: This focused on OrderDetails only")
    print("📊 Check dashboard at http://localhost:3000")
    print("🔧 Backend API at http://localhost:8080")
    print("=" * 50)

def run_single_date_test():
    """Test with a single May date first"""
    
    print("🧪 Testing single May 2025 date first...")
    test_date = "20250501"  # May 1, 2025
    
    try:
        cmd = ['python', 'main.py', '--date', test_date]
        env = os.environ.copy()
        env['ETL_DATE'] = test_date
        
        print(f"🔄 Testing {test_date}...")
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print(f"✅ Test successful for {test_date}")
            print("📊 Sample output:")
            lines = result.stdout.split('\n')
            for line in lines[-10:]:  # Show last 10 lines
                if line.strip():
                    print(f"   {line}")
            return True
        else:
            print(f"❌ Test failed for {test_date}")
            print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

if __name__ == '__main__':
    # Ask user what they want to do
    print("🍴 Toast ETL May 2025 Backfill Options:")
    print("1. Test single date (May 1, 2025)")
    print("2. Run full May 2025 backfill")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        run_single_date_test()
    elif choice == "2":
        run_may_2025_simple_backfill()
    else:
        print("Invalid choice. Running single date test...")
        run_single_date_test() 