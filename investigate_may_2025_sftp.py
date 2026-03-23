#!/usr/bin/env python3
"""
Diagnostic script to investigate May 2025 SFTP files
and understand why backfill detected business closures
"""

import sys
import os
sys.path.append('src')

from datetime import datetime, timedelta

def investigate_may_2025():
    """Investigate May 2025 SFTP files and business closure detection"""
    
    print("🔍 Investigating May 2025 SFTP Files")
    print("=" * 60)
    
    # Generate May 2025 dates
    start_date = datetime(2025, 5, 1)
    end_date = datetime(2025, 5, 31)
    
    may_dates = []
    current_date = start_date
    while current_date <= end_date:
        may_dates.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)
    
    print(f"📅 Checking {len(may_dates)} dates in May 2025")
    print(f"Date range: {may_dates[0]} to {may_dates[-1]}")
    print()
    
    try:
        from extractors.sftp_extractor import SFTPExtractor
        from validators.business_calendar import BusinessCalendar
        
        sftp_extractor = SFTPExtractor()
        business_calendar = BusinessCalendar()
        
        print("✅ Components initialized successfully")
        
        # Check SFTP file availability
        print("\n🌐 SFTP File Availability Check:")
        print("-" * 40)
        
        available_dates = []
        missing_dates = []
        file_counts = {}
        
        for date_str in may_dates[:5]:  # Check first 5 dates as sample
            try:
                files = sftp_extractor.list_files_for_date(date_str)
                if files and len(files) > 0:
                    available_dates.append(date_str)
                    file_counts[date_str] = len(files)
                    print(f"✅ {date_str}: {len(files)} files found")
                    for file in files[:2]:  # Show first 2 files
                        print(f"   📄 {file}")
                else:
                    missing_dates.append(date_str)
                    print(f"❌ {date_str}: No files found")
                    
            except Exception as e:
                missing_dates.append(date_str)
                print(f"⚠️  {date_str}: Error - {str(e)}")
        
        print(f"\nSample check complete. Available: {len(available_dates)}, Missing: {len(missing_dates)}")
        
        # Check business closure detection
        print("\n🏢 Business Closure Detection Check:")
        print("-" * 40)
        
        for date_str in may_dates[:5]:  # Check same sample dates
            try:
                is_closure = business_calendar.is_business_closure(date_str)
                status = "CLOSURE" if is_closure else "DATA"
                print(f"📊 {date_str}: Detected as {status}")
            except Exception as e:
                print(f"⚠️  {date_str}: Closure detection error - {str(e)}")
        
    except ImportError as e:
        print(f"❌ Import error: {str(e)}")
        print("This suggests there may be module path issues")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    
    print("\n💡 Tomorrow's Investigation Plan:")
    print("=" * 60)
    print("1. Fix any import/module issues")
    print("2. Check SFTP connection and file listing")
    print("3. Analyze business closure detection logic")
    print("4. Test individual date processing")
    print("5. Implement proper bulk file-by-file loading")
    print("\n🛏️  Good night! The investigation setup is ready.")

if __name__ == "__main__":
    investigate_may_2025() 