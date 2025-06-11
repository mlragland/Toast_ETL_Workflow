#!/usr/bin/env python3
"""
Analyze SFTP data availability for April 2024, focusing on weekends.
"""

import sys
import os
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings

def get_april_2024_weekends():
    """Get all Friday and Saturday dates in April 2024."""
    april_start = datetime(2024, 4, 1)
    april_end = datetime(2024, 4, 30)
    
    weekends = []
    current = april_start
    
    while current <= april_end:
        # Friday = 4, Saturday = 5 (Monday = 0)
        if current.weekday() in [4, 5]:  # Friday or Saturday
            weekends.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    
    return weekends

def download_and_analyze_date(date):
    """Download and analyze files for a specific date."""
    print(f"\n🔍 Analyzing {date} ({datetime.strptime(date, '%Y%m%d').strftime('%A, %B %d, %Y')})")
    
    # Create local directory
    local_dir = f"/tmp/sftp_analysis/{date}"
    os.makedirs(local_dir, exist_ok=True)
    
    try:
        # Build SFTP command
        ssh_key_path = os.path.expanduser(settings.ssh_key_path)
        sftp_command = [
            "sftp",
            "-i", ssh_key_path,
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "LogLevel=ERROR",
            f"{settings.sftp_user}@{settings.sftp_server}"
        ]
        
        # SFTP commands to download files
        batch_commands = [
            f"cd 185129/{date}",
            f"lcd {local_dir}",
            "mget *",
            "quit"
        ]
        
        # Execute SFTP command
        process = subprocess.Popen(
            sftp_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate(input="\n".join(batch_commands))
        
        if process.returncode != 0:
            print(f"❌ SFTP failed: {stderr.strip()}")
            return None
        
        # Analyze downloaded files
        files_info = {}
        total_size = 0
        total_records = 0
        
        if os.path.exists(local_dir):
            for file in os.listdir(local_dir):
                if file.endswith('.csv'):
                    file_path = os.path.join(local_dir, file)
                    file_size = os.path.getsize(file_path)
                    total_size += file_size
                    
                    # Count records in CSV
                    try:
                        df = pd.read_csv(file_path)
                        record_count = len(df)
                        total_records += record_count
                        
                        files_info[file] = {
                            'size_bytes': file_size,
                            'records': record_count,
                            'has_data': record_count > 0 and file_size > 100  # More than just headers
                        }
                    except Exception as e:
                        files_info[file] = {
                            'size_bytes': file_size,
                            'records': 0,
                            'has_data': False,
                            'error': str(e)
                        }
        
        # Print analysis
        if files_info:
            print(f"📁 Files found: {len(files_info)}")
            for filename, info in files_info.items():
                status = "✅ HAS DATA" if info['has_data'] else "❌ Empty/Headers only"
                print(f"   📄 {filename}: {info['records']} records, {info['size_bytes']} bytes - {status}")
            
            print(f"📊 Total: {total_records} records, {total_size} bytes")
            has_meaningful_data = any(info['has_data'] for info in files_info.values())
            return {
                'date': date,
                'files': files_info,
                'total_records': total_records,
                'total_size': total_size,
                'has_data': has_meaningful_data
            }
        else:
            print("❌ No CSV files found")
            return None
            
    except Exception as e:
        print(f"💥 Error: {e}")
        return None
    finally:
        # Cleanup
        if os.path.exists(local_dir):
            import shutil
            shutil.rmtree(local_dir, ignore_errors=True)

def main():
    """Main analysis function."""
    
    print("🔍 SFTP Data Analysis - April 2024 Weekends")
    print("=" * 60)
    
    # Create analysis directory
    os.makedirs("/tmp/sftp_analysis", exist_ok=True)
    
    # Get April 2024 weekend dates
    weekend_dates = get_april_2024_weekends()
    print(f"📅 Analyzing {len(weekend_dates)} weekend dates in April 2024:")
    for date in weekend_dates:
        day_name = datetime.strptime(date, '%Y%m%d').strftime('%A')
        print(f"   • {date} ({day_name})")
    
    print("\n" + "=" * 60)
    
    # Analyze each date
    results = []
    dates_with_data = []
    
    for date in weekend_dates[:6]:  # Analyze first 6 weekend dates
        result = download_and_analyze_date(date)
        if result:
            results.append(result)
            if result['has_data']:
                dates_with_data.append(date)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 APRIL 2024 WEEKEND ANALYSIS SUMMARY")
    print("=" * 60)
    
    if results:
        total_analyzed = len(results)
        dates_with_meaningful_data = len(dates_with_data)
        
        print(f"🔍 Dates analyzed: {total_analyzed}")
        print(f"✅ Dates with data: {dates_with_meaningful_data}")
        print(f"❌ Empty dates: {total_analyzed - dates_with_meaningful_data}")
        print(f"📈 Data availability: {(dates_with_meaningful_data/total_analyzed)*100:.1f}%")
        
        if dates_with_data:
            print(f"\n🎯 Best test dates (with data):")
            for date in dates_with_data[:3]:
                day_name = datetime.strptime(date, '%Y%m%d').strftime('%A, %B %d')
                print(f"   • {date} ({day_name})")
        
        # Find date with most records
        if results:
            best_date = max(results, key=lambda x: x['total_records'])
            if best_date['total_records'] > 0:
                day_name = datetime.strptime(best_date['date'], '%Y%m%d').strftime('%A, %B %d')
                print(f"\n🏆 Richest dataset: {best_date['date']} ({day_name})")
                print(f"   📊 {best_date['total_records']} total records")
    else:
        print("❌ No data found in analyzed dates")
    
    print("=" * 60)

if __name__ == "__main__":
    main() 