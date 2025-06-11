#!/usr/bin/env python3
"""
Analyze recent weekend dates to find the best test data.
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

def get_recent_weekend_dates():
    """Get recent Friday/Saturday dates for testing."""
    dates = [
        '20240503',  # Friday, May 3
        '20240504',  # Saturday, May 4
        '20240510',  # Friday, May 10
        '20240511',  # Saturday, May 11
        '20240517',  # Friday, May 17
        '20240518',  # Saturday, May 18
        '20240524',  # Friday, May 24
        '20240525',  # Saturday, May 25
        '20240531',  # Friday, May 31
        '20240601',  # Saturday, June 1
    ]
    return dates

def quick_analyze_date(date):
    """Quick analysis of a single date."""
    local_dir = f"/tmp/quick_analysis/{date}"
    os.makedirs(local_dir, exist_ok=True)
    
    try:
        ssh_key_path = os.path.expanduser(settings.ssh_key_path)
        sftp_command = [
            "sftp", "-i", ssh_key_path, "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR",
            f"{settings.sftp_user}@{settings.sftp_server}"
        ]
        
        batch_commands = [f"cd 185129/{date}", f"lcd {local_dir}", "mget *", "quit"]
        
        process = subprocess.Popen(sftp_command, stdin=subprocess.PIPE, 
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate(input="\n".join(batch_commands))
        
        if process.returncode != 0:
            return None
        
        total_records = 0
        files_found = 0
        
        if os.path.exists(local_dir):
            for file in os.listdir(local_dir):
                if file.endswith('.csv'):
                    files_found += 1
                    file_path = os.path.join(local_dir, file)
                    try:
                        df = pd.read_csv(file_path)
                        total_records += len(df)
                    except:
                        pass
        
        return {'date': date, 'files': files_found, 'records': total_records}
        
    except Exception:
        return None
    finally:
        if os.path.exists(local_dir):
            import shutil
            shutil.rmtree(local_dir, ignore_errors=True)

def main():
    """Main function."""
    print("🔍 Quick Analysis - Recent Weekend Dates")
    print("=" * 50)
    
    os.makedirs("/tmp/quick_analysis", exist_ok=True)
    
    dates = get_recent_weekend_dates()
    results = []
    
    for date in dates:
        day_name = datetime.strptime(date, '%Y%m%d').strftime('%a %b %d')
        print(f"📅 {date} ({day_name})... ", end="", flush=True)
        
        result = quick_analyze_date(date)
        if result and result['records'] > 0:
            print(f"✅ {result['records']} records")
            results.append(result)
        else:
            print("❌ No data")
    
    print("\n" + "=" * 50)
    print("🎯 RECOMMENDED TEST DATES (Fridays/Saturdays)")
    print("=" * 50)
    
    # Sort by record count (highest first)
    results.sort(key=lambda x: x['records'], reverse=True)
    
    for i, result in enumerate(results[:5], 1):
        date = result['date']
        day_name = datetime.strptime(date, '%Y%m%d').strftime('%A, %B %d, %Y')
        print(f"{i}. 📊 {date} ({day_name}) - {result['records']} records")
    
    if results:
        print(f"\n🏆 Best test date: {results[0]['date']} with {results[0]['records']} records")

if __name__ == "__main__":
    main() 