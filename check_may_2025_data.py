#!/usr/bin/env python3
"""
Check May 2025 data availability on Toast SFTP server
"""

import os
from datetime import datetime, timedelta
import paramiko
from pathlib import Path

# SFTP connection details
SFTP_HOST = 'sftp.toasttab.com'
SFTP_USERNAME = 'Maurices-Gourmet-Burgers'
SFTP_PASSWORD = 'Burgers123!'

def check_may_2025_data():
    """Check what May 2025 data is available on SFTP"""
    
    print('🔍 Checking available May 2025 data on SFTP...')
    print('=' * 50)

    try:
        # Connect to SFTP
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(SFTP_HOST, username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = ssh.open_sftp()
        
        # Check May 2025 dates (May 1-31, 2025)
        may_dates = []
        for day in range(1, 32):
            date_str = f'2025-05-{day:02d}'
            try:
                files = sftp.listdir(f'/reports/{date_str}')
                if files:
                    may_dates.append(date_str)
                    print(f'✅ {date_str}: {len(files)} files')
                    # Show file types for first few dates
                    if len(may_dates) <= 3:
                        for file in files:
                            print(f'   📄 {file}')
            except:
                print(f'❌ {date_str}: No data')
        
        print('=' * 50)
        print(f'📊 Total May 2025 dates with data: {len(may_dates)}')
        
        if may_dates:
            print(f'📅 Date range: {may_dates[0]} to {may_dates[-1]}')
            return may_dates
        else:
            print('⚠️  No May 2025 data found')
            return []
        
        sftp.close()
        ssh.close()
        
    except Exception as e:
        print(f'❌ SFTP connection error: {e}')
        return []

if __name__ == '__main__':
    check_may_2025_data() 