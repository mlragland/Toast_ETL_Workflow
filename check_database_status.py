#!/usr/bin/env python3
"""
Check current database status and record counts.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.loaders.bigquery_loader import BigQueryLoader

def main():
    """Check database status."""
    
    try:
        loader = BigQueryLoader()

        print('📊 Current Database Status - Toast ETL Pipeline')
        print('=' * 60)

        tables = ['order_details', 'check_details', 'kitchen_timings', 'cash_entries', 
                 'all_items_report', 'item_selection_details', 'payment_details']

        total_records = 0
        existing_tables = []
        
        for table in tables:
            try:
                query = f'SELECT COUNT(*) as count FROM `toast-analytics-444116.toast_analytics.{table}`'
                result = loader.client.query(query).result()
                count = list(result)[0].count
                total_records += count
                if count > 0:
                    existing_tables.append(table)
                print(f'📋 {table.ljust(20)}: {count:,} records')
            except Exception as e:
                print(f'❌ {table.ljust(20)}: Table not found or error')

        print('=' * 60)
        print(f'🎯 TOTAL RECORDS: {total_records:,}')
        print(f'📊 Tables with data: {len(existing_tables)}/{len(tables)}')

        # Get date range for order_details (main table)
        if 'order_details' in existing_tables:
            try:
                query = '''
                SELECT 
                    MIN(DATE(opened)) as earliest_date,
                    MAX(DATE(opened)) as latest_date,
                    COUNT(DISTINCT DATE(opened)) as unique_dates
                FROM `toast-analytics-444116.toast_analytics.order_details`
                WHERE opened IS NOT NULL
                '''
                result = loader.client.query(query).result()
                row = list(result)[0]
                
                print(f'📅 Date Range: {row.earliest_date} to {row.latest_date}')
                print(f'📈 Unique Days: {row.unique_dates} days of data')
            except Exception as e:
                print(f'❌ Could not get date range: {e}')
        
        # Check processing dates
        try:
            query = '''
            SELECT 
                MIN(DATE(loaded_at)) as first_load,
                MAX(DATE(loaded_at)) as last_load
            FROM `toast-analytics-444116.toast_analytics.order_details`
            WHERE loaded_at IS NOT NULL
            '''
            result = loader.client.query(query).result()
            row = list(result)[0]
            
            print(f'🔄 Data Loading: {row.first_load} to {row.last_load}')
        except Exception as e:
            print(f'❌ Could not get processing dates: {e}')

        print('=' * 60)
        
        if total_records > 0:
            print('✅ Database contains data and is operational')
        else:
            print('⚠️  Database is empty - ready for backfill')
        
    except Exception as e:
        print(f'💥 Error checking database: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 