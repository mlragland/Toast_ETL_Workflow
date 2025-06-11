#!/usr/bin/env python3
"""
Toast ETL Database Row Count Checker
Checks current row counts across all BigQuery tables.
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path for imports
sys.path.append(str(Path(__file__).parent))

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def check_database_status():
    """Check the current status and row counts of all BigQuery tables."""
    
    # Configuration
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
    
    print("🍴 Toast ETL Database Status Check")
    print("=" * 60)
    print(f"Project: {project_id}")
    print(f"Dataset: {dataset_id}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        dataset_ref = client.dataset(dataset_id)
        
        # List of expected tables
        expected_tables = [
            'all_items_report',
            'check_details', 
            'cash_entries',
            'item_selection_details',
            'kitchen_timings',
            'order_details',
            'payment_details'
        ]
        
        total_rows = 0
        table_status = {}
        
        print("📊 Table Row Counts:")
        print("-" * 40)
        
        for table_name in expected_tables:
            try:
                table_ref = dataset_ref.table(table_name)
                table = client.get_table(table_ref)
                
                # Get row count
                row_count = table.num_rows
                total_rows += row_count
                table_status[table_name] = {
                    'exists': True,
                    'rows': row_count,
                    'size_mb': round(table.num_bytes / (1024 * 1024), 2) if table.num_bytes else 0
                }
                
                print(f"  ✅ {table_name:<25} {row_count:>8,} rows ({table_status[table_name]['size_mb']} MB)")
                
            except NotFound:
                table_status[table_name] = {'exists': False, 'rows': 0, 'size_mb': 0}
                print(f"  ❌ {table_name:<25} Table not found")
            except Exception as e:
                table_status[table_name] = {'exists': False, 'rows': 0, 'size_mb': 0}
                print(f"  ⚠️  {table_name:<25} Error: {str(e)}")
        
        print("-" * 40)
        print(f"📈 Total Rows: {total_rows:,}")
        
        # Check for recent data
        print("\n📅 Recent Data Analysis:")
        print("-" * 40)
        
        if table_status['order_details']['exists'] and table_status['order_details']['rows'] > 0:
            try:
                # Check date range of data
                query = f"""
                SELECT 
                    MIN(DATE(created_date)) as earliest_date,
                    MAX(DATE(created_date)) as latest_date,
                    COUNT(DISTINCT DATE(created_date)) as unique_dates,
                    COUNT(*) as total_orders
                FROM `{project_id}.{dataset_id}.order_details`
                WHERE created_date IS NOT NULL
                """
                
                result = client.query(query).result()
                for row in result:
                    print(f"  📊 Date Range: {row.earliest_date} to {row.latest_date}")
                    print(f"  📅 Unique Dates: {row.unique_dates}")
                    print(f"  🛒 Total Orders: {row.total_orders:,}")
                    
                    if row.latest_date:
                        days_ago = (datetime.now().date() - row.latest_date).days
                        print(f"  ⏰ Most Recent Data: {days_ago} days ago")
                
            except Exception as e:
                print(f"  ⚠️  Error analyzing recent data: {str(e)}")
        else:
            print("  ❌ No order data available for analysis")
        
        # Business metrics summary
        if table_status['order_details']['exists'] and table_status['order_details']['rows'] > 0:
            print("\n💰 Business Metrics Summary:")
            print("-" * 40)
            
            try:
                metrics_query = f"""
                SELECT 
                    COUNT(*) as total_orders,
                    ROUND(SUM(CAST(net_price AS FLOAT64)), 2) as total_sales,
                    ROUND(AVG(CAST(net_price AS FLOAT64)), 2) as avg_order_value,
                    COUNT(DISTINCT business_date) as business_days
                FROM `{project_id}.{dataset_id}.order_details`
                WHERE net_price IS NOT NULL AND net_price != ''
                """
                
                result = client.query(metrics_query).result()
                for row in result:
                    print(f"  🛒 Total Orders: {row.total_orders:,}")
                    print(f"  💵 Total Sales: ${row.total_sales:,.2f}" if row.total_sales else "  💵 Total Sales: N/A")
                    print(f"  📊 Avg Order Value: ${row.avg_order_value:.2f}" if row.avg_order_value else "  📊 Avg Order Value: N/A")
                    print(f"  📅 Business Days: {row.business_days}")
                    
            except Exception as e:
                print(f"  ⚠️  Error calculating business metrics: {str(e)}")
        
        # Data quality summary
        tables_with_data = sum(1 for status in table_status.values() if status['exists'] and status['rows'] > 0)
        total_expected = len(expected_tables)
        data_coverage = (tables_with_data / total_expected) * 100
        
        print("\n🎯 Data Coverage Summary:")
        print("-" * 40)
        print(f"  📋 Tables with Data: {tables_with_data}/{total_expected} ({data_coverage:.0f}%)")
        print(f"  💾 Total Database Size: {sum(t['size_mb'] for t in table_status.values()):.1f} MB")
        
        if total_rows == 0:
            print("  🚨 Database is empty - no data loaded yet")
            print("  💡 Recommendation: Run ETL pipeline or backfill to load data")
        elif total_rows < 1000:
            print("  ⚠️  Limited data available")
            print("  💡 Recommendation: Consider running historical backfill")
        elif total_rows < 10000:
            print("  ✅ Good amount of data for development and testing")
        else:
            print("  🎉 Substantial dataset available for analytics")
        
        return table_status, total_rows
        
    except Exception as e:
        print(f"❌ Error accessing BigQuery: {str(e)}")
        print("💡 Make sure you have proper authentication and permissions")
        return {}, 0

def main():
    """Main function to check database status."""
    table_status, total_rows = check_database_status()
    
    print("\n" + "=" * 60)
    if total_rows > 0:
        print("✅ Database Status: Active with data")
        print("🚀 Ready for dashboard development and analytics")
    else:
        print("⚠️  Database Status: Empty or inaccessible")
        print("🔧 Next Steps: Run ETL pipeline to load data")
    print("=" * 60)

if __name__ == "__main__":
    main() 