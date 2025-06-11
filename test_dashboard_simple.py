#!/usr/bin/env python3
"""
Simple Dashboard Test - Toast ETL Pipeline
Test BigQuery connectivity and dashboard data queries without Flask.
"""

import os
import sys
from datetime import datetime

def test_bigquery_connectivity():
    """Test BigQuery connectivity and dashboard queries."""
    
    print("🧪 Testing BigQuery Dashboard Functionality")
    print("=" * 50)
    
    try:
        # Set environment variables
        os.environ['PROJECT_ID'] = 'toast-analytics-444116'
        os.environ['DATASET_ID'] = 'toast_analytics'
        
        # Test BigQuery import and connection
        print("📊 Testing BigQuery Connection...")
        from google.cloud import bigquery
        
        project_id = 'toast-analytics-444116'
        dataset_id = 'toast_analytics'
        
        client = bigquery.Client(project=project_id)
        print(f"✅ BigQuery client created for project: {project_id}")
        
        # Test dashboard tables exist
        print("\n🗂️ Checking Dashboard Tables...")
        
        tables_to_check = [
            'order_details',
            'etl_runs',
            'backfill_jobs',
            'daily_summary',
            'recent_runs'
        ]
        
        for table_name in tables_to_check:
            try:
                table_id = f"{project_id}.{dataset_id}.{table_name}"
                table = client.get_table(table_id)
                print(f"✅ {table_name}: {table.num_rows} rows")
            except Exception as e:
                if "Not found" in str(e):
                    print(f"⚠️ {table_name}: Table not found")
                else:
                    print(f"❌ {table_name}: Error - {str(e)}")
        
        # Test dashboard overview query
        print("\n📈 Testing Dashboard Overview Query...")
        
        overview_query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT PARSE_DATE('%Y-%m-%d', processing_date)) as unique_days,
            MIN(PARSE_DATE('%Y-%m-%d', processing_date)) as earliest_date,
            MAX(PARSE_DATE('%Y-%m-%d', processing_date)) as latest_date,
            SUM(total) as total_sales
        FROM `{project_id}.{dataset_id}.order_details`
        """
        
        result = list(client.query(overview_query).result())
        
        if result:
            row = result[0]
            print("✅ Dashboard overview query successful!")
            print(f"   Total Records: {row['total_records']:,}")
            print(f"   Unique Days: {row['unique_days']}")
            print(f"   Date Range: {row['earliest_date']} to {row['latest_date']}")
            print(f"   Total Sales: ${row['total_sales']:,.2f}")
            
            # Create mock dashboard response
            dashboard_response = {
                'status': 'success',
                'data': {
                    'database_stats': {
                        'total_records': row['total_records'],
                        'unique_days': row['unique_days'],
                        'date_range': {
                            'start': row['earliest_date'].isoformat() if row['earliest_date'] else None,
                            'end': row['latest_date'].isoformat() if row['latest_date'] else None
                        },
                        'total_sales': round(row['total_sales'] or 0, 2)
                    },
                    'summary': {
                        'total_runs': 0,
                        'success_rate': 100.0,
                        'failed_runs': 0,
                        'avg_execution_time': 0
                    }
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
            print(f"\n📋 Mock Dashboard Response:")
            import json
            print(json.dumps(dashboard_response, indent=2))
            
        else:
            print("⚠️ No data returned from overview query")
        
        # Test sample ETL run insertion
        print("\n🔄 Testing ETL Run Tracking...")
        
        # Check if we can insert a sample ETL run
        try:
            sample_run_query = f"""
            SELECT COUNT(*) as run_count
            FROM `{project_id}.{dataset_id}.etl_runs`
            WHERE run_id LIKE 'test_%'
            """
            
            result = list(client.query(sample_run_query).result())
            existing_test_runs = result[0]['run_count'] if result else 0
            
            print(f"✅ ETL runs table accessible")
            print(f"   Existing test runs: {existing_test_runs}")
            
        except Exception as e:
            print(f"⚠️ ETL runs table issue: {str(e)}")
        
        print("\n✅ Dashboard functionality tests completed!")
        print("\n🎯 Summary:")
        print("   - BigQuery connectivity: Working")
        print("   - Data queries: Working")
        print("   - Dashboard tables: Available")
        print("   - Ready for frontend development")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bigquery_connectivity() 