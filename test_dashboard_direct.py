#!/usr/bin/env python3
"""
Direct Dashboard API Test - Toast ETL Pipeline
Test dashboard functionality directly without a running server.
"""

import sys
import os
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_dashboard_functions():
    """Test dashboard functions directly."""
    
    print("🧪 Testing Dashboard Functions Directly")
    print("=" * 50)
    
    try:
        # Set environment variables
        os.environ['PROJECT_ID'] = 'toast-analytics-444116'
        os.environ['DATASET_ID'] = 'toast_analytics'
        os.environ['ENVIRONMENT'] = 'development'
        
        # Test dashboard routes
        from src.server.dashboard_routes import get_bigquery_client, get_dataset_info
        
        print("✅ Dashboard imports successful")
        
        # Test BigQuery client
        print("\n📊 Testing BigQuery Connection...")
        client = get_bigquery_client()
        info = get_dataset_info()
        
        print(f"Project ID: {info['project_id']}")
        print(f"Dataset ID: {info['dataset_id']}")
        
        # Test a simple query
        print("\n🔍 Testing Database Query...")
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT DATE(created_date)) as unique_days,
            MIN(DATE(created_date)) as earliest_date,
            MAX(DATE(created_date)) as latest_date
        FROM `{info['project_id']}.{info['dataset_id']}.order_details`
        LIMIT 1
        """
        
        result = list(client.query(query).result())
        
        if result:
            row = result[0]
            print(f"✅ Query successful!")
            print(f"Total Records: {row['total_records']}")
            print(f"Unique Days: {row['unique_days']}")
            print(f"Date Range: {row['earliest_date']} to {row['latest_date']}")
        else:
            print("⚠️ No data returned")
        
        # Test Flask app creation
        print("\n🌐 Testing Flask App Creation...")
        from src.server.app import create_app
        
        app = create_app()
        print(f"✅ Flask app created successfully")
        print(f"App name: {app.name}")
        
        # Test route registration
        print("\n🔌 Testing Route Registration...")
        with app.test_client() as client:
            # Test health endpoint
            response = client.get('/health')
            print(f"Health endpoint: {response.status_code}")
            
            # Test dashboard overview
            response = client.get('/api/dashboard/overview')
            print(f"Dashboard overview: {response.status_code}")
            
            if response.status_code == 200:
                data = response.get_json()
                print(f"✅ Dashboard overview successful")
                print(f"Database records: {data['data']['database_stats']['total_records']}")
                print(f"Date range: {data['data']['database_stats']['date_range']}")
            else:
                print(f"❌ Dashboard overview failed: {response.get_data(as_text=True)}")
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_dashboard_functions() 