#!/usr/bin/env python3
"""
Test Dashboard API Endpoints - Toast ETL Pipeline
Test script to verify dashboard API functionality.
"""

import requests
import json
import sys
import os
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_dashboard_endpoints():
    """Test dashboard API endpoints."""
    
    # Base URL (adjust if running on different port)
    base_url = "http://localhost:8080"
    
    print("🧪 Testing Dashboard API Endpoints")
    print("=" * 50)
    
    # Test endpoints
    endpoints = [
        ("/health", "Health Check"),
        ("/api/dashboard/overview", "Dashboard Overview"),
        ("/api/dashboard/data/summary", "Data Summary")
    ]
    
    for endpoint, description in endpoints:
        print(f"\n📍 Testing {description}: {endpoint}")
        
        try:
            response = requests.get(f"{base_url}{endpoint}", timeout=10)
            
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("✅ SUCCESS")
                
                # Pretty print response (truncated)
                json_str = json.dumps(data, indent=2, default=str)
                if len(json_str) > 500:
                    json_str = json_str[:500] + "...\n}"
                print(f"Response: {json_str}")
                
            else:
                print("❌ FAILED")
                print(f"Error: {response.text}")
                
        except requests.exceptions.ConnectionError:
            print("❌ CONNECTION ERROR - Server not running")
            print("Start the Flask server with: python -m src.server.app")
            
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
    
    print("\n" + "=" * 50)
    print("🏁 Dashboard API Test Complete")


def start_test_server():
    """Start the Flask server for testing."""
    print("🚀 Starting Flask Server for Testing...")
    
    try:
        # Set environment variables
        os.environ['PROJECT_ID'] = 'toast-analytics-444116'
        os.environ['DATASET_ID'] = 'toast_analytics'
        os.environ['ENVIRONMENT'] = 'development'
        
        # Import and run Flask app
        from src.server.app import app
        
        print("Server starting on http://localhost:8080")
        print("Press Ctrl+C to stop the server")
        
        app.run(host='0.0.0.0', port=8080, debug=True)
        
    except ImportError as e:
        print(f"❌ Import Error: {str(e)}")
        print("Make sure you're in the project root directory")
    except Exception as e:
        print(f"❌ Server Error: {str(e)}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        start_test_server()
    else:
        test_dashboard_endpoints() 