#!/usr/bin/env python3
"""
Simple server to run the Toast ETL Dashboard without monitoring dependencies.
"""
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Disable monitoring
os.environ['ENABLE_MONITORING'] = 'false'

try:
    from src.server.app import create_app
    
    # Create app with monitoring disabled
    app = create_app()
    
    if __name__ == '__main__':
        port = 8080
        print(f"🍴 Toast ETL Dashboard starting on http://localhost:{port}")
        print("📊 Dashboard will show the newly loaded 20250611 data")
        print("🔗 API endpoints available at /api/")
        app.run(host='0.0.0.0', port=port, debug=True)
        
except Exception as e:
    print(f"❌ Error starting server: {e}")
    print("Please check dependencies and configuration")
    sys.exit(1) 