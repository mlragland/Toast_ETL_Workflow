#!/usr/bin/env python3
"""
Toast ETL Workflow - System Status Checker
Comprehensive validation of all project components and current state.
"""

import sys
import os
import subprocess
import requests
from pathlib import Path

def check_dependencies():
    """Check if all required Python dependencies are available."""
    print("🔍 Checking Python Dependencies...")
    
    required_packages = [
        'flask', 'pandas', 'google.cloud.bigquery', 
        'google.cloud.storage', 'pydantic', 'python_dotenv'
    ]
    
    missing = []
    for package in required_packages:
        try:
            __import__(package.replace('.', '_') if '.' in package else package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package}")
            missing.append(package)
    
    return len(missing) == 0

def check_project_structure():
    """Validate project directory structure."""
    print("\n📁 Checking Project Structure...")
    
    required_dirs = [
        'src/config', 'src/extractors', 'src/transformers', 
        'src/loaders', 'src/validators', 'src/server',
        'src/backfill', 'dashboard/src', 'infrastructure'
    ]
    
    all_exist = True
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print(f"  ✅ {dir_path}")
        else:
            print(f"  ❌ {dir_path}")
            all_exist = False
    
    return all_exist

def check_import_paths():
    """Test critical import paths."""
    print("\n🔗 Checking Import Paths...")
    
    try:
        # Add project root to path
        sys.path.append(str(Path(__file__).parent))
        
        from src.backfill.backfill_manager import BackfillManager
        print("  ✅ BackfillManager import working")
        
        from src.server.app import app
        print("  ✅ Flask app import working")
        
        from src.transformers.toast_transformer import ToastDataTransformer
        print("  ✅ ToastDataTransformer import working")
        
        return True
        
    except ImportError as e:
        print(f"  ❌ Import error: {e}")
        return False

def check_frontend():
    """Check React frontend status."""
    print("\n⚛️  Checking React Frontend...")
    
    # Check if package.json exists
    if Path('dashboard/package.json').exists():
        print("  ✅ React app configured")
    else:
        print("  ❌ React app missing")
        return False
    
    # Check if server is running
    try:
        response = requests.get('http://localhost:3000', timeout=2)
        if response.status_code == 200:
            print("  ✅ React dev server running on port 3000")
            return True
        else:
            print("  ⚠️  React server responding but with issues")
            return False
    except requests.exceptions.RequestException:
        print("  ❌ React dev server not running on port 3000")
        return False

def check_backend():
    """Check Flask backend status."""
    print("\n🐍 Checking Flask Backend...")
    
    # Test different common ports
    ports = [5000, 8080, 8000]
    
    for port in ports:
        try:
            response = requests.get(f'http://localhost:{port}/health', timeout=2)
            if response.status_code == 200:
                print(f"  ✅ Flask server running on port {port}")
                return True
        except requests.exceptions.RequestException:
            pass
    
    print("  ❌ Flask server not running on common ports (5000, 8080, 8000)")
    return False

def get_progress_status():
    """Summarize current progress based on checklist."""
    print("\n📊 Progress Summary:")
    print("=" * 50)
    
    phases = {
        "Phase 1: Foundation & Architecture": "✅ COMPLETE",
        "Phase 2: Infrastructure & Containerization": "✅ COMPLETE", 
        "Phase 3: Data Transformation Layer": "✅ COMPLETE",
        "Phase 4: Advanced Data Processing & QA": "✅ COMPLETE",
        "Phase 5: Infrastructure & Deployment": "✅ COMPLETE",
        "Phase 6: Dashboard UI & API Development": "🚧 IN PROGRESS",
        "Phase 7: Advanced Features & Analytics": "⏳ PENDING"
    }
    
    for phase, status in phases.items():
        print(f"  {status} {phase}")
    
    print(f"\n📈 Overall Progress: 71% Complete (5 of 7 phases)")

def main():
    """Run comprehensive system check."""
    print("🍴 Toast ETL Workflow - System Status Check")
    print("=" * 60)
    
    results = {
        "dependencies": check_dependencies(),
        "structure": check_project_structure(), 
        "imports": check_import_paths(),
        "frontend": check_frontend(),
        "backend": check_backend()
    }
    
    get_progress_status()
    
    print("\n🔧 Current Issues & Next Steps:")
    print("=" * 50)
    
    if not results["backend"]:
        print("  ❗ Backend Server: Flask API needs to be started")
        print("     Run: export PROJECT_ID=toast-analytics-444116 && export DATASET_ID=toast_analytics && python -c \"from src.server.app import app; app.run(host='0.0.0.0', port=8080)\"")
    
    if results["frontend"] and not results["backend"]:
        print("  🎯 Priority: Start Flask backend to enable frontend-backend communication")
    
    if results["frontend"] and results["backend"]:
        print("  🚀 Ready for Phase 6 development: Full-stack dashboard implementation")
    
    print("\n🎯 Immediate Next Actions:")
    print("  1. Start Flask backend server")
    print("  2. Implement dashboard UI components")
    print("  3. Connect frontend to backend APIs")
    print("  4. Test full-stack integration")
    
    overall_health = sum(results.values()) / len(results) * 100
    print(f"\n💚 System Health: {overall_health:.0f}%")
    
    if overall_health > 80:
        print("🎉 System ready for active development!")
    elif overall_health > 60:
        print("⚡ System mostly ready - minor fixes needed")
    else:
        print("🔧 System needs attention - several issues to resolve")

if __name__ == "__main__":
    main() 