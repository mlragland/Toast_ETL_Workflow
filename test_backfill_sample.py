#!/usr/bin/env python3
"""
Test script to run backfill on a small sample of dates first.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import from source
from src.backfill.backfill_manager import BackfillManager
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def main():
    """Test backfill with a small sample."""
    
    print("🧪 Testing Toast ETL Historical Backfill")
    print("=" * 50)
    print("📊 Target: 3 sample dates")
    print("📅 Dates: 20240607, 20240608, 20240609")
    print("=" * 50)
    
    try:
        # Initialize backfill manager
        print("⚙️  Initializing backfill manager...")
        
        backfill_manager = BackfillManager(
            max_workers=1,  # Single thread for testing
            batch_size=3,   # Process all 3 at once
            skip_existing=False,  # Process even if exists
            validate_data=False  # Skip validation for speed
        )
        
        # Test with specific dates that should have data
        test_dates = ['20240607', '20240608', '20240609']  # The dates we know have data
        print(f"🎯 Testing with dates: {test_dates}")
        
        summary = backfill_manager.run_backfill(specific_dates=test_dates)
        
        # Display results
        print("\n" + "=" * 50)
        print("🧪 TEST RESULTS")
        print("=" * 50)
        print(f"📅 Processed: {summary['processed_dates']}/{summary['total_dates']}")
        print(f"❌ Failed: {summary['failed_dates']}")
        print(f"📊 Records: {summary['total_records']:,}")
        print(f"⏱️  Duration: {summary['duration']}")
        print(f"✅ Success rate: {summary['success_rate']:.1f}%")
        
        if summary['failed_date_list']:
            print(f"⚠️  Failed dates: {summary['failed_date_list']}")
        
        if summary['success_rate'] == 100:
            print("🎉 Test successful! Ready for full backfill.")
        else:
            print("⚠️  Issues detected. Check logs before full backfill.")
        
        print("=" * 50)
        
    except Exception as e:
        print(f"💥 Test failed: {e}")
        logger.error(f"Test backfill error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 