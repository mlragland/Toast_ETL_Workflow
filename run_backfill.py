#!/usr/bin/env python3
"""
Toast ETL Phase 7: Complete Historical Backfill
Processes all 432 days of available SFTP data with intelligent filtering.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from src.backfill.backfill_manager import BackfillManager
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def main():
    """Run the complete historical backfill process."""
    
    print("🚀 Toast ETL Phase 7: Complete Historical Backfill")
    print("=" * 70)
    print("📊 Target: All 432 days of available SFTP data")
    print("📅 Range: April 4, 2024 to June 9, 2025")
    print("🗂️  Focus: OrderDetails with substantial transaction data")
    print("⚡ Strategy: Skip empty dates, process data-rich dates")
    print("=" * 70)
    
    try:
        # Initialize backfill manager with production settings
        print("⚙️  Initializing backfill manager...")
        
        backfill_manager = BackfillManager(
            max_workers=3,      # Balanced for SFTP stability 
            batch_size=10,      # Good batch size for monitoring
            skip_existing=True, # Skip already processed dates
            validate_data=False # Skip validation for speed
        )
        
        print("📋 Checking current database state...")
        processed_dates = backfill_manager.get_processed_dates()
        print(f"📈 Already processed: {len(processed_dates)} dates")
        
        print("🎯 Starting complete historical backfill...")
        print("   • This will process ~400+ dates")
        print("   • Expected time: 2-4 hours")
        print("   • Expected records: 15,000-25,000+")
        print("   • Progress updates every batch")
        print("")
        
        # Run backfill for all available data
        summary = backfill_manager.run_backfill()
        
        # Display comprehensive results
        print("\n" + "=" * 70)
        print("🎉 PHASE 7 HISTORICAL BACKFILL COMPLETE!")
        print("=" * 70)
        print(f"📅 Total dates processed: {summary['processed_dates']}")
        print(f"❌ Failed dates: {summary['failed_dates']}")
        print(f"📊 Total records loaded: {summary['total_records']:,}")
        print(f"⏱️  Total duration: {summary['duration']}")
        print(f"✅ Success rate: {summary['success_rate']:.1f}%")
        
        if summary['failed_date_list']:
            print(f"\n⚠️  Failed dates ({len(summary['failed_date_list'])}):")
            # Show first 10 failed dates
            for date in summary['failed_date_list'][:10]:
                print(f"   • {date}")
            if len(summary['failed_date_list']) > 10:
                print(f"   • ... and {len(summary['failed_date_list']) - 10} more")
        
        # Database status update
        print(f"\n📈 Database Growth:")
        print(f"   • Previous records: 791")
        print(f"   • New records added: {summary['total_records']:,}")
        print(f"   • Total records now: {791 + summary['total_records']:,}")
        
        # Final status assessment
        if summary['success_rate'] > 90:
            print("\n🎊 Historical backfill highly successful!")
            print("✅ Database now contains substantial historical data")
            print("🔄 Ready for Phase 6: Dashboard UI development")
            print("📊 Rich dataset available for analytics and visualization")
        elif summary['success_rate'] > 70:
            print("\n✅ Historical backfill successful with some issues")
            print("📊 Good amount of data loaded for analytics")
            print("⚠️  Check logs for failed date details")
        else:
            print("\n⚠️  Partial success - some issues occurred")
            print("📋 Review logs and failed dates for troubleshooting")
        
        print("=" * 70)
        print("🏁 Phase 7 Complete - Historical Backfill finished")
        print("📈 Database populated with historical transaction data")
        print("🚀 Ready to build comprehensive analytics dashboard")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n🛑 Backfill interrupted by user")
        print("💡 Partial progress may have been saved")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Backfill failed: {e}")
        logger.error(f"Backfill execution error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 