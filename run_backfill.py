#!/usr/bin/env python3
"""
Toast ETL Comprehensive Backfill - Date-by-Date Processing Strategy

This script implements the recommended backfill strategy:
1. Date-by-Date Processing: Each date processed individually through complete ETL pipeline
2. Business Closure Detection: Automatic detection and handling of closure dates
3. Complete Date Coverage: Every date gets either real data or closure records
4. Duplicate Prevention: Skip already processed dates automatically
5. Fault Tolerance: Robust error handling and retry mechanisms
6. Operational Insights: Track closure patterns for business intelligence

Usage:
    python run_backfill.py --all                                    # Process all 432+ available dates
    python run_backfill.py --start-date 20240404 --end-date 20240430  # Process date range
    python run_backfill.py --dates 20241225 20250101               # Process specific dates
    python run_backfill.py --dry-run --all                         # Preview what would be processed
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
import json

# Add project root to path for proper imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.backfill.backfill_manager import BackfillManager
    from src.utils.logging_utils import get_logger
    from src.config.settings import settings
except ImportError as e:
    print(f"❌ Import Error: {e}")
    print("Make sure you're running from the project root directory")
    print("Current directory:", os.getcwd())
    print("Expected files:")
    print("  - src/backfill/backfill_manager.py")
    print("  - src/utils/logging_utils.py")
    print("  - src/config/settings.py")
    sys.exit(1)

logger = get_logger(__name__)


def parse_arguments():
    """Parse command line arguments for backfill configuration."""
    parser = argparse.ArgumentParser(
        description="Toast ETL Comprehensive Backfill - Date-by-Date Processing Strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                                    # Process all 432+ available dates
  %(prog)s --start-date 20240404 --end-date 20240430  # Process April 2024
  %(prog)s --dates 20241225 20250101               # Process specific dates
  %(prog)s --dry-run --all                         # Preview processing scope
  %(prog)s --max-workers 5 --batch-size 15 --all   # Custom performance settings
        """
    )
    
    # Processing scope options
    scope_group = parser.add_mutually_exclusive_group(required=True)
    scope_group.add_argument(
        '--all', 
        action='store_true',
        help='Process all available dates from SFTP (432+ dates from April 2024 to June 2025)'
    )
    scope_group.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYYMMDD format (requires --end-date)'
    )
    scope_group.add_argument(
        '--dates',
        nargs='+',
        help='Specific dates to process in YYYYMMDD format'
    )
    
    # Date range option
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYYMMDD format (used with --start-date)'
    )
    
    # Performance configuration
    parser.add_argument(
        '--max-workers',
        type=int,
        default=3,
        help='Maximum number of concurrent processing threads (default: 3)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of dates to process in each batch (default: 10)'
    )
    
    # Processing options
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        default=True,
        help='Skip dates already processed in BigQuery (default: True)'
    )
    parser.add_argument(
        '--no-skip-existing',
        action='store_true',
        help='Process all dates even if already processed'
    )
    parser.add_argument(
        '--validate-data',
        action='store_true',
        help='Run data validation after loading (slower but more thorough)'
    )
    
    # Execution options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be processed without actually running'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        default='backfill_log.json',
        help='Path to save backfill log (default: backfill_log.json)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()


def validate_arguments(args):
    """Validate command line arguments."""
    errors = []
    
    # Validate date range
    if args.start_date and not args.end_date:
        errors.append("--end-date is required when using --start-date")
    
    if args.end_date and not args.start_date:
        errors.append("--start-date is required when using --end-date")
    
    # Validate date formats
    if args.start_date:
        try:
            datetime.strptime(args.start_date, '%Y%m%d')
        except ValueError:
            errors.append(f"Invalid start date format: {args.start_date} (expected YYYYMMDD)")
    
    if args.end_date:
        try:
            datetime.strptime(args.end_date, '%Y%m%d')
        except ValueError:
            errors.append(f"Invalid end date format: {args.end_date} (expected YYYYMMDD)")
    
    if args.dates:
        for date in args.dates:
            try:
                datetime.strptime(date, '%Y%m%d')
            except ValueError:
                errors.append(f"Invalid date format: {date} (expected YYYYMMDD)")
    
    # Validate performance settings
    if args.max_workers < 1 or args.max_workers > 10:
        errors.append("--max-workers must be between 1 and 10")
    
    if args.batch_size < 1 or args.batch_size > 50:
        errors.append("--batch-size must be between 1 and 50")
    
    if errors:
        print("❌ Argument validation errors:")
        for error in errors:
            print(f"   • {error}")
        sys.exit(1)


def print_banner():
    """Print the application banner."""
    print("🍴 Toast ETL Pipeline - Comprehensive Backfill")
    print("=" * 70)
    print("📋 Strategy: Date-by-Date Processing with Business Closure Detection")
    print("📊 Scope: 432+ available dates (April 2024 to June 2025)")
    print("🎯 Goal: Complete data coverage with operational insights")
    print("=" * 70)


def print_configuration(args, backfill_manager):
    """Print the current configuration."""
    print("⚙️  Configuration:")
    print(f"   • Max Workers: {args.max_workers}")
    print(f"   • Batch Size: {args.batch_size}")
    print(f"   • Skip Existing: {not args.no_skip_existing}")
    print(f"   • Validate Data: {args.validate_data}")
    print(f"   • Log File: {args.log_file}")
    print(f"   • Environment: {getattr(settings, 'environment', 'development')}")
    print(f"   • Project ID: {getattr(settings, 'gcp_project_id', 'Not configured')}")
    print(f"   • Dataset: {getattr(settings, 'bigquery_dataset', 'Not configured')}")


def preview_processing_scope(args, backfill_manager):
    """Preview what would be processed in dry-run mode."""
    print("🔍 DRY RUN MODE - Analyzing processing scope...")
    print()
    
    try:
        if args.all:
            print("📂 Scope: All available data from SFTP")
            available_dates = backfill_manager.get_available_sftp_dates()
            dates_to_process = backfill_manager.filter_dates_to_process(available_dates)
        elif args.start_date:
            print(f"📅 Scope: Date range {args.start_date} to {args.end_date}")
            available_dates = backfill_manager.get_date_range(args.start_date, args.end_date)
            dates_to_process = backfill_manager.filter_dates_to_process(available_dates)
        else:
            print(f"🎯 Scope: Specific dates {args.dates}")
            dates_to_process = backfill_manager.filter_dates_to_process(args.dates)
        
        print(f"📊 Processing Analysis:")
        print(f"   • Total dates to process: {len(dates_to_process)}")
        
        if dates_to_process:
            print(f"   • First date: {dates_to_process[0]}")
            print(f"   • Last date: {dates_to_process[-1]}")
            
            # Estimate processing time
            estimated_time_per_date = 3  # minutes
            total_estimated_minutes = len(dates_to_process) * estimated_time_per_date / args.max_workers
            estimated_hours = total_estimated_minutes / 60
            
            print(f"   • Estimated duration: {estimated_hours:.1f} hours")
            print(f"   • Expected records: {len(dates_to_process) * 50:,} - {len(dates_to_process) * 150:,}")
            
            if len(dates_to_process) > 20:
                print(f"   • Sample dates: {dates_to_process[:10]} ... {dates_to_process[-10:]}")
            else:
                print(f"   • All dates: {dates_to_process}")
        else:
            print("   • No new dates to process (all dates already processed)")
        
        print()
        print("💡 To execute this backfill, remove the --dry-run flag")
        
    except Exception as e:
        print(f"❌ Error during dry-run analysis: {e}")
        sys.exit(1)


def monitor_progress(backfill_manager, total_dates):
    """Monitor and display real-time progress."""
    import time
    import threading
    
    def progress_monitor():
        while True:
            try:
                stats = backfill_manager.get_current_stats()
                if stats['total_dates'] == 0:
                    time.sleep(5)
                    continue
                
                progress = stats['progress_percentage']
                print(f"\r📈 Progress: {progress:.1f}% | "
                      f"✅ {stats['processed_dates']} | "
                      f"🏢 {stats['closure_dates']} | "
                      f"❌ {stats['failed_dates']} | "
                      f"📊 {stats['total_records']:,} records", end='', flush=True)
                
                if progress >= 100:
                    break
                    
                time.sleep(10)  # Update every 10 seconds
                
            except Exception:
                break
    
    # Start progress monitor in background thread
    monitor_thread = threading.Thread(target=progress_monitor, daemon=True)
    monitor_thread.start()
    
    return monitor_thread


def main():
    """Main execution function."""
    try:
        # Parse and validate arguments
        args = parse_arguments()
        validate_arguments(args)
        
        # Print banner and configuration
        print_banner()
        
        # Initialize backfill manager with date-by-date strategy
        print("⚙️  Initializing backfill manager...")
        
        skip_existing = not args.no_skip_existing
        backfill_manager = BackfillManager(
            max_workers=args.max_workers,
            batch_size=args.batch_size,
            skip_existing=skip_existing,
            validate_data=args.validate_data
        )
        
        print_configuration(args, backfill_manager)
        print()
        
        # Handle dry-run mode
        if args.dry_run:
            preview_processing_scope(args, backfill_manager)
            return
        
        # Verify environment setup
        print("🔍 Verifying environment setup...")
        try:
            # Test BigQuery connection
            processed_dates = backfill_manager.get_processed_dates()
            print(f"✅ BigQuery connection verified ({len(processed_dates)} dates already processed)")
            
            # Test SFTP availability
            available_dates = backfill_manager.get_available_sftp_dates()
            print(f"✅ SFTP analysis complete ({len(available_dates)} dates available)")
            
        except Exception as e:
            print(f"❌ Environment verification failed: {e}")
            print("Please check your configuration and credentials")
            sys.exit(1)
        
        print()
        
        # Execute backfill based on scope
        print("🚀 Starting comprehensive backfill process...")
        start_time = datetime.now()
        
        try:
            if args.all:
                print("📂 Processing all available data from SFTP...")
                summary = backfill_manager.run_backfill()
            elif args.start_date:
                print(f"📅 Processing date range: {args.start_date} to {args.end_date}")
                summary = backfill_manager.run_backfill(
                    start_date=args.start_date,
                    end_date=args.end_date
                )
            else:
                print(f"🎯 Processing specific dates: {args.dates}")
                summary = backfill_manager.run_backfill(specific_dates=args.dates)
            
            # Save results and display final summary
            backfill_manager.save_backfill_log(summary, args.log_file)
            
            print("\n" + "=" * 70)
            print("🎉 COMPREHENSIVE BACKFILL COMPLETE!")
            print("=" * 70)
            print(f"📅 Total dates processed: {summary['processed_dates'] + summary['closure_dates']}")
            print(f"✅ Successful dates: {summary['processed_dates']}")
            print(f"🏢 Closure dates: {summary['closure_dates']}")
            print(f"❌ Failed dates: {summary['failed_dates']}")
            print(f"📊 Total records loaded: {summary['total_records']:,}")
            print(f"⏱️  Total duration: {summary['duration']}")
            print(f"🎯 Success rate: {summary['success_rate']:.1f}%")
            print(f"📝 Log saved to: {args.log_file}")
            
            # Show closure analysis if any closures detected
            if summary['closure_dates'] > 0:
                print(f"\n🏢 Business Closure Analysis:")
                print(f"   • Total closure dates: {summary['closure_dates']}")
                if summary['closure_date_list']:
                    print(f"   • Sample closure dates: {summary['closure_date_list'][:5]}")
                print(f"   • Closure detection ensures 100% date coverage")
            
            # Show failed dates if any
            if summary['failed_date_list']:
                print(f"\n⚠️  Failed Dates ({len(summary['failed_date_list'])}):")
                for date in summary['failed_date_list'][:10]:
                    print(f"   • {date}")
                if len(summary['failed_date_list']) > 10:
                    print(f"   • ... and {len(summary['failed_date_list']) - 10} more")
                print(f"\n💡 To retry failed dates:")
                print(f"   python run_backfill.py --dates {' '.join(summary['failed_date_list'][:5])}")
            
            # Success indicators
            if summary['success_rate'] >= 95:
                print(f"\n🏆 Excellent success rate! Backfill completed successfully.")
            elif summary['success_rate'] >= 85:
                print(f"\n✅ Good success rate. Consider retrying failed dates.")
            else:
                print(f"\n⚠️  Lower success rate. Please review failed dates and retry.")
            
            print(f"\n📊 Dashboard: Data is now available for analysis")
            print(f"🔗 API: http://localhost:8080/api/dashboard/summary")
            print(f"🌐 Frontend: http://localhost:3000")
            
        except KeyboardInterrupt:
            print(f"\n⏹️  Backfill interrupted by user")
            print(f"📊 Partial results may be available in BigQuery")
            sys.exit(1)
            
        except Exception as e:
            print(f"\n❌ Backfill failed: {e}")
            print(f"📝 Check logs for detailed error information")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 