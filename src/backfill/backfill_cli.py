#!/usr/bin/env python3
"""
CLI interface for Toast ETL Historical Backfill.
Provides command-line access to backfill operations.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from backfill.backfill_manager import BackfillManager
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Toast ETL Historical Backfill Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill all available data
  python backfill_cli.py --all

  # Backfill specific date range
  python backfill_cli.py --start-date 20240404 --end-date 20240430

  # Backfill specific dates
  python backfill_cli.py --dates 20240607 20240608 20240609

  # Backfill with custom settings
  python backfill_cli.py --all --max-workers 5 --batch-size 20 --no-validate

  # Dry run (show what would be processed)
  python backfill_cli.py --all --dry-run
        """
    )
    
    # Date selection options (mutually exclusive)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        '--all',
        action='store_true',
        help='Process all available dates from SFTP'
    )
    date_group.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYYMMDD format'
    )
    date_group.add_argument(
        '--dates',
        nargs='+',
        help='Specific dates to process (YYYYMMDD format)'
    )
    
    # End date for range processing
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYYMMDD format (required with --start-date)'
    )
    
    # Processing options
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
    parser.add_argument(
        '--no-skip-existing',
        action='store_true',
        help='Process dates even if already in database'
    )
    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='Skip data validation after loading'
    )
    
    # Other options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without actually processing'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        default='backfill_log.json',
        help='Name of log file to save results (default: backfill_log.json)'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()


def validate_date_format(date_str: str) -> bool:
    """Validate date string format."""
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False


def main():
    """Main CLI function."""
    args = parse_arguments()
    
    # Validate arguments
    if args.start_date and not args.end_date:
        print("Error: --end-date is required when using --start-date")
        sys.exit(1)
    
    # Validate date formats
    dates_to_validate = []
    if args.start_date:
        dates_to_validate.append(args.start_date)
    if args.end_date:
        dates_to_validate.append(args.end_date)
    if args.dates:
        dates_to_validate.extend(args.dates)
    
    for date_str in dates_to_validate:
        if not validate_date_format(date_str):
            print(f"Error: Invalid date format '{date_str}'. Use YYYYMMDD format.")
            sys.exit(1)
    
    try:
        # Initialize backfill manager
        print("🚀 Initializing Toast ETL Historical Backfill...")
        
        backfill_manager = BackfillManager(
            max_workers=args.max_workers,
            batch_size=args.batch_size,
            skip_existing=not args.no_skip_existing,
            validate_data=not args.no_validate
        )
        
        # Determine what to process
        if args.dry_run:
            print("🔍 DRY RUN MODE - No actual processing will occur")
            
            if args.all:
                available_dates = backfill_manager.get_available_sftp_dates()
                dates_to_process = backfill_manager.filter_dates_to_process(available_dates)
            elif args.start_date:
                available_dates = backfill_manager.get_date_range(args.start_date, args.end_date)
                dates_to_process = backfill_manager.filter_dates_to_process(available_dates)
            else:
                dates_to_process = backfill_manager.filter_dates_to_process(args.dates)
            
            print(f"📊 Would process {len(dates_to_process)} dates:")
            if dates_to_process:
                print(f"   First date: {dates_to_process[0]}")
                print(f"   Last date: {dates_to_process[-1]}")
                if len(dates_to_process) > 10:
                    print(f"   Sample dates: {dates_to_process[:5]} ... {dates_to_process[-5:]}")
                else:
                    print(f"   All dates: {dates_to_process}")
            else:
                print("   No new dates to process")
            
            return
        
        # Run backfill
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
        
        # Save and display results
        backfill_manager.save_backfill_log(summary, args.log_file)
        
        print("\n" + "="*60)
        print("📊 BACKFILL SUMMARY")
        print("="*60)
        print(f"📅 Total dates: {summary['total_dates']}")
        print(f"✅ Processed: {summary['processed_dates']}")
        print(f"❌ Failed: {summary['failed_dates']}")
        print(f"📈 Total records: {summary['total_records']:,}")
        print(f"⏱️  Duration: {summary['duration']}")
        print(f"📊 Success rate: {summary['success_rate']:.1f}%")
        
        if summary['failed_date_list']:
            print(f"❌ Failed dates: {summary['failed_date_list']}")
        
        print("="*60)
        
        if summary['failed_dates'] > 0:
            print("⚠️  Some dates failed to process. Check logs for details.")
            sys.exit(1)
        else:
            print("🎉 Backfill completed successfully!")
            
    except KeyboardInterrupt:
        print("\n🛑 Backfill interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Backfill failed: {e}")
        logger.error(f"Backfill CLI error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 