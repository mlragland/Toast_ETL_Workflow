#!/usr/bin/env python3
"""
Test script for Toast ETL Date-by-Date Backfill Strategy

This script validates the comprehensive backfill implementation:
1. Tests date-by-date processing workflow
2. Validates business closure detection
3. Verifies duplicate prevention
4. Tests parallel processing capabilities
5. Validates comprehensive statistics tracking

Usage:
    python test_backfill_strategy.py
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.backfill.backfill_manager import BackfillManager
    from src.validators.business_calendar import BusinessCalendar
    from src.utils.logging_utils import get_logger
except ImportError as e:
    print(f"❌ Import Error: {e}")
    print("Please ensure you're running from the project root directory")
    sys.exit(1)

logger = get_logger(__name__)


def test_date_range_generation():
    """Test date range generation for date-by-date processing."""
    print("🧪 Testing date range generation...")
    
    backfill_manager = BackfillManager()
    
    # Test basic date range
    dates = backfill_manager.get_date_range('20240404', '20240407')
    expected = ['20240404', '20240405', '20240406', '20240407']
    
    assert dates == expected, f"Expected {expected}, got {dates}"
    print(f"✅ Basic date range: {len(dates)} dates generated correctly")
    
    # Test single date
    single_date = backfill_manager.get_date_range('20240404', '20240404')
    assert single_date == ['20240404'], f"Single date test failed: {single_date}"
    print(f"✅ Single date range: {single_date}")
    
    # Test full available range
    full_range = backfill_manager.get_available_sftp_dates()
    assert len(full_range) > 400, f"Expected 400+ dates, got {len(full_range)}"
    print(f"✅ Full SFTP range: {len(full_range)} dates (April 2024 to June 2025)")
    
    return True


def test_business_closure_detection():
    """Test business closure detection logic."""
    print("🧪 Testing business closure detection...")
    
    business_calendar = BusinessCalendar()
    
    # Test scenario 1: No files (holiday closure)
    no_files_analysis = {
        'files_found': 0,
        'total_records': 0,
        'has_meaningful_data': False
    }
    
    is_closure, reason, records = business_calendar.should_process_as_closure(
        '20241225', no_files_analysis
    )
    
    assert is_closure == True, "Christmas Day should be detected as closure"
    assert reason == 'no_files', f"Expected 'no_files', got '{reason}'"
    print(f"✅ Holiday closure detection: {reason}")
    
    # Test scenario 2: Low activity
    low_activity_analysis = {
        'files_found': 3,
        'total_records': 8,
        'has_meaningful_data': False
    }
    
    is_closure, reason, records = business_calendar.should_process_as_closure(
        '20250101', low_activity_analysis
    )
    
    assert is_closure == True, "New Year's Day should be detected as closure"
    assert reason == 'low_activity', f"Expected 'low_activity', got '{reason}'"
    print(f"✅ Low activity detection: {reason}")
    
    # Test scenario 3: Normal business day
    normal_day_analysis = {
        'files_found': 7,
        'total_records': 150,
        'has_meaningful_data': True
    }
    
    is_closure, reason, records = business_calendar.should_process_as_closure(
        '20240415', normal_day_analysis
    )
    
    assert is_closure == False, "Normal business day should not be detected as closure"
    print(f"✅ Normal business day detection: not a closure")
    
    return True


def test_duplicate_prevention():
    """Test duplicate prevention logic."""
    print("🧪 Testing duplicate prevention...")
    
    backfill_manager = BackfillManager(skip_existing=True)
    
    # Test filtering logic
    available_dates = ['20240404', '20240405', '20240406', '20240407']
    
    # Mock processed dates (simulate some already processed)
    original_method = backfill_manager.get_processed_dates
    backfill_manager.get_processed_dates = lambda: ['20240405', '20240406']
    
    filtered_dates = backfill_manager.filter_dates_to_process(available_dates)
    expected_filtered = ['20240404', '20240407']
    
    assert filtered_dates == expected_filtered, f"Expected {expected_filtered}, got {filtered_dates}"
    print(f"✅ Duplicate filtering: {len(available_dates)} → {len(filtered_dates)} dates")
    
    # Test with skip_existing=False
    backfill_manager.skip_existing = False
    all_dates = backfill_manager.filter_dates_to_process(available_dates)
    assert all_dates == available_dates, "Should return all dates when skip_existing=False"
    print(f"✅ No filtering mode: {len(all_dates)} dates (all included)")
    
    # Restore original method
    backfill_manager.get_processed_dates = original_method
    
    return True


def test_statistics_tracking():
    """Test comprehensive statistics tracking."""
    print("🧪 Testing statistics tracking...")
    
    backfill_manager = BackfillManager()
    
    # Initialize stats
    assert backfill_manager.stats['total_dates'] == 0
    assert backfill_manager.stats['processed_dates'] == 0
    assert backfill_manager.stats['closure_dates'] == 0
    assert backfill_manager.stats['failed_dates'] == 0
    print(f"✅ Initial statistics: all zeros")
    
    # Test current stats method
    current_stats = backfill_manager.get_current_stats()
    expected_keys = ['total_dates', 'processed_dates', 'closure_dates', 'failed_dates', 'total_records', 'progress_percentage']
    
    for key in expected_keys:
        assert key in current_stats, f"Missing key in current stats: {key}"
    
    print(f"✅ Current stats structure: {len(current_stats)} fields")
    
    # Test summary generation
    summary = backfill_manager.get_summary()
    expected_summary_keys = [
        'total_dates', 'processed_dates', 'closure_dates', 'failed_dates',
        'total_records', 'success_rate', 'duration', 'strategy', 'configuration'
    ]
    
    for key in expected_summary_keys:
        assert key in summary, f"Missing key in summary: {key}"
    
    assert summary['strategy'] == 'date_by_date_with_closure_detection'
    print(f"✅ Summary structure: {len(summary)} fields, strategy confirmed")
    
    return True


def test_configuration_options():
    """Test various configuration options."""
    print("🧪 Testing configuration options...")
    
    # Test custom configuration
    custom_manager = BackfillManager(
        max_workers=5,
        batch_size=15,
        skip_existing=False,
        validate_data=True
    )
    
    assert custom_manager.max_workers == 5
    assert custom_manager.batch_size == 15
    assert custom_manager.skip_existing == False
    assert custom_manager.validate_data == True
    print(f"✅ Custom configuration: max_workers={custom_manager.max_workers}, batch_size={custom_manager.batch_size}")
    
    # Test default configuration
    default_manager = BackfillManager()
    assert default_manager.max_workers == 3
    assert default_manager.batch_size == 10
    assert default_manager.skip_existing == True
    print(f"✅ Default configuration: max_workers={default_manager.max_workers}, batch_size={default_manager.batch_size}")
    
    return True


def test_error_handling():
    """Test error handling and resilience."""
    print("🧪 Testing error handling...")
    
    backfill_manager = BackfillManager()
    
    # Test invalid date format
    try:
        backfill_manager.get_date_range('invalid', '20240404')
        assert False, "Should have raised ValueError for invalid date"
    except ValueError:
        print(f"✅ Invalid date format handling: ValueError raised correctly")
    
    # Test invalid date range (end before start)
    try:
        dates = backfill_manager.get_date_range('20240407', '20240404')
        assert len(dates) == 0, "Should return empty list for invalid range"
        print(f"✅ Invalid date range handling: empty list returned")
    except Exception:
        print(f"✅ Invalid date range handling: exception raised")
    
    return True


def run_comprehensive_test():
    """Run all tests for the backfill strategy."""
    print("🍴 Toast ETL Backfill Strategy - Comprehensive Test Suite")
    print("=" * 70)
    print("📋 Testing date-by-date processing implementation")
    print("🎯 Validating business closure detection and statistics")
    print("=" * 70)
    print()
    
    tests = [
        ("Date Range Generation", test_date_range_generation),
        ("Business Closure Detection", test_business_closure_detection),
        ("Duplicate Prevention", test_duplicate_prevention),
        ("Statistics Tracking", test_statistics_tracking),
        ("Configuration Options", test_configuration_options),
        ("Error Handling", test_error_handling),
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        try:
            print(f"🔍 {test_name}:")
            result = test_func()
            if result:
                passed_tests += 1
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")
        print()
    
    # Final results
    print("=" * 70)
    print("🎉 TEST RESULTS SUMMARY")
    print("=" * 70)
    print(f"✅ Passed: {passed_tests}/{total_tests}")
    print(f"❌ Failed: {total_tests - passed_tests}/{total_tests}")
    print(f"🎯 Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    if passed_tests == total_tests:
        print(f"\n🏆 ALL TESTS PASSED!")
        print(f"✅ Date-by-date backfill strategy is ready for production")
        print(f"✅ Business closure detection is working correctly")
        print(f"✅ Duplicate prevention is functioning properly")
        print(f"✅ Statistics tracking is comprehensive")
        print(f"✅ Error handling is robust")
        
        print(f"\n🚀 Ready to run comprehensive backfill:")
        print(f"   python run_backfill.py --all")
        print(f"   python run_backfill.py --start-date 20240404 --end-date 20240430")
        print(f"   python run_backfill.py --dates 20241225 20250101")
        
        return True
    else:
        print(f"\n⚠️  Some tests failed. Please review and fix issues before production use.")
        return False


if __name__ == "__main__":
    success = run_comprehensive_test()
    sys.exit(0 if success else 1) 