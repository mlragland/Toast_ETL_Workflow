#!/usr/bin/env python3
import json
from datetime import datetime
from collections import defaultdict

def get_day_of_week(date_str):
    """Convert YYYYMMDD to day of week."""
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    return date_obj.strftime("%A")

def analyze_failures():
    # Read the backfill results
    with open('backfill_results.json', 'r') as f:
        results = json.load(f)
    
    # Initialize counters and storage
    closed_dates = []
    unknown_dates = []
    
    # Process each failed date
    for date, failures in results['failed_dates'].items():
        empty_tables = [f for f in failures if f.endswith('_empty')]
        num_empty = len(empty_tables)
        day_of_week = get_day_of_week(date)
        
        if num_empty == 6:
            # All tables empty - likely business closure
            closed_dates.append({
                'date': date,
                'day_of_week': day_of_week,
                'num_empty_tables': num_empty,
                'reason': 'closed'
            })
        else:
            # Partial failure - needs investigation
            unknown_dates.append({
                'date': date,
                'day_of_week': day_of_week,
                'num_empty_tables': num_empty,
                'reason': 'unknown'
            })
    
    # Sort both lists by date
    closed_dates.sort(key=lambda x: x['date'])
    unknown_dates.sort(key=lambda x: x['date'])
    
    # Print report
    print("\n=== Business Closure Analysis Report ===\n")
    
    print("1. Confirmed Business Closures (All Tables Empty):")
    print("-" * 80)
    print(f"{'Date':<12} {'Day':<12} {'Empty Tables':<15} {'Reason':<10}")
    print("-" * 80)
    for entry in closed_dates:
        print(f"{entry['date']:<12} {entry['day_of_week']:<12} {entry['num_empty_tables']:<15} {entry['reason']:<10}")
    
    print("\n2. Potential Data Loading Issues (Partial Failures):")
    print("-" * 80)
    print(f"{'Date':<12} {'Day':<12} {'Empty Tables':<15} {'Reason':<10}")
    print("-" * 80)
    for entry in unknown_dates:
        print(f"{entry['date']:<12} {entry['day_of_week']:<12} {entry['num_empty_tables']:<15} {entry['reason']:<10}")
    
    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total failed dates: {len(closed_dates) + len(unknown_dates)}")
    print(f"Confirmed closures: {len(closed_dates)}")
    print(f"Potential data issues: {len(unknown_dates)}")
    
    # Analyze day of week patterns for closures
    day_counts = defaultdict(int)
    for entry in closed_dates:
        day_counts[entry['day_of_week']] += 1
    
    print("\nClosure Patterns by Day of Week:")
    for day, count in sorted(day_counts.items()):
        print(f"{day}: {count} closures")

if __name__ == "__main__":
    analyze_failures() 