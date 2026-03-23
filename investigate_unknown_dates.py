#!/usr/bin/env python3
import json
import subprocess
from datetime import datetime
import os
from typing import Dict, List, Tuple
import glob

def get_local_files(date_str: str) -> Dict[str, bool]:
    """Check local data directory for files on a specific date."""
    # Convert date format from YYYYMMDD to YYYY-MM-DD
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    local_date = date_obj.strftime("%Y-%m-%d")
    
    files_present = {
        'check_details': False,
        'all_items_report': False,
        'cash_entries': False,
        'item_selection_details': False,
        'kitchen_timings': False,
        'order_details': False,
        'payment_details': False
    }
    
    # Check each table's data directory
    for table in files_present.keys():
        pattern = f"data/{local_date}/{table}*.csv"
        if glob.glob(pattern):
            files_present[table] = True
    
    return files_present

def check_bigquery_tables(date_str: str) -> Dict[str, int]:
    """Check BigQuery tables for data on a specific date."""
    # Convert date format from YYYYMMDD to YYYY-MM-DD
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    bq_date = date_obj.strftime("%Y-%m-%d")
    
    tables = {
        'check_details': 0,
        'all_items_report': 0,
        'cash_entries': 0,
        'item_selection_details': 0,
        'kitchen_timings': 0,
        'order_details': 0,
        'payment_details': 0
    }
    
    for table in tables.keys():
        query = f"""
        SELECT COUNT(*) as count
        FROM `toast-analytics-444116.toast_analytics.{table}`
        WHERE processing_date = '{bq_date}'
        """
        try:
            result = subprocess.run(
                ["bq", "query", "--nouse_legacy_sql", "--format=prettyjson", query],
                capture_output=True,
                text=True,
                check=True
            )
            data = json.loads(result.stdout)
            tables[table] = int(data[0]['count'])
        except Exception as e:
            print(f"Error querying {table}: {e}")
    
    return tables

def analyze_unknown_dates(report_to_file: bool = False):
    # Read the backfill results
    with open('backfill_results.json', 'r') as f:
        results = json.load(f)
    
    # Get the unknown dates (where num_empty_tables < 6)
    unknown_dates = []
    for date, failures in results['failed_dates'].items():
        empty_tables = [f for f in failures if f.endswith('_empty')]
        if len(empty_tables) < 6:
            unknown_dates.append(date)
    
    report_lines = []
    report_lines.append("\n=== Investigation of Unknown Dates ===\n")
    
    for date in unknown_dates:
        report_lines.append(f"\nInvestigating date: {date}")
        report_lines.append("-" * 80)
        
        # Check local files
        report_lines.append("\nLocal Files Status:")
        report_lines.append("-" * 40)
        local_files = get_local_files(date)
        for table, present in local_files.items():
            report_lines.append(f"{table}: {'Present' if present else 'Not Found'}")
        
        # Check BigQuery tables
        report_lines.append("\nBigQuery Data Status:")
        report_lines.append("-" * 40)
        bq_data = check_bigquery_tables(date)
        for table, count in bq_data.items():
            report_lines.append(f"{table}: {count} records")
        
        # Analyze the results
        report_lines.append("\nAnalysis:")
        report_lines.append("-" * 40)
        load_issues = []
        missing_data = []
        
        for table in local_files.keys():
            if local_files[table] and bq_data[table] == 0:
                load_issues.append(table)
            elif not local_files[table] and bq_data[table] == 0:
                missing_data.append(table)
        
        if load_issues:
            report_lines.append("Potential Load Issues:")
            for table in load_issues:
                report_lines.append(f"- {table}: File present locally but no data in BigQuery")
        
        if missing_data:
            report_lines.append("\nMissing Data (Not in local files or BigQuery):")
            for table in missing_data:
                report_lines.append(f"- {table}: No file locally and no data in BigQuery")
        
        report_lines.append("\n" + "=" * 80)
    
    # Print the report to console
    print("\n".join(report_lines))
    
    # Optionally write the report to a file
    if report_to_file:
        with open('backfill_investigation_report.txt', 'w') as f:
            f.write("\n".join(report_lines))
        print("\nReport saved to backfill_investigation_report.txt")

if __name__ == "__main__":
    analyze_unknown_dates(report_to_file=True) 