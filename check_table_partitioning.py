#!/usr/bin/env python3
"""
Check BigQuery Table Partitioning Configuration

This script checks the current partitioning configuration of BigQuery tables
to identify the partitioning field issues preventing data loading.
"""

import os
from google.cloud import bigquery

def check_table_partitioning():
    """Check partitioning configuration for all tables"""
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
    
    client = bigquery.Client(project=project_id)
    
    # Focus on the 3 failing tables
    failing_tables = ['all_items_report', 'item_selection_details', 'payment_details']
    working_tables = ['check_details', 'cash_entries', 'kitchen_timings', 'order_details']
    
    print("🍴 BigQuery Table Partitioning Analysis")
    print("="*80)
    
    print("\n❌ FAILING TABLES (Partitioning field not found):")
    print("-" * 60)
    
    for table_name in failing_tables:
        try:
            table = client.get_table(f'{project_id}.{dataset_id}.{table_name}')
            print(f"\n🔧 {table_name.upper()}:")
            
            if table.time_partitioning:
                print(f"   📅 Partitioning Type: {table.time_partitioning.type_}")
                print(f"   🎯 Partition Field: '{table.time_partitioning.field}'")
            else:
                print(f"   ❌ No partitioning configured")
            
            print(f"   📋 Schema Fields:")
            for field in table.schema:
                field_type = f"{field.field_type}"
                if field.mode != 'NULLABLE':
                    field_type += f" ({field.mode})"
                print(f"      • {field.name}: {field_type}")
                
        except Exception as e:
            print(f"❌ Error checking {table_name}: {e}")
    
    print("\n✅ WORKING TABLES (For comparison):")
    print("-" * 60)
    
    for table_name in working_tables:
        try:
            table = client.get_table(f'{project_id}.{dataset_id}.{table_name}')
            print(f"\n🔧 {table_name.upper()}:")
            
            if table.time_partitioning:
                print(f"   📅 Partitioning Type: {table.time_partitioning.type_}")
                print(f"   🎯 Partition Field: '{table.time_partitioning.field}'")
            else:
                print(f"   ❌ No partitioning configured")
                
            # Check if partitioning field exists in schema
            if table.time_partitioning and table.time_partitioning.field:
                partition_field = table.time_partitioning.field
                field_exists = any(f.name == partition_field for f in table.schema)
                print(f"   ✅ Partition field exists in schema: {field_exists}")
            
        except Exception as e:
            print(f"❌ Error checking {table_name}: {e}")
    
    print("\n" + "="*80)
    print("🔍 ANALYSIS SUMMARY:")
    print("="*80)
    
    print("\nThe error 'field specified for partitioning cannot be found in the schema'")
    print("indicates that BigQuery tables are configured with partitioning fields that")
    print("don't exist in the actual CSV data being loaded.")
    print("\n💡 LIKELY CAUSES:")
    print("   1. Tables configured to partition on 'loaded_at' field")
    print("   2. CSV data contains 'processing_date' field instead")
    print("   3. Schema mismatch between table definition and actual data")
    print("\n🔧 SOLUTIONS:")
    print("   1. Update table partitioning to use 'processing_date' field")
    print("   2. OR modify CSV transformation to include 'loaded_at' field")
    print("   3. OR remove partitioning entirely for problematic tables")

if __name__ == "__main__":
    check_table_partitioning() 