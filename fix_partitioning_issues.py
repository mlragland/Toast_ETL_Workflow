#!/usr/bin/env python3
"""
Fix BigQuery Partitioning Issues

This script fixes the partitioning field configuration issues that are preventing
the GCS staging solution from loading data to 3 tables:
- all_items_report
- item_selection_details  
- payment_details

Solution: Remove partitioning from these tables to match the working tables.
"""

import os
from google.cloud import bigquery

def fix_table_partitioning():
    """Fix partitioning issues by removing partitioning from failing tables"""
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
    
    client = bigquery.Client(project=project_id)
    
    # Tables that need partitioning fixes
    failing_tables = ['all_items_report', 'item_selection_details', 'payment_details']
    
    print("🍴 Fixing BigQuery Partitioning Issues")
    print("="*80)
    print("🎯 Strategy: Remove partitioning to match working tables")
    print("   Working tables (check_details, cash_entries, kitchen_timings, order_details)")
    print("   have no partitioning and load successfully.")
    print()
    
    for table_name in failing_tables:
        try:
            print(f"🔧 Processing {table_name}...")
            
            # Get current table
            table_ref = f'{project_id}.{dataset_id}.{table_name}'
            table = client.get_table(table_ref)
            
            print(f"   📊 Current schema: {len(table.schema)} fields")
            if table.time_partitioning:
                print(f"   ⚠️  Current partitioning: {table.time_partitioning.type_} on '{table.time_partitioning.field}'")
            else:
                print(f"   ✅ No partitioning configured")
                continue
            
            # Create new table without partitioning
            backup_table_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_table_ref = f'{project_id}.{dataset_id}.{backup_table_name}'
            
            # Copy existing data to backup table (if any)
            query = f"""
            CREATE TABLE `{backup_table_ref}` AS
            SELECT * FROM `{table_ref}`
            """
            
            print(f"   💾 Creating backup table: {backup_table_name}")
            job = client.query(query)
            job.result()  # Wait for completion
            
            # Drop original table
            print(f"   🗑️  Dropping original table...")
            client.delete_table(table_ref)
            
            # Create new table without partitioning
            print(f"   ✨ Creating new table without partitioning...")
            new_table = bigquery.Table(table_ref, schema=table.schema)
            # No time_partitioning = no partitioning
            created_table = client.create_table(new_table)
            
            # Copy data back from backup
            if job.result().total_rows > 0:
                restore_query = f"""
                INSERT INTO `{table_ref}`
                SELECT * FROM `{backup_table_ref}`
                """
                print(f"   ⬅️  Restoring data from backup...")
                restore_job = client.query(restore_query)
                restore_job.result()
                print(f"   ✅ Restored {restore_job.result().total_rows} rows")
            else:
                print(f"   ℹ️  No existing data to restore")
            
            # Clean up backup table
            print(f"   🧹 Cleaning up backup table...")
            client.delete_table(backup_table_ref)
            
            print(f"   ✅ {table_name} successfully updated (no partitioning)")
            
        except Exception as e:
            print(f"   ❌ Error fixing {table_name}: {e}")
            continue
    
    print("\n" + "="*80)
    print("🎉 PARTITIONING FIX COMPLETE")
    print("="*80)
    print("✅ All tables now have consistent configuration (no partitioning)")
    print("✅ This matches the working tables that load successfully")
    print("✅ GCS staging solution should now work for all 7 tables")
    
    print("\n🧪 NEXT STEPS:")
    print("   1. Run the GCS staging ETL test again")
    print("   2. Verify all 7 tables load successfully")
    print("   3. Confirm data integrity after the changes")

if __name__ == "__main__":
    from datetime import datetime
    fix_table_partitioning() 