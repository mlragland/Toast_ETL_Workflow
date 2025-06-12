#!/usr/bin/env python3
"""
Update BigQuery Tables for Business Closure Detection

Adds closure_indicator and closure_reason fields to all existing tables
to support business closure detection and zero-record generation.
"""

import os
import sys
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def update_table_schema(client, project_id, dataset_id, table_name):
    """Add closure indicator fields to an existing table."""
    
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    try:
        # Get current table
        table = client.get_table(table_id)
        print(f"📋 Updating {table_name}...")
        
        # Get current schema
        current_schema = list(table.schema)
        
        # Check if closure fields already exist
        field_names = [field.name for field in current_schema]
        
        if 'closure_indicator' in field_names and 'closure_reason' in field_names:
            print(f"   ✅ {table_name} already has closure fields")
            return True
        
        # Add new fields
        new_fields = []
        
        if 'closure_indicator' not in field_names:
            new_fields.append(bigquery.SchemaField(
                'closure_indicator', 
                'BOOLEAN', 
                mode='NULLABLE',
                description='Indicates if this record represents a business closure day'
            ))
        
        if 'closure_reason' not in field_names:
            new_fields.append(bigquery.SchemaField(
                'closure_reason', 
                'STRING', 
                mode='NULLABLE',
                description='Reason for business closure (low_activity, no_files, etc.)'
            ))
        
        if new_fields:
            # Update schema
            updated_schema = current_schema + new_fields
            table.schema = updated_schema
            
            # Apply the update
            updated_table = client.update_table(table, ["schema"])
            
            print(f"   ✅ Added {len(new_fields)} closure fields to {table_name}")
            for field in new_fields:
                print(f"      • {field.name} ({field.field_type})")
            
            return True
        else:
            print(f"   ✅ {table_name} already up to date")
            return True
            
    except NotFound:
        print(f"   ❌ Table {table_name} not found")
        return False
    except Exception as e:
        print(f"   ❌ Error updating {table_name}: {str(e)}")
        return False

def main():
    """Update all tables with closure detection fields."""
    
    # Configuration
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
    
    print("🍴 Toast ETL - Adding Business Closure Detection Fields")
    print("=" * 65)
    print(f"📊 Project: {project_id}")
    print(f"📁 Dataset: {dataset_id}")
    print("=" * 65)
    
    # Initialize BigQuery client
    try:
        client = bigquery.Client(project=project_id)
        print("✅ BigQuery client initialized")
    except Exception as e:
        print(f"❌ Failed to initialize BigQuery client: {str(e)}")
        return False
    
    # Tables to update
    tables = [
        'all_items_report',
        'check_details', 
        'cash_entries',
        'item_selection_details',
        'kitchen_timings',
        'order_details',
        'payment_details'
    ]
    
    print(f"\n📋 Updating {len(tables)} tables...")
    print("-" * 40)
    
    successful_updates = 0
    failed_updates = 0
    
    for table_name in tables:
        success = update_table_schema(client, project_id, dataset_id, table_name)
        if success:
            successful_updates += 1
        else:
            failed_updates += 1
    
    print("-" * 40)
    print(f"📈 Update Summary:")
    print(f"   ✅ Successful: {successful_updates}/{len(tables)}")
    print(f"   ❌ Failed: {failed_updates}/{len(tables)}")
    
    if successful_updates == len(tables):
        print("\n🎯 All tables updated successfully!")
        print("\n📝 Next Steps:")
        print("   1. 🔄 Update ETL pipeline to detect business closures")
        print("   2. 📊 Generate zero records for closure dates")
        print("   3. 🎨 Update dashboard to show closure insights")
        print("   4. 📈 Test with historical data")
        
        # Show example closure record structure
        print("\n💡 Example Closure Record Structure:")
        print("   {")
        print("     'order_id': 'CLOSURE_RECORD',")
        print("     'location': 'Business Closed',")
        print("     'total': 0.0,")
        print("     'processing_date': '2024-12-25',")
        print("     'closure_indicator': True,")
        print("     'closure_reason': 'low_activity'")
        print("   }")
        
        # Show updated query examples
        print("\n📊 Updated Query Examples:")
        print("\n   Business Metrics (Exclude Closures):")
        print("   SELECT SUM(total) FROM order_details")
        print("   WHERE (closure_indicator IS NULL OR closure_indicator = FALSE)")
        
        print("\n   Closure Analysis:")
        print("   SELECT processing_date, closure_reason, COUNT(*)")
        print("   FROM order_details WHERE closure_indicator = TRUE")
        print("   GROUP BY processing_date, closure_reason")
        
        return True
    else:
        print(f"\n⚠️  {failed_updates} tables failed to update")
        print("   Check the errors above and retry if needed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 