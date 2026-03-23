#!/usr/bin/env python3
"""
Recreate BigQuery Tables with Correct Schemas

This script drops and recreates BigQuery tables with the corrected schemas
that match the legacy implementation.
"""

import os
import sys
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from validators.schema_enforcer import SchemaEnforcer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bigquery_client():
    """Initialize BigQuery client."""
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    return bigquery.Client(project=project_id)

def convert_schema_to_bigquery_fields(schema_definition):
    """Convert schema definition to BigQuery SchemaField objects."""
    fields = []
    for field_def in schema_definition:
        # Map our types to BigQuery types
        bq_type = field_def['type']
        if bq_type == 'DATETIME':
            bq_type = 'TIMESTAMP'
        
        mode = field_def.get('mode', 'NULLABLE')
        
        field = bigquery.SchemaField(
            name=field_def['name'],
            field_type=bq_type,
            mode=mode
        )
        fields.append(field)
    
    return fields

def recreate_table(client, dataset_id, table_name, new_schema):
    """Drop and recreate a BigQuery table with new schema."""
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    
    try:
        # Try to delete existing table
        try:
            client.delete_table(table_id)
            logger.info(f"🗑️  Deleted existing table: {table_name}")
        except NotFound:
            logger.info(f"📋 Table {table_name} doesn't exist, creating new")
        
        # Convert schema to BigQuery fields
        new_fields = convert_schema_to_bigquery_fields(new_schema)
        
        # Create table with new schema
        table = bigquery.Table(table_id, schema=new_fields)
        
        # Add partitioning on processing_date if it exists
        processing_date_field = next((f for f in new_fields if f.name == 'processing_date'), None)
        if processing_date_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field='processing_date'
            )
            logger.info(f"   📅 Added date partitioning on processing_date")
        
        created_table = client.create_table(table)
        
        logger.info(f"✅ Created table: {table_name}")
        logger.info(f"   📊 Schema has {len(new_fields)} fields")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error recreating table {table_name}: {e}")
        return False

def main():
    """Main function to recreate all tables."""
    logger.info("🔄 Recreating BigQuery tables with corrected schemas...")
    logger.info("⚠️  WARNING: This will delete all existing data!")
    
    # Confirm with user
    response = input("Are you sure you want to recreate all tables? This will delete existing data! (y/N): ")
    if response.lower() != 'y':
        logger.info("❌ Operation cancelled")
        return False
    
    # Initialize components
    client = get_bigquery_client()
    schema_enforcer = SchemaEnforcer()
    dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
    
    # Table name mappings (CSV filename to BigQuery table name)
    table_mappings = {
        'AllItemsReport.csv': 'all_items_report',
        'CheckDetails.csv': 'check_details', 
        'CashEntries.csv': 'cash_entries',
        'ItemSelectionDetails.csv': 'item_selection_details',
        'KitchenTimings.csv': 'kitchen_timings',
        'OrderDetails.csv': 'order_details',
        'PaymentDetails.csv': 'payment_details'
    }
    
    # Track results
    created_tables = []
    failed_tables = []
    
    # Recreate each table
    for csv_filename, table_name in table_mappings.items():
        logger.info(f"\n📋 Processing {csv_filename} → {table_name}")
        
        # Get schema definition
        schema_definition = schema_enforcer.get_schema_for_file(csv_filename)
        if not schema_definition:
            logger.error(f"❌ No schema definition found for {csv_filename}")
            failed_tables.append(table_name)
            continue
        
        # Recreate table
        success = recreate_table(client, dataset_id, table_name, schema_definition)
        
        if success:
            created_tables.append(table_name)
        else:
            failed_tables.append(table_name)
    
    # Summary
    logger.info(f"\n📊 Table Recreation Summary:")
    logger.info(f"✅ Successfully created: {len(created_tables)} tables")
    for table in created_tables:
        logger.info(f"   - {table}")
    
    if failed_tables:
        logger.info(f"❌ Failed to create: {len(failed_tables)} tables")
        for table in failed_tables:
            logger.info(f"   - {table}")
    
    logger.info(f"\n🎯 Key Schema Changes Applied:")
    logger.info(f"   - CheckDetails.check_number: STRING → INTEGER")
    logger.info(f"   - CashEntries.entry_id: NULLABLE → REQUIRED")
    logger.info(f"   - ItemSelectionDetails.order_number: STRING → INTEGER")
    logger.info(f"   - ItemSelectionDetails.dining_option_tax: FLOAT → STRING")
    logger.info(f"   - KitchenTimings.check_number: STRING → INTEGER")
    logger.info(f"   - OrderDetails.duration_opened_to_paid: STRING → TIME")
    
    if len(created_tables) == len(table_mappings):
        logger.info(f"\n🎉 All tables recreated successfully!")
        logger.info(f"💡 You can now run the May 2025 backfill with all tables.")
        logger.info(f"🚀 Next step: python test_schema_fixes.py")
        return True
    else:
        logger.warning(f"\n⚠️  Some tables failed to recreate.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 