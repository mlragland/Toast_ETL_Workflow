#!/usr/bin/env python3
"""
Update BigQuery Table Schemas

This script updates the BigQuery table schemas to match the corrected legacy schemas,
resolving the schema discrepancies identified in the analysis.
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

def update_table_schema(client, dataset_id, table_name, new_schema):
    """Update a BigQuery table schema."""
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    
    try:
        # Get existing table
        table = client.get_table(table_id)
        logger.info(f"Found existing table: {table_id}")
        
        # Convert schema to BigQuery fields
        new_fields = convert_schema_to_bigquery_fields(new_schema)
        
        # Update table schema
        table.schema = new_fields
        updated_table = client.update_table(table, ["schema"])
        
        logger.info(f"✅ Updated schema for table: {table_name}")
        logger.info(f"   New schema has {len(new_fields)} fields")
        
        return True
        
    except NotFound:
        logger.warning(f"⚠️  Table not found: {table_id}")
        return False
    except Exception as e:
        logger.error(f"❌ Error updating table {table_name}: {e}")
        return False

def main():
    """Main function to update all table schemas."""
    logger.info("🔄 Starting BigQuery schema updates...")
    
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
    updated_tables = []
    failed_tables = []
    
    # Update each table schema
    for csv_filename, table_name in table_mappings.items():
        logger.info(f"\n📋 Processing {csv_filename} → {table_name}")
        
        # Get schema definition
        schema_definition = schema_enforcer.get_schema_for_file(csv_filename)
        if not schema_definition:
            logger.error(f"❌ No schema definition found for {csv_filename}")
            failed_tables.append(table_name)
            continue
        
        # Update table schema
        success = update_table_schema(client, dataset_id, table_name, schema_definition)
        
        if success:
            updated_tables.append(table_name)
        else:
            failed_tables.append(table_name)
    
    # Summary
    logger.info(f"\n📊 Schema Update Summary:")
    logger.info(f"✅ Successfully updated: {len(updated_tables)} tables")
    for table in updated_tables:
        logger.info(f"   - {table}")
    
    if failed_tables:
        logger.info(f"❌ Failed to update: {len(failed_tables)} tables")
        for table in failed_tables:
            logger.info(f"   - {table}")
    
    logger.info(f"\n🎯 Key Schema Changes Applied:")
    logger.info(f"   - CheckDetails.check_number: STRING → INTEGER")
    logger.info(f"   - CashEntries.entry_id: NULLABLE → REQUIRED")
    logger.info(f"   - ItemSelectionDetails.order_number: STRING → INTEGER")
    logger.info(f"   - ItemSelectionDetails.dining_option_tax: FLOAT → STRING")
    logger.info(f"   - KitchenTimings.check_number: STRING → INTEGER")
    logger.info(f"   - OrderDetails.duration_opened_to_paid: STRING → TIME")
    
    if len(updated_tables) == len(table_mappings):
        logger.info(f"\n🎉 All table schemas updated successfully!")
        logger.info(f"💡 You can now run the May 2025 backfill with all tables.")
        return True
    else:
        logger.warning(f"\n⚠️  Some table schemas failed to update.")
        logger.info(f"💡 OrderDetails table should still work for backfill.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 