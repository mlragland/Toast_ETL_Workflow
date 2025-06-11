#!/usr/bin/env python3
"""Quick schema checker for order_details table."""

import os
from google.cloud import bigquery

# Configuration
project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
dataset_id = os.getenv('DATASET_ID', 'toast_analytics')

client = bigquery.Client(project=project_id)
table = client.get_table(f'{project_id}.{dataset_id}.order_details')

print('Column names in order_details:')
for field in table.schema:
    print(f'  - {field.name} ({field.field_type})')

print(f'\nTotal columns: {len(table.schema)}')
print(f'Total rows: {table.num_rows}')

# Check first few rows to see actual data
print('\nSample data (first 3 rows):')
query = f"""
SELECT *
FROM `{project_id}.{dataset_id}.order_details`
LIMIT 3
"""

result = client.query(query).result()
for i, row in enumerate(result):
    print(f'Row {i+1}: {dict(row)}')
    if i >= 2:  # Only show first 3 rows
        break 