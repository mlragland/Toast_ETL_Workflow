"""
Tests for Phase 2 Components
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

def test_bigquery_loader_imports():
    """Test that BigQuery loader can be imported"""
    from src.loaders.bigquery_loader import BigQueryLoader
    assert BigQueryLoader is not None

def test_bigquery_loader_initialization():
    """Test BigQuery loader initialization"""
    with patch('src.loaders.bigquery_loader.bigquery.Client'):
        from src.loaders.bigquery_loader import BigQueryLoader
        loader = BigQueryLoader(
            project_id="test-project",
            dataset_id="test_dataset"
        )
        assert loader.project_id == "test-project"
        assert loader.dataset_id == "test_dataset"

def test_table_schemas_exist():
    """Test that table schemas are defined"""
    with patch('src.loaders.bigquery_loader.bigquery.Client'):
        from src.loaders.bigquery_loader import BigQueryLoader
        loader = BigQueryLoader()
        
        expected_tables = [
            'all_items_report',
            'check_details', 
            'cash_entries',
            'item_selection_details',
            'kitchen_timings',
            'order_details',
            'payment_details'
        ]
        
        for table in expected_tables:
            assert table in loader.table_schemas 