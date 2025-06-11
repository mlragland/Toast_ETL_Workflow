"""
Unit tests for BigQuery Loader
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError

from src.loaders.bigquery_loader import BigQueryLoader


class TestBigQueryLoader:
    """Test suite for BigQueryLoader class"""
    
    @pytest.fixture
    def mock_bq_client(self):
        """Mock BigQuery client"""
        with patch('src.loaders.bigquery_loader.bigquery.Client') as mock_client:
            yield mock_client.return_value
    
    @pytest.fixture
    def bq_loader(self, mock_bq_client):
        """BigQuery loader instance with mocked client"""
        loader = BigQueryLoader(
            project_id="test-project",
            dataset_id="test_dataset",
            location="US"
        )
        loader.client = mock_bq_client
        return loader
    
    @pytest.fixture
    def sample_dataframe(self):
        """Sample DataFrame for testing"""
        return pd.DataFrame({
            'GUID': ['guid1', 'guid2', 'guid3'],
            'Name': ['Item 1', 'Item 2', 'Item 3'],
            'Price': [10.99, 15.50, 8.75],
            'CreatedDate': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
        })
    
    def test_initialization(self, mock_bq_client):
        """Test BigQueryLoader initialization"""
        loader = BigQueryLoader(
            project_id="test-project",
            dataset_id="test_dataset",
            location="US"
        )
        
        assert loader.project_id == "test-project"
        assert loader.dataset_id == "test_dataset"
        assert loader.location == "US"
        assert 'all_items_report' in loader.table_schemas
    
    def test_table_schemas_defined(self, bq_loader):
        """Test that all required table schemas are defined"""
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
            assert table in bq_loader.table_schemas
            assert len(bq_loader.table_schemas[table]) > 0
    
    def test_ensure_dataset_exists_already_exists(self, bq_loader, mock_bq_client):
        """Test ensure_dataset_exists when dataset already exists"""
        # Dataset exists, should not create
        mock_bq_client.get_dataset.return_value = Mock()
        
        bq_loader.ensure_dataset_exists()
        
        mock_bq_client.get_dataset.assert_called_once()
        mock_bq_client.create_dataset.assert_not_called()
    
    def test_ensure_dataset_exists_not_found(self, bq_loader, mock_bq_client):
        """Test ensure_dataset_exists when dataset doesn't exist"""
        # Dataset doesn't exist, should create
        mock_bq_client.get_dataset.side_effect = NotFound("Dataset not found")
        mock_dataset = Mock()
        mock_bq_client.create_dataset.return_value = mock_dataset
        
        bq_loader.ensure_dataset_exists()
        
        mock_bq_client.get_dataset.assert_called_once()
        mock_bq_client.create_dataset.assert_called_once()
    
    @patch('src.loaders.bigquery_loader.pd.Timestamp')
    def test_load_dataframe_success(self, mock_timestamp, bq_loader, mock_bq_client, sample_dataframe):
        """Test successful DataFrame loading"""
        # Mock timestamp
        mock_timestamp.now.return_value = pd.Timestamp('2024-01-01 12:00:00', tz='UTC')
        
        # Mock BigQuery components
        mock_job = Mock()
        mock_job.job_id = "test-job-123"
        mock_job.result.return_value = None
        
        mock_table = Mock()
        mock_table.num_rows = 3
        
        mock_bq_client.load_table_from_dataframe.return_value = mock_job
        mock_bq_client.get_table.return_value = mock_table
        mock_bq_client.get_dataset.return_value = Mock()  # Dataset exists
        
        # Test loading
        result = bq_loader.load_dataframe(
            df=sample_dataframe,
            table_name='all_items_report',
            source_file='test.csv'
        )
        
        # Assertions
        assert result['success'] is True
        assert result['rows_loaded'] == 3
        assert result['total_rows'] == 3
        assert result['job_id'] == "test-job-123"
        assert result['source_file'] == 'test.csv'
        
        mock_bq_client.load_table_from_dataframe.assert_called_once()
        mock_job.result.assert_called_once()
    
    def test_load_dataframe_failure(self, bq_loader, mock_bq_client, sample_dataframe):
        """Test DataFrame loading failure"""
        # Mock dataset exists
        mock_bq_client.get_dataset.return_value = Mock()
        
        # Mock load failure
        mock_bq_client.load_table_from_dataframe.side_effect = GoogleCloudError("Load failed")
        
        # Test loading - should raise exception
        with pytest.raises(GoogleCloudError):
            bq_loader.load_dataframe(
                df=sample_dataframe,
                table_name='all_items_report',
                source_file='test.csv'
            )
    
    def test_load_dataframe_adds_metadata_columns(self, bq_loader, mock_bq_client, sample_dataframe):
        """Test that metadata columns are added to DataFrame"""
        # Mock components
        mock_job = Mock()
        mock_job.job_id = "test-job-123"
        mock_job.result.return_value = None
        
        mock_table = Mock()
        mock_table.num_rows = 3
        
        mock_bq_client.load_table_from_dataframe.return_value = mock_job
        mock_bq_client.get_table.return_value = mock_table
        mock_bq_client.get_dataset.return_value = Mock()
        
        with patch('src.loaders.bigquery_loader.pd.Timestamp') as mock_timestamp:
            mock_timestamp.now.return_value = pd.Timestamp('2024-01-01 12:00:00', tz='UTC')
            
            bq_loader.load_dataframe(
                df=sample_dataframe,
                table_name='all_items_report',
                source_file='test.csv'
            )
            
            # Check that load_table_from_dataframe was called with modified DataFrame
            call_args = mock_bq_client.load_table_from_dataframe.call_args
            df_passed = call_args[0][0]  # First positional argument
            
            assert 'loaded_at' in df_passed.columns
            assert 'source_file' in df_passed.columns
            assert all(df_passed['source_file'] == 'test.csv')
    
    def test_get_table_info_exists(self, bq_loader, mock_bq_client):
        """Test get_table_info for existing table"""
        # Mock table
        mock_table = Mock()
        mock_table.num_rows = 100
        mock_table.num_bytes = 1024
        mock_table.created = pd.Timestamp('2024-01-01', tz='UTC')
        mock_table.modified = pd.Timestamp('2024-01-02', tz='UTC')
        mock_table.schema = ['field1', 'field2']
        mock_table.time_partitioning = Mock()
        mock_table.clustering_fields = None
        
        mock_bq_client.get_table.return_value = mock_table
        
        result = bq_loader.get_table_info('test_table')
        
        assert result['table_name'] == 'test_table'
        assert result['num_rows'] == 100
        assert result['num_bytes'] == 1024
        assert result['schema_fields'] == 2
        assert result['partitioned'] is True
        assert result['clustered'] is False
    
    def test_get_table_info_not_found(self, bq_loader, mock_bq_client):
        """Test get_table_info for non-existent table"""
        mock_bq_client.get_table.side_effect = NotFound("Table not found")
        
        result = bq_loader.get_table_info('nonexistent_table')
        
        assert result['table_name'] == 'nonexistent_table'
        assert result['exists'] is False
    
    def test_run_query_success(self, bq_loader, mock_bq_client):
        """Test successful query execution"""
        # Mock query job
        mock_query_job = Mock()
        expected_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        mock_query_job.to_dataframe.return_value = expected_df
        
        mock_bq_client.query.return_value = mock_query_job
        
        result = bq_loader.run_query("SELECT * FROM test_table")
        
        pd.testing.assert_frame_equal(result, expected_df)
        mock_bq_client.query.assert_called_once_with("SELECT * FROM test_table")
    
    def test_run_query_failure(self, bq_loader, mock_bq_client):
        """Test query execution failure"""
        mock_bq_client.query.side_effect = GoogleCloudError("Query failed")
        
        with pytest.raises(GoogleCloudError):
            bq_loader.run_query("SELECT * FROM test_table")
    
    def test_get_pipeline_stats(self, bq_loader):
        """Test pipeline statistics collection"""
        with patch.object(bq_loader, 'get_table_info') as mock_get_table_info:
            mock_get_table_info.return_value = {'table_name': 'test', 'num_rows': 100}
            
            stats = bq_loader.get_pipeline_stats()
            
            assert stats['dataset_id'] == 'test_dataset'
            assert stats['project_id'] == 'test-project'
            assert 'tables' in stats
            
            # Verify get_table_info was called for each table schema
            assert mock_get_table_info.call_count == len(bq_loader.table_schemas)
    
    def test_invalid_table_name(self, bq_loader, sample_dataframe):
        """Test loading with invalid table name"""
        with pytest.raises(ValueError, match="Unknown table"):
            bq_loader.load_dataframe(
                df=sample_dataframe,
                table_name='invalid_table',
                source_file='test.csv'
            )
    
    @pytest.mark.parametrize("table_name,expected_fields", [
        ('all_items_report', ['GUID', 'Name', 'Price']),
        ('check_details', ['GUID', 'EntityType', 'CheckNumber']),
        ('cash_entries', ['GUID', 'CashDrawerGUID', 'Type']),
    ])
    def test_table_schemas_contain_expected_fields(self, bq_loader, table_name, expected_fields):
        """Test that table schemas contain expected fields"""
        schema = bq_loader.table_schemas[table_name]
        schema_field_names = [field.name for field in schema]
        
        for field in expected_fields:
            assert field in schema_field_names
        
        # All tables should have metadata fields
        assert 'loaded_at' in schema_field_names
        assert 'source_file' in schema_field_names 