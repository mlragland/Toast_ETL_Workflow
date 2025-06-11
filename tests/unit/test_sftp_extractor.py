"""Unit tests for SFTP extractor."""

import os
import tempfile
import pytest
from unittest.mock import Mock, patch, MagicMock

from src.extractors.sftp_extractor import SFTPExtractor


class TestSFTPExtractor:
    """Test cases for SFTP extractor."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.extractor = SFTPExtractor(
            sftp_user="test_user",
            sftp_server="test_server",
            ssh_key_path="/tmp/test_key",
            local_dir=self.temp_dir
        )
    
    def test_init(self):
        """Test extractor initialization."""
        assert self.extractor.sftp_user == "test_user"
        assert self.extractor.sftp_server == "test_server"
        assert self.extractor.ssh_key_path == "/tmp/test_key"
        assert self.extractor.local_dir == self.temp_dir
    
    @patch('os.path.exists')
    def test_validate_ssh_key_not_found(self, mock_exists):
        """Test SSH key validation when key doesn't exist."""
        mock_exists.return_value = False
        
        result = self.extractor._validate_ssh_key()
        
        assert result is False
        mock_exists.assert_called_once_with("/tmp/test_key")
    
    @patch('os.stat')
    @patch('os.path.exists')
    def test_validate_ssh_key_success(self, mock_exists, mock_stat):
        """Test SSH key validation when key exists with correct permissions."""
        mock_exists.return_value = True
        mock_stat.return_value.st_mode = 0o100600  # -rw-------
        
        result = self.extractor._validate_ssh_key()
        
        assert result is True
        mock_exists.assert_called_once_with("/tmp/test_key")
        mock_stat.assert_called_once_with("/tmp/test_key")
    
    def test_list_downloaded_files_empty_directory(self):
        """Test listing files from empty directory."""
        # Create empty directory
        test_dir = os.path.join(self.temp_dir, "test_date")
        os.makedirs(test_dir)
        
        files = self.extractor._list_downloaded_files(test_dir)
        
        assert files == []
    
    def test_list_downloaded_files_with_csv_files(self):
        """Test listing CSV files from directory."""
        # Create directory with test files
        test_dir = os.path.join(self.temp_dir, "test_date")
        os.makedirs(test_dir)
        
        # Create test files
        csv_file = os.path.join(test_dir, "test.csv")
        txt_file = os.path.join(test_dir, "test.txt")
        
        with open(csv_file, 'w') as f:
            f.write("test,data\n")
        with open(txt_file, 'w') as f:
            f.write("test data")
        
        files = self.extractor._list_downloaded_files(test_dir)
        
        assert files == ["test.csv"]
    
    def test_get_file_info(self):
        """Test getting file information."""
        # Create directory with test file
        test_dir = os.path.join(self.temp_dir, "20241210")
        os.makedirs(test_dir)
        
        test_file = os.path.join(test_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("test,data\n1,2\n")
        
        file_info = self.extractor.get_file_info("20241210")
        
        assert file_info['date'] == "20241210"
        assert file_info['total_files'] == 1
        assert len(file_info['files']) == 1
        assert file_info['files'][0]['name'] == "test.csv"
        assert file_info['files'][0]['size'] > 0
    
    @patch('subprocess.Popen')
    @patch('os.makedirs')
    def test_download_files_success(self, mock_makedirs, mock_popen):
        """Test successful file download."""
        # Mock SSH key validation
        with patch.object(self.extractor, '_validate_ssh_key', return_value=True):
            # Mock subprocess
            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate.return_value = ("success", "")
            mock_popen.return_value = mock_process
            
            # Mock file listing
            with patch.object(self.extractor, '_list_downloaded_files', return_value=["test.csv"]):
                result = self.extractor.download_files("20241210")
                
                assert result is not None
                assert "20241210" in result
                mock_makedirs.assert_called_once()
                mock_popen.assert_called_once()
    
    @patch('subprocess.Popen')
    def test_download_files_ssh_key_invalid(self, mock_popen):
        """Test file download with invalid SSH key."""
        # Mock SSH key validation to fail
        with patch.object(self.extractor, '_validate_ssh_key', return_value=False):
            result = self.extractor.download_files("20241210")
            
            assert result is None
            mock_popen.assert_not_called()
    
    @patch('subprocess.Popen')
    @patch('os.makedirs')
    def test_download_files_subprocess_error(self, mock_makedirs, mock_popen):
        """Test file download with subprocess error."""
        # Mock SSH key validation
        with patch.object(self.extractor, '_validate_ssh_key', return_value=True):
            # Mock subprocess to fail
            mock_process = MagicMock()
            mock_process.returncode = 1
            mock_process.communicate.return_value = ("", "error message")
            mock_popen.return_value = mock_process
            
            with pytest.raises(Exception):
                self.extractor.download_files("20241210")
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True) 