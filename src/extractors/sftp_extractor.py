"""SFTP extractor for downloading files from Toast POS server."""

import os
import subprocess
from datetime import datetime
from typing import Optional, List
from pathlib import Path
import shutil
import shutil

from ..config.settings import settings
from ..utils.logging_utils import get_logger
from ..utils.retry_utils import retry_with_backoff

logger = get_logger(__name__)


class SFTPExtractor:
    """Extract data from Toast POS SFTP server."""
    
    def __init__(self, 
                 sftp_user: Optional[str] = None,
                 sftp_server: Optional[str] = None,
                 ssh_key_path: Optional[str] = None,
                 local_dir: Optional[str] = None):
        """
        Initialize SFTP extractor.
        
        Args:
            sftp_user: SFTP username (defaults to settings)
            sftp_server: SFTP server address (defaults to settings)
            ssh_key_path: Path to SSH private key (defaults to settings)
            local_dir: Local directory for downloads (defaults to settings)
        """
        self.sftp_user = sftp_user or settings.sftp_user
        self.sftp_server = sftp_server or settings.sftp_server
        self.ssh_key_path = ssh_key_path or settings.ssh_key_path
        self.local_dir = local_dir or settings.raw_local_dir
        
        # Expand user home directory
        self.ssh_key_path = os.path.expanduser(self.ssh_key_path)
        
        logger.info(f"SFTP Extractor initialized for server: {self.sftp_server}")
    
    def _validate_ssh_key(self) -> bool:
        """
        Validate that SSH key exists and has proper permissions.
        
        Returns:
            True if SSH key is valid, False otherwise
        """
        if not os.path.exists(self.ssh_key_path):
            logger.error(f"SSH key not found at: {self.ssh_key_path}")
            return False
        
        # Check file permissions (should be 600 or 400)
        stat_info = os.stat(self.ssh_key_path)
        permissions = oct(stat_info.st_mode)[-3:]
        
        if permissions not in ['600', '400']:
            logger.warning(f"SSH key has permissions {permissions}, should be 600 or 400")
        
        return True
    
    @retry_with_backoff(
        max_attempts=3,
        base_delay=30.0,
        exceptions=(subprocess.CalledProcessError, OSError)
    )
    def download_files(self, date: str) -> Optional[str]:
        """
        Download files from SFTP server for the specified date.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Local directory path where files were downloaded, or None if failed
        """
        try:
            logger.info(f"Starting SFTP download for date: {date}")
            
            # Validate SSH key
            if not self._validate_ssh_key():
                return None
            
            # Create remote path
            remote_path = settings.sftp_path_template.format(date=date)
            
            # Create local directory
            local_date_dir = os.path.join(self.local_dir, date)
            os.makedirs(local_date_dir, exist_ok=True)
            
            # Build SFTP command
            sftp_command = [
                "sftp",
                "-i", self.ssh_key_path,
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "LogLevel=ERROR",
                f"{self.sftp_user}@{self.sftp_server}"
            ]
            
            # Create batch file for SFTP commands
            batch_commands = [
                f"cd {os.path.dirname(remote_path)}",
                f"lcd {local_date_dir}",
                f"mget {os.path.basename(remote_path)}",
                "quit"
            ]
            
            logger.info(f"Executing SFTP command to download from: {remote_path}")
            logger.info(f"Local destination: {local_date_dir}")
            
            # Execute SFTP command
            process = subprocess.Popen(
                sftp_command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(input="\n".join(batch_commands))
            
            if process.returncode != 0:
                logger.error(f"SFTP command failed with return code {process.returncode}")
                logger.error(f"STDERR: {stderr}")
                raise subprocess.CalledProcessError(process.returncode, sftp_command)
            
            # Verify files were downloaded
            downloaded_files = self._list_downloaded_files(local_date_dir)
            
            if not downloaded_files:
                logger.warning(f"No files found in {local_date_dir} after download")
                return None
            
            logger.info(f"Successfully downloaded {len(downloaded_files)} files:")
            for file in downloaded_files:
                logger.info(f"  - {file}")
            
            return local_date_dir
            
        except subprocess.CalledProcessError as e:
            logger.error(f"SFTP download failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during SFTP download: {e}")
            raise
    
    def _list_downloaded_files(self, directory: str) -> List[str]:
        """
        List files in the downloaded directory.
        
        Args:
            directory: Directory to list files from
            
        Returns:
            List of file names
        """
        try:
            if not os.path.exists(directory):
                return []
            
            return [f for f in os.listdir(directory) 
                   if os.path.isfile(os.path.join(directory, f)) and f.endswith('.csv')]
        except Exception as e:
            logger.error(f"Error listing files in {directory}: {e}")
            return []
    
    def get_file_info(self, date: str) -> dict:
        """
        Get information about downloaded files for a specific date.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Dictionary with file information
        """
        local_date_dir = os.path.join(self.local_dir, date)
        files = self._list_downloaded_files(local_date_dir)
        
        file_info = {
            'date': date,
            'local_directory': local_date_dir,
            'files': [],
            'total_files': len(files)
        }
        
        for file in files:
            file_path = os.path.join(local_date_dir, file)
            try:
                stat_info = os.stat(file_path)
                file_info['files'].append({
                    'name': file,
                    'size': stat_info.st_size,
                    'modified': datetime.fromtimestamp(stat_info.st_mtime).isoformat()
                })
            except Exception as e:
                logger.warning(f"Could not get info for file {file}: {e}")
        
        return file_info
    
    def cleanup_date_files(self, date: str) -> None:
        """
        Clean up downloaded files for a specific date.
        
        Args:
            date: Date in YYYYMMDD format
        """
        try:
            local_date_dir = os.path.join(self.local_dir, date)
            if os.path.exists(local_date_dir):
                logger.info(f"Cleaning up files for date: {date}")
                shutil.rmtree(local_date_dir)
        except Exception as e:
            logger.error(f"Error cleaning up files for {date}: {e}")
    
    def cleanup_old_files(self, days_to_keep: int = 7) -> None:
        """
        Clean up old downloaded files to save disk space.
        
        Args:
            days_to_keep: Number of days worth of files to keep
        """
        try:
            if not os.path.exists(self.local_dir):
                return
            
            cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
            
            for item in os.listdir(self.local_dir):
                item_path = os.path.join(self.local_dir, item)
                
                if os.path.isdir(item_path):
                    # Check if directory is older than cutoff
                    if os.path.getmtime(item_path) < cutoff_date:
                        logger.info(f"Cleaning up old directory: {item_path}")
                        shutil.rmtree(item_path)
                        
        except Exception as e:
            logger.error(f"Error during cleanup: {e}") 