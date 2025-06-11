"""Logging utility functions for Toast ETL Pipeline."""

import logging
import sys
from typing import Optional

try:
    from google.cloud import logging as cloud_logging
    from google.cloud.logging_v2.handlers import CloudLoggingHandler
    CLOUD_LOGGING_AVAILABLE = True
except ImportError:
    CLOUD_LOGGING_AVAILABLE = False

from ..config.settings import settings


def setup_logging(
    name: str = "toast_etl",
    level: int = logging.INFO,
    enable_cloud_logging: bool = True
) -> logging.Logger:
    """
    Set up logging with both local console and Google Cloud Logging.
    
    Args:
        name: Logger name
        level: Logging level
        enable_cloud_logging: Whether to enable Google Cloud Logging
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Add Cloud Logging handler if enabled and available
    if enable_cloud_logging and settings.environment != "development" and CLOUD_LOGGING_AVAILABLE:
        try:
            client = cloud_logging.Client(project=settings.gcp_project_id)
            cloud_handler = CloudLoggingHandler(client)
            cloud_handler.setLevel(level)
            logger.addHandler(cloud_handler)
            logger.info("Google Cloud Logging initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize Cloud Logging: {e}")
    elif enable_cloud_logging and not CLOUD_LOGGING_AVAILABLE:
        logger.warning("Cloud Logging requested but not available")
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name, defaults to 'toast_etl'
        
    Returns:
        Logger instance
    """
    if name is None:
        name = "toast_etl"
    
    logger = logging.getLogger(name)
    
    # If logger doesn't have handlers, set it up
    if not logger.handlers:
        logger = setup_logging(name)
    
    return logger 