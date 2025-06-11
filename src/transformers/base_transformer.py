"""Base transformer class for Toast ETL Pipeline."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd

from ..utils.logging_utils import get_logger

logger = get_logger(__name__)


class BaseTransformer(ABC):
    """Abstract base class for data transformers."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize base transformer.
        
        Args:
            config: Configuration dictionary for the transformer
        """
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)
    
    @abstractmethod
    def transform(self, data: Any, **kwargs) -> Any:
        """
        Transform data according to the implementation.
        
        Args:
            data: Input data to transform
            **kwargs: Additional transformation parameters
            
        Returns:
            Transformed data
        """
        pass
    
    @abstractmethod
    def validate_input(self, data: Any) -> bool:
        """
        Validate input data format.
        
        Args:
            data: Input data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_output(self, data: Any) -> bool:
        """
        Validate transformed data format.
        
        Args:
            data: Transformed data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        pass
    
    def get_transformation_stats(self, input_data: Any, output_data: Any) -> Dict[str, Any]:
        """
        Get statistics about the transformation.
        
        Args:
            input_data: Original input data
            output_data: Transformed output data
            
        Returns:
            Dictionary containing transformation statistics
        """
        stats = {
            'transformation_type': self.__class__.__name__,
            'config': self.config
        }
        
        # Add DataFrame-specific stats if applicable
        if isinstance(input_data, pd.DataFrame):
            stats.update({
                'input_rows': len(input_data),
                'input_columns': len(input_data.columns) if hasattr(input_data, 'columns') else 0
            })
            
        if isinstance(output_data, pd.DataFrame):
            stats.update({
                'output_rows': len(output_data),
                'output_columns': len(output_data.columns) if hasattr(output_data, 'columns') else 0
            })
            
        return stats 