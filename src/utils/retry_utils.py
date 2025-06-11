"""Retry utility functions for Toast ETL Pipeline."""

import time
import random
from typing import Callable, Any, Optional, Type, Tuple
from functools import wraps

from ..config.settings import settings
from .logging_utils import get_logger

logger = get_logger(__name__)


def retry_with_backoff(
    max_attempts: Optional[int] = None,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts (defaults to settings)
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delay
        exceptions: Tuple of exception types to catch and retry
        
    Returns:
        Decorated function with retry logic
    """
    if max_attempts is None:
        max_attempts = settings.max_retry_attempts
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts - 1:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts. "
                            f"Last error: {str(e)}"
                        )
                        raise e
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    # Add jitter to prevent thundering herd
                    if jitter:
                        delay = delay * (0.5 + random.random() * 0.5)
                    
                    logger.warning(
                        f"Function {func.__name__} failed on attempt {attempt + 1}/{max_attempts}. "
                        f"Retrying in {delay:.2f} seconds. Error: {str(e)}"
                    )
                    
                    time.sleep(delay)
            
            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
            
        return wrapper
    return decorator


def retry_on_failure(
    func: Callable,
    *args,
    max_attempts: Optional[int] = None,
    base_delay: float = 1.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    **kwargs
) -> Any:
    """
    Execute a function with retry logic (non-decorator version).
    
    Args:
        func: Function to execute
        *args: Positional arguments for the function
        max_attempts: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
        exceptions: Tuple of exception types to catch and retry
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result of the function execution
        
    Raises:
        The last exception if all retries fail
    """
    if max_attempts is None:
        max_attempts = settings.max_retry_attempts
    
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            
            if attempt == max_attempts - 1:
                logger.error(
                    f"Function {func.__name__} failed after {max_attempts} attempts. "
                    f"Last error: {str(e)}"
                )
                raise e
            
            delay = base_delay * (2 ** attempt)
            logger.warning(
                f"Function {func.__name__} failed on attempt {attempt + 1}/{max_attempts}. "
                f"Retrying in {delay} seconds. Error: {str(e)}"
            )
            
            time.sleep(delay)
    
    if last_exception:
        raise last_exception 