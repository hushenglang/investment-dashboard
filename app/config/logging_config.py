import logging
import sys
from typing import Optional


def setup_logging(
    level: int = logging.INFO,
    log_format: Optional[str] = None
) -> None:
    """
    Setup logging configuration for the application.
    
    Args:
        level: The logging level (default: logging.INFO)
        log_format: Custom log format string (optional)
    """
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format=log_format,
        stream=sys.stdout
    )

    # Prevent propagation of logs from third-party libraries
    for logger_name in ['urllib3', 'requests']:
        logging.getLogger(logger_name).propagate = False


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: The name for the logger (typically __name__)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name) 