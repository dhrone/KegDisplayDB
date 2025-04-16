"""
Logging configuration for KegDisplayDB
"""

import logging
import os
import sys
from datetime import datetime, UTC

def configure_logging(log_level='INFO'):
    """
    Configure logging for KegDisplayDB with the specified log level
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Create logs directory in user config folder if it doesn't exist
    user_home = os.path.expanduser("~")
    config_dir = os.path.join(user_home, ".KegDisplayDB")
    logs_dir = os.path.join(config_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    # Configure root logger
    logger = logging.getLogger("KegDisplay")
    logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicate logs
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    
    # Create file handler
    log_date = datetime.now(UTC).strftime("%Y-%m-%d")
    log_file = os.path.join(logs_dir, f"kegdisplay-{log_date}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(numeric_level)
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    logger.info(f"Logging initialized at level {log_level}")
    logger.info(f"Log file: {log_file}")
    
    return logger
