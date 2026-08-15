import logging
import sys

def get_logger(name: str = "EcommercePipeline") -> logging.Logger:
    """
    Returns a configured logger with standard formatting for ETL telemetry.
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if logger is already initialized
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create console handler with formatting
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        
        # Professional log format including timestamp, log level, module name, and message
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s:%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger
