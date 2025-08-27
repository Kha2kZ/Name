import logging
import os
from datetime import datetime

def setup_logging(log_level: str = "INFO", log_to_file: bool = True, log_dir: str = "logs"):
    """Setup logging configuration for the bot"""
    
    # Create logs directory if it doesn't exist
    if log_to_file:
        os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging level
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler
    if log_to_file:
        log_filename = f"antibot_{datetime.now().strftime('%Y%m%d')}.log"
        log_filepath = os.path.join(log_dir, log_filename)
        
        file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Configure discord.py logging to be less verbose
    discord_logger = logging.getLogger('discord')
    discord_logger.setLevel(logging.WARNING)
    
    # Log startup message
    startup_logger = logging.getLogger('startup')
    startup_logger.info("Logging system initialized")
    startup_logger.info(f"Log level: {log_level}")
    if log_to_file:
        startup_logger.info(f"Log file: {log_filepath}")
