"""Module for initializing the console or file logging.

"""
import logging

from src.config import get_config

_LOG_FILE_NAME = 'run.log'
"""Log file name."""


def initialize_logging():
    """Initialize the logging for the application.

    """
    log_file = (_LOG_FILE_NAME
                if get_config()['Logging']['file'].lower() == 'true' else None)
    logging.basicConfig(level=logging.INFO, filename=log_file)
