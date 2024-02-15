"""Cross-chain MEV extraction entry point.

"""
import datetime
import logging

from src.analysis import find_non_atomic_mev
from src.config import get_config
from src.database import initialize_database
from src.fetch import DataFetcher
from src.logging import initialize_logging

_logger = logging.getLogger(__name__)
"""Logger for this module."""


def initialize_application():
    """Initialize the application requirements.

    """
    initialize_database()
    initialize_logging()


def fetch_data(block_number_start: int, block_number_end: int):
    """Fetch the required data.

    """
    data_fetcher = DataFetcher()
    data_fetcher.fetch_block_data(block_number_start, block_number_end)
    data_fetcher.fetch_mev_block_data(block_number_start, block_number_end)
    data_fetcher.fetch_and_process_traces(block_number_start, block_number_end)


def analyze_data(block_number_start: int, block_number_end: int):
    """Analyze the data.

    """
    find_non_atomic_mev(block_number_start, block_number_end)


if __name__ == "__main__":
    initialize_application()
    config = get_config()
    _logger.info(
        f'===========NEW RUN STARTED ON {datetime.datetime.now()}===========')
    block_number_start = int(config['Block_number']['start'])
    block_number_end = int(config['Block_number']['end'])
    try:
        fetch_data(block_number_start, block_number_end)
        analyze_data(block_number_start, block_number_end)
    except Exception:
        _logger.error('error', exc_info=True)
