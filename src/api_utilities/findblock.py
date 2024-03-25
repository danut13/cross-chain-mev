"""Module for finding blocks by timestamp.

"""
import logging
import time

import requests

from src import REQUEST_RETRY_SECONDS

_FIND_BLOCK_URL = 'https://api.findblock.xyz/v1/'
_POLYGON_CHAIN_ID = 137
_BLOCK_RESOURCE = f'chain/{_POLYGON_CHAIN_ID}/block/'
_BEFORE_TIMESTAMP_RESOURCE = f'{_BLOCK_RESOURCE}before/'
_AFTER_TIMESTAMP_RESOURCE = f'{_BLOCK_RESOURCE}after/'

_logger = logging.getLogger(__name__)
"""Logger for this module."""


class FindBlock:
    """Class for finding blocks by timestamp.

    """
    def __init__(self):
        """Initialize the ZeroMev interactoro.

        """
        # Using the same request session for caching/improved performance.
        self.__request_session = requests.Session()

    def find_polygon_block_before_timestamp(self, timestamp: int) -> int:
        """Find the polygon block before the given timestamp.

        Parameters
        ----------
        timestamp : int
            The given timestamp.

        Returns
        -------
        int
            The block number.

        """
        while True:
            try:
                response = self.__request_session.get(
                    f'{_FIND_BLOCK_URL}{_BEFORE_TIMESTAMP_RESOURCE}'
                    f'{timestamp}?'
                    'inclusive=true')
                return response.json()['number']
            except Exception as error:
                _logger.error(
                    f'unable to fetch get polygon before {timestamp}; '
                    f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)

    def find_polygon_block_after_timestamp(self, timestamp: int):
        """Find the polygon block after the given timestamp.

        Parameters
        ----------
        timestamp : int
            The given timestamp.

        Returns
        -------
        int
            The block number.

        """
        while True:
            try:
                response = self.__request_session.get(
                    f'{_FIND_BLOCK_URL}{_AFTER_TIMESTAMP_RESOURCE}{timestamp}?'
                    'inclusive=true')
                return response.json()['number']
            except Exception as error:
                _logger.error(
                    f'unable to fetch get polygon after {timestamp}; '
                    f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)
