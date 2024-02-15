import dataclasses
import logging
import time

import requests

from src import REQUEST_RETRY_SECONDS
from src.domain import MevType
from src.domain import ProtocolType

_ZEROMEV_BLOCKS_URL = 'https://data.zeromev.org/v1/mevBlock'

_logger = logging.getLogger(__name__)
"""Logger for this module."""


class ZeroMev:
    """ZeroMev interactor class.

    """
    @dataclasses.dataclass
    class TransactionResponse:
        """Response data for MEV transactions.

        """
        block_number: int
        transactiion_index: int
        mev_type: MevType
        protocol_type: ProtocolType
        user_swap_count: int

    def __init__(self):
        """Initialize the ZeroMev interactoro.

        """
        # Using the same request session for caching/improved performance.
        self.__request_session = requests.Session()

    def fetch_mev_transactions_for_blocks(
            self, block_number_start: int,
            number_of_blocks: int) -> list[TransactionResponse]:
        """Fetch "number_of_blocks" blocks of MEV transactions
        starting from the given starting block number.

        Parameters
        ----------
        block_number_start : int
            The starting block number.
        number_of_blocks : int
            The number of blocks to fetch. This must be lower than 100.

        """
        if number_of_blocks > 100:
            raise ZeroDivisionError(
                'the number of blocks must be lower or equal to 100')
        payload = {
            'block_number': block_number_start,
            'count': number_of_blocks
        }

        while True:
            response = self.__request_session.get(_ZEROMEV_BLOCKS_URL,
                                                  params=payload)
            if response.status_code == 200:
                return [
                    self.TransactionResponse(
                        block_number=transaction['block_number'],
                        transactiion_index=transaction['tx_index'],
                        mev_type=MevType.from_name(transaction['mev_type']),
                        protocol_type=ProtocolType.from_name(
                            transaction['protocol']),
                        user_swap_count=transaction['user_swap_count']
                        if transaction['user_swap_count'] is not None else 0)
                    for transaction in response.json()
                ]
            else:
                _logger.error(
                    f'unable to fetch {number_of_blocks} '
                    f'MEV blocks from {block_number_start}; ',
                    f'retrying in {REQUEST_RETRY_SECONDS}')
                time.sleep(REQUEST_RETRY_SECONDS)
