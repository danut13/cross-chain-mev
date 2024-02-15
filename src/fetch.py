"""Module that defines functionalities for fetching data
from different sources and saving it into the database.

"""
import collections.abc
import concurrent.futures
import logging
import typing

from src.config import get_config
from src.database.access import add_blocks
from src.database.access import add_transactions
from src.database.access import get_block_builder_address
from src.database.access import get_blocks_without_coinbase_transaction_count
from src.database.access import get_non_meved_blocks_count
from src.database.access import get_saved_block_numbers
from src.database.access import update_blocks_with_coinbase_transfer
from src.database.access import update_blocks_with_mev
from src.database.access import update_transaction_coinbase_transfer_value
from src.database.access import update_transaction_with_bridge_information
from src.database.access import update_transaction_with_mev
from src.domain import Block
from src.domain import BlockTrace
from src.domain import Transaction
from src.evm import EthereumService
from src.exceptions import BaseError
from src.zeromev import ZeroMev

_logger = logging.getLogger(__name__)
"""Logger for this module."""

_POLYGON_BRIDGE_ADDRESS = '0xA0c68C638235ee32657e8f720a23ceC1bFc77C77'


class DataFetcherError(BaseError):
    """Exception class for all data fetching errors.

    """


class DataFetcher():
    """Class responsible for fetching data and saving it
    to the database.

    Attributes
    ----------
    __ethereum_service : EthereumService
        The ethereum service.

    """
    def __init__(self):
        """Construct an instance.

        """
        self.__ethereum_service = EthereumService(
            get_config()['URL']['ethereum'])
        self.__zero_mev = ZeroMev()

    def fetch_block_data(self, block_number_start: int,
                         block_number_end: int) -> None:
        """Fetch blockchain data and save it to the database.

        block_number_start : int
            The number of the block to start from.
        block_number_end : int
            The number of the block to end at.

        """
        saved_block_numbers = get_saved_block_numbers(block_number_start,
                                                      block_number_end)
        blocks_to_add = sorted(
            list(
                set(range(block_number_start, block_number_end + 1)) -
                set(saved_block_numbers)))

        _logger.info('the number of requested blocks: '
                     f'{block_number_end - block_number_start + 1}')
        _logger.info('the number of blocks that need '
                     f'to be added: {len(blocks_to_add)}')

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_block_number = {
                executor.submit(
                    self.__ethereum_service.fetch_block,  # yapf
                    block_number): block_number
                for block_number in blocks_to_add
            }
            for future in concurrent.futures.as_completed(
                    future_to_block_number):
                block_number = future_to_block_number[future]
                block = future.result()
                _logger.info(f'saving block {block_number}')
                self.__save_block_data([block])

    def fetch_mev_block_data(self, block_number_start: int,
                             block_number_end: int) -> None:
        """Fetch MEV blockchain data and save it to the database.

        Parameters
        ----------
        block_number_start : int
            The number of the block to start from.
        block_number_end : int
            The number of the block to end at.

        """
        self.__check_blocks_are_fetched(block_number_start, block_number_end)
        number_of_non_meved_blocks = get_non_meved_blocks_count(
            block_number_start, block_number_end)
        if number_of_non_meved_blocks == 0:
            _logger.info(f'blocks from {block_number_start} '
                         f'to {block_number_end} are already MEVed')
            return
        _logger.info(f'{number_of_non_meved_blocks} non MEVed blocks. '
                     'adding MEV details to blocks from '
                     f'{block_number_start} to {block_number_end}')
        self.__fetch_mev_block_data(block_number_start, block_number_end)

    def fetch_and_process_traces(self, block_number_start: int,
                                 block_number_end: int) -> None:
        """Fetch the traces of the blocks. The traces will be processed
        for coinbase transfers and determining if transactions
        interacted with the Polygon bridge.

        Parameters
        ----------
        block_number_start : int
            The number of the block to start from.
        block_number_end : int
            The number of the block to end at.

        """
        self.__check_blocks_are_fetched(block_number_start, block_number_end)
        number_of_blocks_without_coinbase_transfer = \
            get_blocks_without_coinbase_transaction_count(
                block_number_start, block_number_end)
        if number_of_blocks_without_coinbase_transfer == 0:
            _logger.info(f'blocks from {block_number_start} '
                         f'to {block_number_end} already have '
                         'the coinbase transfer added')
            return
        _logger.info(f'{number_of_blocks_without_coinbase_transfer} without '
                     'the coinbase transfer added. adding coinbase transfer '
                     f'from {block_number_start} to {block_number_end}')
        block_traces = self.__fetch_block_traces(block_number_start,
                                                 block_number_end)
        self.__process_block_traces(block_traces)

    def __check_blocks_are_fetched(self, block_number_start: int,
                                   block_number_end: int) -> None:
        saved_blocks = get_saved_block_numbers(block_number_start,
                                               block_number_end)
        non_saved_blocks_number = (block_number_end - block_number_start + 1 -
                                   len(saved_blocks))
        if non_saved_blocks_number != 0:
            raise DataFetcherError(
                f'{non_saved_blocks_number} blocks are not fetched')

    def __fetch_block_traces(self, block_number_start: int,
                             block_number_end: int) -> list[BlockTrace]:
        block_traces: list[BlockTrace] = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_block_number = {
                executor.submit(
                    self.__ethereum_service.fetch_transaction_traces,  # yapf
                    block_number): block_number
                for block_number in range(block_number_start,
                                          block_number_end + 1)
            }
            for future in concurrent.futures.as_completed(
                    future_to_block_number):
                block_number = future_to_block_number[future]
                block_traces.append(BlockTrace(block_number, future.result()))
            update_blocks_with_coinbase_transfer(block_number_start,
                                                 block_number_end)
            return block_traces

    def __process_block_traces(self, block_traces: list[BlockTrace]):
        for block_trace in block_traces:
            builder_address = get_block_builder_address(
                block_trace.block_number)
            for transaction_trace in block_trace.transaction_traces:
                sent_value = 0
                update_polygon_bridge_interaction = False
                # process traces
                for trace in transaction_trace['trace']:
                    # polygon bridge
                    if _POLYGON_BRIDGE_ADDRESS in (
                            trace['action'].get('to'),
                            trace['action'].get('from')):
                        update_polygon_bridge_interaction = True
                    # coinbase transfer
                    if trace['action'].get('to') == builder_address:
                        sent_value += trace['action']['value']
                transaction_hash = transaction_trace['transactionHash']
                if update_polygon_bridge_interaction:
                    update_transaction_with_bridge_information(
                        transaction_hash)
                if sent_value > 0:
                    _logger.info('updating coinbase transfer for block '
                                 f'{block_trace.block_number} '
                                 f'transaction hash {transaction_hash.hex()} '
                                 f'with value {sent_value}')
                    update_transaction_coinbase_transfer_value(
                        transaction_hash, str(sent_value))

    def __fetch_mev_block_data(self, block_number_start: int,
                               block_number_end: int) -> None:
        mev_transactions: list[ZeroMev.TransactionResponse] = []
        number_of_blocks = block_number_end - block_number_start + 1
        number_of_100_batches = int(number_of_blocks / 100)
        number_of_blocks_in_last_batch = number_of_blocks % 100
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures_to_block_range = {}
            for number_of_100_batch in range(0, number_of_100_batches):
                block_number_start_ = \
                    block_number_start + (100 * number_of_100_batch)
                futures_to_block_range[executor.submit(
                    self.__zero_mev.fetch_mev_transactions_for_blocks,
                    block_number_start_, 100)] = (block_number_start_, 100)
            block_number_start_ = \
                block_number_start + (100 * number_of_100_batches)
            futures_to_block_range[executor.submit(
                self.__zero_mev.fetch_mev_transactions_for_blocks,
                block_number_start_, number_of_blocks_in_last_batch)] = (
                    block_number_start_, number_of_blocks_in_last_batch)
            for future in concurrent.futures.as_completed(
                    futures_to_block_range):
                block_number_start, number_of_blocks = futures_to_block_range[
                    future]
                block_number_end = block_number_start + number_of_blocks - 1
                mev_transactions = future.result()
                for mev_transaction in mev_transactions:
                    _logger.info(f'updating MEV transaction in block '
                                 f'{mev_transaction.block_number} with index '
                                 f'{mev_transaction.transactiion_index}')
                    update_transaction_with_mev(mev_transaction)
                _logger.info(
                    'updating MEV blocks from block '
                    f'{block_number_start} to block {block_number_end}')
                update_blocks_with_mev(block_number_start, block_number_end)

    def __save_block_data(self, blocks: list[Block]) -> None:
        """Save the block data to the database.

        """
        add_blocks(blocks)
        for block in blocks:
            add_transactions(
                typing.cast(collections.abc.Sequence[Transaction],
                            block['transactions']))
