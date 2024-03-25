"""Module that defines functionalities for fetching data
from different sources and saving it into the database.

"""
import logging
import os
import queue
import threading
import typing

from src.api_utilities.zeromev import ZeroMev
from src.blockchains.ethereum import EthereumService
from src.config import get_config
from src.database.access import add_block
from src.database.access import add_transactions
from src.database.access import get_block_builder_address
from src.database.access import get_blocks_without_traces_processed
from src.database.access import get_non_meved_blocks
from src.database.access import get_saved_block_numbers
from src.database.access import update_block_with_trace_processed
from src.database.access import update_blocks_with_mev
from src.database.access import update_transaction_coinbase_transfer_value
from src.database.access import update_transaction_with_bridge_information
from src.database.access import update_transaction_with_mev
from src.domain import Block
from src.domain import BlockTrace
from src.domain import PolygonBridgeInteraction
from src.domain import TransactionTrace
from src.exceptions import BaseError

_logger = logging.getLogger(__name__)
"""Logger for this module."""

_POLYGON_ROOT_CHAIN_MANAGER_PROXY = \
    '0xA0c68C638235ee32657e8f720a23ceC1bFc77C77'
_POLYGON_BRIDGE_FROM_ETHEREUM_FUNCTION_SELECTORS = ['0x4faa8a26', '0xe3dec8fb']
_POLYGON_BRIDGE_TO_ETHEREUM_FUNCTION_SELECTORS = ['0x3805550f']


class DataFetcherError(BaseError):
    """Exception class for all data fetching errors.

    """


class DataFetcher():
    """Class responsible for fetching data and saving it
    to the database.

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

        Parameters
        ----------
        block_number_start : int
            The number of the block to start from.
        block_number_end : int
            The number of the block to end at.

        """
        _logger.info('the number of requested blocks: '
                     f'{block_number_end - block_number_start + 1}')
        saved_block_numbers = get_saved_block_numbers(block_number_start,
                                                      block_number_end)
        blocks_to_add = sorted(
            list(
                set(range(block_number_start, block_number_end + 1)) -
                set(saved_block_numbers)))
        _logger.info('the number of blocks that need '
                     f'to be added: {len(blocks_to_add)}')

        q: queue.Queue = queue.Queue()

        def worker():
            not_done = True
            while not_done:
                try:
                    block_number = q.get(block=False)
                    block = \
                        self.__ethereum_service.fetch_block(block_number)
                    _logger.info(f'saving block {block_number}')
                    self.__save_block_data(block)
                    q.task_done()
                except queue.Empty:
                    not_done = False
                except Exception:
                    q.task_done()
                    _logger.warning(
                        f'error when fetching block {block_number} ',
                        exc_info=True)
                    q.put(block_number)

        for block_number in blocks_to_add:
            q.put(block_number)

        consumers = [
            threading.Thread(target=worker, daemon=True)
            for _ in range(0,
                           os.cpu_count() or 4)
        ]

        for consumer_ in consumers:
            consumer_.start()

        for consumer_ in consumers:
            consumer_.join()

        q.join()

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
        _logger.info('the number of requested MEV blocks: '
                     f'{block_number_end - block_number_start + 1}')
        self.__check_blocks_are_fetched(block_number_start, block_number_end)
        non_meved_blocks = get_non_meved_blocks(block_number_start,
                                                block_number_end)
        blocks_to_mev = self.__get_lists_of_consecutive_block_numbers(
            non_meved_blocks)
        _logger.info(
            'the number of blocks that need to have the MEV details '
            f'added: {sum(len(sublist) for sublist in blocks_to_mev)}')

        for interval in blocks_to_mev:
            if len(interval) == 1:
                self.__fetch_mev_block_data(interval[0], interval[0])
            else:
                self.__fetch_mev_block_data(interval[0], interval[-1])

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
        _logger.info('the number of requested blocks to fetch the traces for: '
                     f'{block_number_end - block_number_start + 1}')
        self.__check_blocks_are_fetched(block_number_start, block_number_end)
        blocks_to_fetch_traces_for = \
            get_blocks_without_traces_processed(
                block_number_start, block_number_end)
        _logger.info(
            'the number of blocks that need to have the traces fetched: '
            f'{len(blocks_to_fetch_traces_for)}')

        self.__fetch_and_process_block_traces(blocks_to_fetch_traces_for)

    def __save_block_data(self, block: Block) -> None:
        add_block(block)
        add_transactions(int(block['number']),
                         typing.cast(list, block['transactions']))

    def __check_blocks_are_fetched(self, block_number_start: int,
                                   block_number_end: int) -> None:
        saved_blocks = get_saved_block_numbers(block_number_start,
                                               block_number_end)
        non_saved_blocks_number = (block_number_end - block_number_start + 1 -
                                   len(saved_blocks))
        if non_saved_blocks_number != 0:
            raise DataFetcherError(
                f'{non_saved_blocks_number} blocks are not fetched')

    def __get_lists_of_consecutive_block_numbers(
            self, block_numbers: list[int]) -> list[list[int]]:
        if len(block_numbers) == 0:
            return []
        block_numbers.sort()

        result = []
        temp = [block_numbers[0]]

        for i in range(1, len(block_numbers)):
            if block_numbers[i] == temp[-1] + 1:
                temp.append(block_numbers[i])
            else:
                result.append(temp)
                temp = [block_numbers[i]]

        result.append(temp)

        return result

    def __fetch_mev_block_data(self, block_number_start: int,
                               block_number_end: int) -> None:
        number_of_blocks = block_number_end - block_number_start + 1
        number_of_100_batches = int(number_of_blocks / 100)
        q: queue.Queue = queue.Queue()

        def worker():
            not_done = True
            while not_done:
                try:
                    block_number_start_ = q.get(block=False)
                    if block_number_start_ + 100 <= block_number_end:
                        block_number_end_ = block_number_start_ + 100
                    else:
                        block_number_end_ = block_number_end
                    mev_transactions = \
                        self.__zero_mev.fetch_mev_transactions_for_blocks(
                            block_number_start_,
                            block_number_end_ - block_number_start_)
                    for mev_transaction in mev_transactions:
                        _logger.info(
                            f'updating MEV transaction in block '
                            f'{mev_transaction.block_number} with index '
                            f'{mev_transaction.transactiion_index}')
                        update_transaction_with_mev(mev_transaction)
                    _logger.info('updating MEV blocks from block '
                                 f'{block_number_start_} to block '
                                 f'{block_number_end_}')
                    update_blocks_with_mev(block_number_start_,
                                           block_number_end_)
                    q.task_done()
                except queue.Empty:
                    not_done = False
                except Exception:
                    q.task_done()
                    _logger.warning(
                        'error when fetching MEV block data '
                        f'from {block_number_start_}', exc_info=True)
                    q.put(block_number_start_)

        q.put(block_number_start)
        for number_of_100_batch in range(1, number_of_100_batches + 1):
            q.put(block_number_start + number_of_100_batch * 100)

        consumers = [
            threading.Thread(target=worker, daemon=True)
            for _ in range(0,
                           os.cpu_count() or 4)
        ]

        for consumer_ in consumers:
            consumer_.start()

        for consumer_ in consumers:
            consumer_.join()

        q.join()

    def __fetch_and_process_block_traces(self, block_numbers: list[int]):
        q: queue.Queue = queue.Queue()

        def worker():
            not_done = True
            while not_done:
                try:
                    block_number = q.get(block=False)
                    tx_traces = \
                        self.__ethereum_service.fetch_transaction_traces(
                            block_number)
                    block_trace = BlockTrace(block_number, tx_traces)
                    self.__process_block_trace(block_trace)
                    update_block_with_trace_processed(block_number)
                    q.task_done()
                except queue.Empty:
                    not_done = False
                except Exception:
                    q.task_done()
                    _logger.warning(
                        'error when fetching traces for block number '
                        f'{block_number}', exc_info=True)
                    q.put(block_number)

        for block_number in block_numbers:
            q.put(block_number)

        consumers = [
            threading.Thread(target=worker, daemon=True)
            for _ in range(0,
                           os.cpu_count() or 4)
        ]

        for consumer_ in consumers:
            consumer_.start()

        for consumer_ in consumers:
            consumer_.join()

        q.join()

    def __process_block_trace(self, block_trace: BlockTrace) -> None:
        builder_address = get_block_builder_address(block_trace.block_number)
        for transaction_trace in block_trace.transaction_traces:
            polygon_bridge_interaction, sent_value = \
                self.__process_transaction_traces(
                    transaction_trace, builder_address)
            transaction_hash = transaction_trace['transactionHash']
            if (polygon_bridge_interaction
                    is not PolygonBridgeInteraction.NONE):
                update_transaction_with_bridge_information(
                    transaction_hash, polygon_bridge_interaction)
            if sent_value > 0:
                _logger.info('updating coinbase transfer for block '
                             f'{block_trace.block_number} '
                             f'transaction hash {transaction_hash.hex()} '
                             f'with value {sent_value}')
                update_transaction_coinbase_transfer_value(
                    transaction_hash, str(sent_value))

    def __process_transaction_traces(
            self, transaction_trace: TransactionTrace,
            builder_address: str) -> tuple[PolygonBridgeInteraction, int]:
        sent_value = 0
        polygon_bridge_interaction = PolygonBridgeInteraction.NONE
        for trace in transaction_trace['trace']:
            # polygon bridge
            if (polygon_bridge_interaction is PolygonBridgeInteraction.NONE
                    and _POLYGON_ROOT_CHAIN_MANAGER_PROXY
                    == trace['action'].get('to')):
                # we extract the function selector from the input data
                # the function selector is the first 4 bytes of it
                function_selector = trace['action']['input'][:4].hex()
                if function_selector in \
                        _POLYGON_BRIDGE_FROM_ETHEREUM_FUNCTION_SELECTORS:
                    polygon_bridge_interaction = \
                        PolygonBridgeInteraction.FROM_ETHEREUM
                elif function_selector in \
                        _POLYGON_BRIDGE_TO_ETHEREUM_FUNCTION_SELECTORS:
                    polygon_bridge_interaction = \
                        PolygonBridgeInteraction.TO_ETHEREUM
            # coinbase transfer
            if trace['action'].get('to') == builder_address:
                sent_value += trace['action']['value']
        return polygon_bridge_interaction, sent_value
