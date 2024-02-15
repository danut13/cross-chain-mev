"""Module for interacting with EVM blockchains.

"""
import logging
import time
import typing

import web3
import web3.exceptions
import web3.tracing
import web3.types

from src import REQUEST_RETRY_SECONDS
from src.domain import Block
from src.domain import TransactionTrace
from src.exceptions import BaseError

_logger = logging.getLogger(__name__)
"""Logger for this module."""


class EthereumServiceError(BaseError):
    """Exception class for all Ethereum service errors.

    """


class EthereumService:
    """Ethereum-specific blockchain service.

    Attributes
    ----------
    w3 : web3.Web3
        Blockchain interactor object.
    tracing : web3.tracing.Tracing
        Blockchain tracing interactor object.

    """
    def __init__(self, rpc_url: str):
        """Construct a blockchain service instance. The w3 object
        establishes an Ethereum blockchain node connection with
        the given RPC server.

        Parameters
        ----------
        rpc_url : str
            The URL of the Ethereum blockchain node RPC.

        """
        try:
            self.w3 = web3.Web3(web3.Web3.HTTPProvider(rpc_url))
            if not self.w3.is_connected():
                raise EthereumServiceError(f'unable to connect to {rpc_url}')
        except Exception:
            raise EthereumServiceError(f'unable to connect to {rpc_url}')
        try:
            self.tracing = web3.tracing.Tracing(self.w3)
            self.tracing.trace_replay_block_transactions(1)
        except web3.exceptions.MethodUnavailable:
            _logger.error(
                f'the {rpc_url} RPC node does not support the tracing API, '
                'please use an Erigon Ethereum client')
            exit()

    def fetch_block(self, block_number: int) -> Block:
        """Fetches the block with the given block number.

        Parameters
        ----------
        block_number : int
            The number of the block to fetch.

        """
        _logger.info(f'fetching block {block_number}')
        while True:
            try:
                block = self.w3.eth.get_block(block_number,
                                              full_transactions=True)
                return block
            except Exception as error:
                _logger.error(f'unable to fetch block {block_number}; '
                              f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)

    def fetch_transaction_traces(self,
                                 block_number: int) -> list[TransactionTrace]:
        """Fetches the transaction traces for the given block number.

        Parameters
        ----------
        block_number : int
            The number of the block to fetch.

        """
        _logger.info(f'fetching traces for block {block_number}')
        while True:
            try:
                transaction_traces = typing.cast(
                    list[TransactionTrace],
                    self.tracing.trace_replay_block_transactions(block_number))
                return transaction_traces
            except Exception as error:
                _logger.error(
                    f'unable to fetch traces for block {block_number}; '
                    f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)
