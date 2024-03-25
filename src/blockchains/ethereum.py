"""Module for interacting with EVM blockchains.

"""
import decimal
import logging
import time
import typing

import eth_typing
import web3
import web3.exceptions
import web3.logs
import web3.tracing
import web3.types

from src import REQUEST_RETRY_SECONDS
from src.domain import Block
from src.domain import TransactionTrace
from src.exceptions import BaseError

_logger = logging.getLogger(__name__)
"""Logger for this module."""

TRANSFER_EVENT_ABI = [{
    "name": "Transfer",
    "type": "event",
    "anonymous": False,
    "inputs": [{
        "indexed": True,
        "name": "from",
        "type": "address",
    }, {
        "indexed": True,
        "name": "to",
        "type": "address",
    }, {
        "indexed": False,
        "name": "value",
        "type": "uint256",
    }]
}]

_LOCKED_ERC20_EVENT_ABI = [{
    "name": "LockedERC20",
    "type": "event",
    "anonymous": False,
    "inputs": [{
        "indexed": True,
        "name": "depositor",
        "type": "address"
    }, {
        "indexed": True,
        "name": "depositReceiver",
        "type": "address"
    }, {
        "indexed": True,
        "name": "rootToken",
        "type": "address"
    }, {
        "indexed": False,
        "name": "amount",
        "type": "uint256"
    }]
}]

_LOCKED_MINTABLE_ERC20_EVENT_ABI = [{
    "name": "LockedMintableERC20",
    "type": "event",
    "anonymous": False,
    "inputs": [{
        "indexed": True,
        "name": "depositor",
        "type": "address"
    }, {
        "indexed": True,
        "name": "depositReceiver",
        "type": "address"
    }, {
        "indexed": True,
        "name": "rootToken",
        "type": "address"
    }, {
        "indexed": False,
        "name": "amount",
        "type": "uint256"
    }]
}]

_ERC20_ABI = [{
    "constant": True,
    "inputs": [],
    "name": "symbol",
    "outputs": [{
        "name": "",
        "type": "string"
    }],
    "payable": False,
    "stateMutability": "view",
    "type": "function"
}, {
    "constant": True,
    "inputs": [],
    "name": "decimals",
    "outputs": [{
        "name": "",
        "type": "uint8"
    }],
    "payable": False,
    "stateMutability": "view",
    "type": "function"
}]

_POLYGON_ERC20_BRIDGE_ADDRESS = '0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf'
_POLYGON_MINTABLE_ERC20_BRIDGE_ADDRESS = \
    '0x9923263fA127b3d1484cFD649df8f1831c2A74e4'
_POLYGON_ESCROW_ERC20_BRIDGE_ADDRESS = \
    '0x21ada4D8A799c4b0ADF100eB597a6f1321bCD3E4'
_POLYGON_ERC20_BRIDGE_ADDRESSES = [
    _POLYGON_ERC20_BRIDGE_ADDRESS, _POLYGON_MINTABLE_ERC20_BRIDGE_ADDRESS,
    _POLYGON_ESCROW_ERC20_BRIDGE_ADDRESS
]


class EthereumServiceError(BaseError):
    """Exception class for all Ethereum service errors.

    """


class EthereumService:
    """Ethereum-specific blockchain service.

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
            self._w3 = web3.Web3(web3.Web3.HTTPProvider(rpc_url))
            if not self._w3.is_connected():
                raise EthereumServiceError(f'unable to connect to {rpc_url}')
        except Exception:
            raise EthereumServiceError(f'unable to connect to {rpc_url}')
        if type(self).__name__ == EthereumService.__name__:
            # Need tracing only for Ethereum and not for subclasses (Polygon)
            try:
                self.__tracing = web3.tracing.Tracing(self._w3)
                self.__tracing.trace_replay_block_transactions(1)
            except web3.exceptions.MethodUnavailable:
                _logger.error(
                    f'the {rpc_url} RPC node does not support the tracing API,'
                    ' please use an Erigon Ethereum client')
                exit()

    def get_web3(self) -> web3.Web3:
        """Get the Web3 initialized object.

        Returns
        -------
        web3.Web3
            The Web3 initialized object.

        """
        return self._w3

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
                block = self._w3.eth.get_block(block_number,
                                               full_transactions=False)
                return block
            except Exception as error:
                _logger.error(f'unable to fetch block {block_number}; '
                              f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)

    def get_transaction_from_and_to(self,
                                    transaction_hash: str) -> tuple[str, str]:
        """Get the from and to of a transaction.

        Parameters
        ----------
        transaction_hash : str
            The hash of the transaction

        """
        while True:
            try:
                transaction = self._w3.eth.get_transaction(
                    eth_typing.HexStr(transaction_hash))
                return transaction['from'], transaction['to']
            except Exception as error:
                _logger.error(f'unable to fetch {transaction_hash}; '
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
                    self.__tracing.trace_replay_block_transactions(
                        block_number))
                return transaction_traces
            except Exception as error:
                _logger.error(
                    f'unable to fetch traces for block {block_number}; '
                    f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)

    def get_from_ethereum_bridge_operation_information(
            self, transaction_hash: str) -> tuple[str, str, int]:
        """Get the token address, the amount and sender of the
        from Ethereum to Polygon cross-chain transfer.

        Parameters
        ----------
        transaction_hash : str
            The hash of the transaction.

        Returns
        -------
        tuple[str, str, int]
            The address of the token, the receiver on Polygon
            and the amount.

        Raises
        ------
        EthereumServiceError
            If no token was found.

        """
        while True:
            try:
                receipt = self._w3.eth.get_transaction_receipt(
                    eth_typing.HexStr(transaction_hash))
                locked_erc20_contract = self._w3.eth.contract(
                    abi=_LOCKED_ERC20_EVENT_ABI)
                locked_mintable_erc20_contract = self._w3.eth.contract(
                    abi=_LOCKED_MINTABLE_ERC20_EVENT_ABI)

                decoded_locked_erc20_logs = \
                    locked_erc20_contract.events.LockedERC20().process_receipt(
                        receipt, errors=web3.logs.DISCARD)
                decoded_locked_mintable_erc20_logs = \
                    locked_mintable_erc20_contract.events.LockedMintableERC20(
                        ).process_receipt(receipt, errors=web3.logs.DISCARD)

                logs = (decoded_locked_erc20_logs +
                        decoded_locked_mintable_erc20_logs)
                if len(logs) == 1:
                    log = logs[0]
                    return log['args']['rootToken'], log['args'][
                        'depositReceiver'], log['args']['amount']

                raise EthereumServiceError(
                    f'no token found for {transaction_hash}')
            except EthereumServiceError:
                raise
            except Exception as error:
                _logger.error(
                    'unable to get details for cross-chain transaction hash '
                    f'for {transaction_hash} '
                    f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)

    def get_to_ethereum_bridge_operation_information(
            self, transaction_hash: str) -> tuple[str, str, int]:
        """Get the token address, the amount and receiver of the
        from Polygon to Ethereum cross-chain transfer.

        Parameters
        ----------
        transaction_hash : str
            The hash of the transaction.

        Returns
        -------
        tuple[str, str, int]
            The address of the token, the sender on Polygon
            and the amount.

        Raises
        ------
        EthereumServiceError
            If no token was found.

        """
        while True:
            try:
                receipt = self._w3.eth.get_transaction_receipt(
                    eth_typing.HexStr(transaction_hash))
                contract = self._w3.eth.contract(abi=TRANSFER_EVENT_ABI)
                decoded_logs = contract.events.Transfer().process_receipt(
                    receipt, errors=web3.logs.DISCARD)
                for log in decoded_logs:
                    if (log['args']['from']
                            in _POLYGON_ERC20_BRIDGE_ADDRESSES):
                        # The sender (on Polygon) of the cross-chain transfer
                        # is always the same with the receiver (on Ethereum)
                        return log['address'], log['args']['to'], log['args'][
                            'value']
                raise EthereumServiceError(
                    f'no token found for {transaction_hash}')
            except EthereumServiceError:
                raise
            except Exception as error:
                _logger.error(
                    'unable to get details for cross-chain transaction hash '
                    f'for {transaction_hash} '
                    f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)

    def get_token_symbol_and_parsed_amount(self, token_address: str,
                                           amount: int) -> tuple[str, str]:
        """Get the token symbol and the parsed amount of the given
        amount.

        Parameters
        ----------
        token_address : str
            The address of the token
        amount : int
            The amount to be parsed

        Returns
        -------
        str
            The symbol of the token.
        str
            The parsed amount.

        """
        while True:
            try:
                erc20_contract = self._w3.eth.contract(
                    address=web3.Web3.to_checksum_address(token_address),
                    abi=_ERC20_ABI)
                symbol = erc20_contract.functions.symbol().call()
                decimals = erc20_contract.functions.decimals().call()
                parsed_amount = self.__format_balance(amount, decimals)
                return symbol, parsed_amount
            except Exception as error:
                _logger.error('unable to get token and symbol '
                              f'for {token_address} '
                              f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)

    def get_transaction_gas_paid(self, transaction_hash: str) -> int:
        """Get the transaction's amount of gas paid.

        """
        while True:
            try:
                transaction_receipt = self._w3.eth.get_transaction_receipt(
                    eth_typing.HexStr(transaction_hash))
                return transaction_receipt[
                    "effectiveGasPrice"] * transaction_receipt["gasUsed"]
            except Exception as error:
                _logger.error('unable to get receipt for '
                              f'for {transaction_hash} '
                              f'retrying in {REQUEST_RETRY_SECONDS}')
                _logger.error(f'error reason: {error}')
                time.sleep(REQUEST_RETRY_SECONDS)

    def __format_balance(self, balance: int, decimals: int) -> str:
        if balance == 0:
            return '0'
        unit = decimal.Decimal(str(10**decimals))
        decimal_balance = decimal.Decimal(balance) / unit
        return f'{decimal_balance:.{decimals}f}'
