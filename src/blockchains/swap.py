"""Module for processing swap events.

"""
import typing

import eth_typing
import web3
import web3.logs
import web3.types

from src.blockchains.ethereum import EthereumService
from src.domain import Swap
from src.exceptions import BaseError

_UNISWAP_POOL = [{
    "inputs": [],
    "name": "token0",
    "outputs": [{
        "internalType": "address",
        "name": "",
        "type": "address"
    }],
    "stateMutability": "view",
    "type": "function"
}, {
    "inputs": [],
    "name": "token1",
    "outputs": [{
        "internalType": "address",
        "name": "",
        "type": "address"
    }],
    "stateMutability": "view",
    "type": "function"
}]

_UNISWAP_V2_EVENT = [{
    "name": "Swap",
    "type": "event",
    "anonymous": False,
    "inputs": [{
        "indexed": True,
        "name": "sender",
        "type": "address"
    }, {
        "indexed": False,
        "name": "amount0In",
        "type": "uint256"
    }, {
        "indexed": False,
        "name": "amount1In",
        "type": "uint256"
    }, {
        "indexed": False,
        "name": "amount0Out",
        "type": "uint256"
    }, {
        "indexed": False,
        "name": "amount1Out",
        "type": "uint256"
    }, {
        "indexed": True,
        "name": "to",
        "type": "address"
    }]
}]

_UNISWAP_V3_EVENT = [{
    "name": "Swap",
    "type": "event",
    "anonymous": False,
    "inputs": [{
        "indexed": True,
        "name": "sender",
        "type": "address"
    }, {
        "indexed": True,
        "name": "recipient",
        "type": "address"
    }, {
        "indexed": False,
        "name": "amount0",
        "type": "int256"
    }, {
        "indexed": False,
        "name": "amount1",
        "type": "int256"
    }, {
        "indexed": False,
        "name": "sqrtPriceX96",
        "type": "uint160"
    }, {
        "indexed": False,
        "name": "liquidity",
        "type": "uint128"
    }, {
        "indexed": False,
        "name": "tick",
        "type": "int24"
    }]
}]


class SwapProcessorError(BaseError):
    """Exception class for all swap processor errors.

    Attributes
    ----------
    transaction_hash : str
        The hash of the transaction.

    """
    def __init__(self, message: str, transaction_hash: str):
        super().__init__(message)
        self.transaction_hash = transaction_hash


class SwapProcessor:
    """Class for processing swap information of transactions.

    """
    def __init__(self, blockchain_service: EthereumService):
        self.__w3 = blockchain_service.get_web3()
        self.__uniswap_v2_contract = self.__w3.eth.contract(
            abi=_UNISWAP_V2_EVENT)
        self.__uniswap_v3_contract = self.__w3.eth.contract(
            abi=_UNISWAP_V3_EVENT)

    def process_transaction(
            self, transaction_hash: str) -> typing.Optional[list[Swap]]:
        """Process the swaps of a transaction.

        Parameters
        ----------
        transactions_hash : str
            The hash of the transaction.

        Returns
        -------
        Swap
            The swap information or None if no swap was found.

        Raises
        ------
        SwapProcessorError
            If something went wrong.

        """
        receipt = self.__w3.eth.get_transaction_receipt(
            eth_typing.HexStr(transaction_hash))
        v2_decoded_logs = self.__uniswap_v2_contract.events.Swap(
        ).process_receipt(receipt, errors=web3.logs.DISCARD)
        v3_decoded_logs = self.__uniswap_v3_contract.events.Swap(
        ).process_receipt(receipt, errors=web3.logs.DISCARD)

        all_swaps: list[Swap] = []

        for v2_log in v2_decoded_logs:
            all_swaps.append(self.__process_v2_swap(v2_log))
        for v3_log in v3_decoded_logs:
            all_swaps.append(self.__process_v3_swap(v3_log))

        if len(all_swaps) == 0:
            return None

        all_swaps.sort(key=lambda swap: swap.event_index)  # type: ignore

        if len(all_swaps) > 1:
            for i in range(len(all_swaps) - 1):
                if all_swaps[i].token_out != all_swaps[i + 1].token_in:
                    # this can mean that maybe 2 different unrelated swaps
                    # are performed; we can handle this in the future if needed
                    raise SwapProcessorError('multiple swaps detected',
                                             transaction_hash=transaction_hash)

        return all_swaps

    def __process_v2_swap(self, log: web3.types.EventData) -> Swap:
        if log['args']['amount0In'] == 0:
            token_started_with = self.__get_token1(log['address'])
            amount_started_with = log['args']['amount1In']
            token_ended_up_with = self.__get_token0(log['address'])
            amount_ended_up_with = log['args']['amount0Out']
        else:
            token_started_with = self.__get_token0(log['address'])
            amount_started_with = log['args']['amount0In']
            token_ended_up_with = self.__get_token1(log['address'])
            amount_ended_up_with = log['args']['amount1Out']
        return Swap(token_started_with, token_ended_up_with,
                    amount_started_with, amount_ended_up_with, log['logIndex'])

    def __process_v3_swap(self, log: web3.types.EventData) -> Swap:
        amount0 = log['args']['amount0']
        amount1 = log['args']['amount1']
        if amount0 < 0:
            token_started_with = self.__get_token1(log['address'])
            amount_started_with = amount1
            token_ended_up_with = self.__get_token0(log['address'])
            amount_ended_up_with = -amount0
        else:
            token_started_with = self.__get_token0(log['address'])
            amount_started_with = amount0
            token_ended_up_with = self.__get_token1(log['address'])
            amount_ended_up_with = -amount1
        return Swap(token_started_with, token_ended_up_with,
                    amount_started_with, amount_ended_up_with, log['logIndex'])

    def __get_token0(self, address: eth_typing.ChecksumAddress) -> str:
        pool_contract = self.__w3.eth.contract(address=address,
                                               abi=_UNISWAP_POOL)
        return pool_contract.functions.token0().call()

    def __get_token1(self, address: eth_typing.ChecksumAddress) -> str:
        pool_contract = self.__w3.eth.contract(address=address,
                                               abi=_UNISWAP_POOL)
        return pool_contract.functions.token1().call()
