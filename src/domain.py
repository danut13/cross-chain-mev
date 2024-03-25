"""Module which encapsulates the core domain objects.

"""
import dataclasses
import enum
import typing

import web3.constants
import web3.contract.contract
import web3.types


class MevType(enum.IntEnum):
    """Enumeration of MEV types.

    """
    NONE = 0
    SANDWICH = 1
    BACKRUN = 2
    LIQUID = 3
    ARB = 4
    FRONTRUN = 5
    SWAP = 6

    @staticmethod
    def from_name(name: str) -> 'MevType':
        """Find an enumeration member by its name.

        Parameters
        ----------
        name : str
            The name to search for.

        Raises
        ------
        NameError
            If no enumeration member can be found for the given name.

        """
        name_upper = name.upper()
        for mev_type in MevType:
            if name_upper == mev_type.name:
                return mev_type
        raise NameError(name)


class PolygonBridgeInteraction(enum.IntEnum):
    """Enumeration of Polygon bridge interaction types.

    """
    NONE = 0
    FROM_ETHEREUM = 1
    TO_ETHEREUM = 2


@dataclasses.dataclass
class Transaction:
    """The transaction model.

    """
    block_number: int
    transaction_hash: str
    transaction_index: int
    mev_type: MevType
    polygon_bridge_interaction: PolygonBridgeInteraction
    coinbase_transfer_value: int


ADDRESS_ZERO = web3.constants.ADDRESS_ZERO
Block = web3.types.BlockData
TransactionTrace = typing.NewType('TransactionTrace', dict[str, typing.Any])
BlockTrace = typing.NamedTuple(
    'BlockTrace', [('block_number', int),
                   ('transaction_traces', list[TransactionTrace])])


@dataclasses.dataclass
class Swap:
    """Information about the swap.

    """
    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    event_index: typing.Optional[int] = None


@dataclasses.dataclass
class EthereumLeg:
    """The Ethereum leg information of the cross-chain MEV extraction.

    """
    token_address: str
    transaction_hash: str
    searcher_eoa_address: str
    searcher_contract_address: str
    swaps: list[Swap]
    gas_paid: typing.Optional[int] = None


@dataclasses.dataclass
class PolygonLeg:
    """The Polygon leg information of the cross-chain MEV extraction.

    """
    token_address: str
    bridge_transaction_hash: str
    swap_transaction_hash: str
    searcher_eoa_address: str
    searcher_contract_address: str
    swaps: list[Swap]
    bridge_transaction_gas_paid: typing.Optional[int] = None
    swap_transaction_gas_paid: typing.Optional[int] = None


@dataclasses.dataclass
class CrossChainMevExtraction:
    """Cross-chain MEV extraction information.

    """
    ethereum_leg: EthereumLeg
    polygon_leg: PolygonLeg
    direction: PolygonBridgeInteraction
    amount_bridged: int
    is_cyclic_arbitrage: bool = False
    profit_amount: typing.Optional[str] = None
    profit_token_symbol: typing.Optional[str] = None


@dataclasses.dataclass
class CrossChainMevFailedExtraction:
    """Failed cross-chain MEV extraction information.

    """
    ethereum_leg: EthereumLeg
    bridge_from_ethereum_transaction_hash: str
    bridge_to_ethereum_transaction_hash: str
    direction: PolygonBridgeInteraction
    amount_bridged: int
