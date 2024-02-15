"""Module which encapsulates the core domain objects.

"""
import enum
import typing

import eth_typing
import web3.constants
import web3.types

Block = web3.types.BlockData
HexStr = eth_typing.encoding.HexStr
ChecksumAddress = eth_typing.ChecksumAddress
HexAddress = eth_typing.HexAddress
TimeStamp = web3.types.Timestamp
Wei = web3.types.Wei
ADDRESS_ZERO = web3.constants.ADDRESS_ZERO
TransactionTrace = typing.NewType('TransactionTrace', dict[str, typing.Any])
BlockTrace = typing.NamedTuple(
    'BlockTrace', [('block_number', int),
                   ('transaction_traces', list[TransactionTrace])])


class MevType(enum.IntEnum):
    """Enumeration of MEV types.

    """
    SANDWICH = 0
    BACKRUN = 1
    LIQUID = 2
    ARB = 3
    FRONTRUN = 4
    SWAP = 5

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


class ProtocolType(enum.IntEnum):
    """Enumeration of Protocol types. For the scope of this analysis,
    it's only relevant if the transaction has toucheda single
    or multiple protocols.

    """
    NONE = 0
    SINGLE = 1
    MULTIPLE = 2

    @staticmethod
    def from_name(name: typing.Optional[str]) -> 'ProtocolType':
        """Find an enumeration member by its name.

        Parameters
        ----------
        name : str
            The name to search for.

        """
        if name is None:
            return ProtocolType.NONE
        if name == 'multiple':
            return ProtocolType.MULTIPLE
        return ProtocolType.SINGLE


class Transaction(web3.types.TxData):
    mev_type: typing.Optional[MevType]
    protocol_type: typing.Optional[ProtocolType]
    coinbase_transfer_value: int
    user_swap_count: int
    interacts_with_polygon_bridge: bool
