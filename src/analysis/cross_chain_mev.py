"""Module for finding cross-cain MEV candidates.

"""
import collections
import typing

from src.database.access import get_transactions
from src.domain import MevType
from src.domain import PolygonBridgeInteraction
from src.domain import Transaction


class CrossChainMev:
    """Class for finding cross-chain MEV candidates.

    """
    def find_cross_chain_mev_candidates(
            self, block_number_start: int,
            block_number_end: int) -> dict[int, list[Transaction]]:
        """Find cross-chain MEV candidates. Those are non atomic MEV
        transactions which also interact with the Polygon bridge.

        Parameters
        ----------
        block_number_start : int
            The number of the starting block.
        block_number_end : int
            The number of the ending block.

        Returns
        -------
        dict of int, list of Transaction
            The block number to the list of candidate transactions.

        """
        transactions = get_transactions(block_number_start, block_number_end)
        block_number_to_transactions = \
            self.__create_block_number_to_transactions_dict(
                transactions)
        block_to_non_atomic_mev_transactions = {}
        block_to_cross_chain_mev_transactions = {}
        for block_number in range(block_number_start, block_number_end + 1):
            non_atomic_mev_transactions, cross_chain_mev_transactions = \
                self.__analyze_block_transactions(
                    block_number_to_transactions[block_number])
            if len(non_atomic_mev_transactions) > 0:
                block_to_non_atomic_mev_transactions[
                    block_number] = non_atomic_mev_transactions
            if len(cross_chain_mev_transactions) > 0:
                block_to_cross_chain_mev_transactions[
                    block_number] = cross_chain_mev_transactions
        return block_to_cross_chain_mev_transactions

    def __create_block_number_to_transactions_dict(
            self, transactions: list[Transaction]) \
            -> typing.DefaultDict[int, list[Transaction]]:
        block_number_to_transactions = collections.defaultdict(list)
        for transaction in transactions:
            block_number_to_transactions[int(
                transaction.block_number)].append(transaction)
        return block_number_to_transactions

    def __analyze_block_transactions(self, transactions: list[Transaction]) \
            -> tuple[list[Transaction], list[Transaction]]:
        non_atomic_mev_transactions: list[Transaction] = []
        cross_chain_mev_transactions: list[Transaction] = []
        transactions_in_block = len(transactions)
        for transaction in transactions:
            if self.__is_transaction_non_atomic_mev(transaction,
                                                    transactions_in_block):
                non_atomic_mev_transactions.append(transaction)
                if (transaction.polygon_bridge_interaction
                        is not PolygonBridgeInteraction.NONE):
                    cross_chain_mev_transactions.append(transaction)
        return non_atomic_mev_transactions, cross_chain_mev_transactions

    def __is_transaction_non_atomic_mev(self, transaction: Transaction,
                                        transactions_in_block: int) -> bool:
        if transaction.mev_type is not MevType.SWAP:
            return False
        if transaction.coinbase_transfer_value > 0:
            return True
        if transaction.transaction_index < (0.1 * transactions_in_block):
            return True
        return False
