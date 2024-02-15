"""Module for analyzing the data.

"""
import collections
import typing

from src.database.access import get_transactions
from src.domain import MevType
from src.domain import Transaction


def find_non_atomic_mev(block_number_start: int, block_number_end: int):
    """Find the transactions which are extracting non atomic MEV.

    """
    transactions = get_transactions(block_number_start, block_number_end)
    block_number_to_transactions = _create_block_number_to_transactions_dict(
        transactions)
    block_to_non_atomic_mev_transactions = {}
    block_to_cross_chain_mev_transactions = {}
    for block_number in range(block_number_start, block_number_end + 1):
        non_atomic_mev_transactions, cross_chain_mev_transactions = \
            _analyze_block_transactions(
                block_number_to_transactions[block_number])
        if len(non_atomic_mev_transactions) > 0:
            block_to_non_atomic_mev_transactions[
                block_number] = non_atomic_mev_transactions
        if len(cross_chain_mev_transactions) > 0:
            block_to_cross_chain_mev_transactions[
                block_number] = cross_chain_mev_transactions
    total_txs = 0
    for key in block_to_non_atomic_mev_transactions:
        total_txs += len(block_to_non_atomic_mev_transactions[key])
    print(total_txs)

    total_txs = 0
    for key in block_to_cross_chain_mev_transactions:
        total_txs += len(block_to_cross_chain_mev_transactions[key])
    print(total_txs)


def _analyze_block_transactions(
        transactions: list[Transaction]) \
        -> tuple[list[Transaction], list[Transaction]]:
    non_atomic_mev_transactions: list[Transaction] = []
    cross_chain_mev_transactions: list[Transaction] = []
    transactions_in_block = len(transactions)
    for transaction in transactions:
        if _is_transaction_non_atomic_mev(transaction, transactions_in_block):
            non_atomic_mev_transactions.append(transaction)
            if transaction['interacts_with_polygon_bridge']:
                print(transaction['hash'].hex())
                cross_chain_mev_transactions.append(transaction)
    return non_atomic_mev_transactions, cross_chain_mev_transactions


def _is_transaction_non_atomic_mev(transaction: Transaction,
                                   transactions_in_block: int) -> bool:
    if transaction['mev_type'] is not MevType.SWAP:
        return False
    if transaction['coinbase_transfer_value'] > 0:
        return True
    if transaction['transactionIndex'] < (0.1 * transactions_in_block):
        return True
    return False


def _create_block_number_to_transactions_dict(
        transactions: list[Transaction]) \
        -> typing.DefaultDict[int, list[Transaction]]:
    block_number_to_transactions = collections.defaultdict(list)
    for transaction in transactions:
        block_number_to_transactions[int(
            transaction['blockNumber'])].append(transaction)
    return block_number_to_transactions
