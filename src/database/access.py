"""Module for accessing database records.

"""
import collections.abc

import hexbytes
import sqlalchemy.exc

from src.database import get_session
from src.database import get_session_maker
from src.database.exceptions import DatabaseError
from src.database.models import BlockModel
from src.database.models import TransactionModel
from src.domain import ADDRESS_ZERO
from src.domain import Block
from src.domain import ChecksumAddress
from src.domain import HexAddress
from src.domain import HexStr
from src.domain import MevType
from src.domain import ProtocolType
from src.domain import TimeStamp
from src.domain import Transaction
from src.domain import Wei
from src.zeromev import ZeroMev


def add_blocks(blocks: collections.abc.Iterable[Block]) -> None:
    """Add the given blocks to the database.

    Parameters
    ----------
    collections.abc.Iterable of Block
        The list of blocks.

    """
    block_models = []
    for block in blocks:
        block_models.append(_block_entity_to_model(block))
    with get_session_maker().begin() as session:
        session.bulk_save_objects(block_models)


def add_transactions(
        transactions: collections.abc.Iterable[Transaction]) -> None:
    """Add the given transactions to the database. The block information
    for the given transaction must already exist in the database.

    Parameters
    ----------
    transactions : collections.abc.Iterable of Transaction
        The list of transactions.

    Raises
    ------
    DatabaseError
        If the block information for the given transaction has not
        already been added to the database.

    """
    transaction_models = []
    for transaction in transactions:
        transaction_models.append(_transaction_entity_to_model(transaction))
    try:
        with get_session_maker().begin() as session:
            session.bulk_save_objects(transaction_models)
    except sqlalchemy.exc.IntegrityError:
        raise DatabaseError('unkown block for the given transactions')


def get_block_builder_address(block_number: int) -> str:
    """Get the block builder address given the block number.

    Parameters
    ----------
    block_number : int
        The block number.

    Returns
    -------
    str
        The address of the block builder.

    """
    statement = sqlalchemy.select(
        BlockModel.miner).where(BlockModel.block_number == block_number)
    with get_session() as session:
        builder = session.execute(statement).scalars().one()
        return builder


def update_transaction_coinbase_transfer_value(transaction_hash: str,
                                               coinbase_transfer_value: str):
    """Update the transaction with the coinbase transfer value.

    Parameters
    ----------
    transaction_hash : str
        The hash of the transaction to update.
    coinbase_transfer_value : str
        The amount of Wei transfered to the builder in the transaction.

    """
    statement = sqlalchemy.update(TransactionModel).where(
        TransactionModel.hash == transaction_hash).values(
            coinbase_transfer_value=coinbase_transfer_value)
    with get_session_maker().begin() as session:
        session.execute(statement)


def get_blocks(block_number_start: int, block_number_end: int) -> list[Block]:
    """Get the blocks which correspond to the given block numbers.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the search from.
    block_number_end : int
        The number of the block to end the search at.

    Returns
    -------
    list of Block
        The blocks to be fetched from the database.

    """
    statement = sqlalchemy.select(BlockModel).where(
        sqlalchemy.and_(BlockModel.block_number >= block_number_start,
                        BlockModel.block_number <= block_number_end))
    with get_session() as session:
        block_models = session.execute(statement).scalars().all()
        session.expunge_all()
        blocks = [
            _block_model_to_entity(block_model) for block_model in block_models
        ]
        return blocks


def get_saved_block_numbers(block_number_start: int,
                            block_number_end: int) -> list[int]:
    """Get the saved block numbers which are in the
    given interval.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the search from.
    block_number_end : int
        The number of the block to end the search at.

    Returns
    -------
    list of Block
        The block numbers to be fetched.

    """
    statement = sqlalchemy.select(BlockModel.block_number).where(
        sqlalchemy.and_(BlockModel.block_number >= block_number_start,
                        BlockModel.block_number <= block_number_end))
    with get_session() as session:
        block_numbers = session.execute(statement).scalars().all()
        session.expunge_all()
        return list(block_numbers)


def get_transactions(block_number_start: int,
                     block_number_end: int) -> list[Transaction]:
    """Get the transactions of the given block numbers.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the search from.
    block_number_end : int
        The number of the block to end the search at.

    Returns
    -------
    list of Transaction
        The transactions to be fetched from the database.

    """
    statement = sqlalchemy.select(TransactionModel).where(
        sqlalchemy.and_(TransactionModel.block_id >= block_number_start,
                        TransactionModel.block_id <= block_number_end))
    with get_session() as session:
        transaction_models = session.execute(statement).scalars().all()
        session.expunge_all()
        transactions = [
            _transaction_model_to_entity(transaction_model)
            for transaction_model in transaction_models
        ]
        return transactions


def get_all_blocks() -> list[int]:
    """Get the numbers of all the blocks saved.

    Returns
    -------
    list of int
        The block numbers of all the blocks saved.

    """
    statement = sqlalchemy.select(BlockModel.block_number)
    with get_session() as session:
        block_numbers = session.execute(statement).scalars().all()
        session.expunge_all()
        return list(block_numbers)


def get_non_meved_blocks_count(block_number_start: int,
                               block_number_end: int) -> int:
    """The amount of blocks that have not been MEVed
    (updated with MEV details).

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the search from.
    block_number_end : int
        The number of the block to end the search at.

    int
        The number of MEVed blocks.

    """
    statement = sqlalchemy.select(sqlalchemy.func.count()).where(
        sqlalchemy.and_(BlockModel.block_number >= block_number_start,
                        BlockModel.block_number <= block_number_end,
                        BlockModel.mev_added == sqlalchemy.false()))
    with get_session() as session:
        non_meved_blocks_number = session.execute(statement).scalars().all()
        return non_meved_blocks_number[0]


def get_blocks_without_coinbase_transaction_count(
        block_number_start: int, block_number_end: int) -> int:
    """The amount of blocks that do not have the
    coinbase transactions added.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the search from.
    block_number_end : int
        The number of the block to end the search at.

    int
        The number of blocks with the coinbase transactions added.

    """
    statement = sqlalchemy.select(sqlalchemy.func.count()).where(
        sqlalchemy.and_(
            BlockModel.block_number >= block_number_start,
            BlockModel.block_number <= block_number_end,
            BlockModel.coinbase_transfer_added == sqlalchemy.false()))
    with get_session() as session:
        result = session.execute(statement).scalars().all()
        return result[0]


def update_blocks_with_mev(block_number_start: int, block_number_end: int):
    """Mark a block record as updated with MEV details.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the update from.
    block_number_end : int
        The number of the block to end the update at.

    """
    statement = sqlalchemy.update(BlockModel).where(
        sqlalchemy.and_(
            BlockModel.block_number >= block_number_start,
            BlockModel.block_number
            <= block_number_end)).values(mev_added=sqlalchemy.true())
    with get_session_maker().begin() as session:
        session.execute(statement)


def update_blocks_with_coinbase_transfer(block_number_start: int,
                                         block_number_end: int):
    """Mark block records as updated with coinbase transfers.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the update from.
    block_number_end : int
        The number of the block to end the update at.

    """
    statement = sqlalchemy.update(BlockModel).where(
        sqlalchemy.and_(BlockModel.block_number >= block_number_start,
                        BlockModel.block_number <= block_number_end)).values(
                            coinbase_transfer_added=sqlalchemy.true())
    with get_session_maker().begin() as session:
        session.execute(statement)


def update_transaction_with_bridge_information(transaction_hash: str):
    """Update a transaction record with Polygon bridge information.

    Parameters
    ----------
    transaction_hash : str
        The hash of the transaction to update.

    """
    statement = sqlalchemy.update(TransactionModel).where(
        TransactionModel.hash == transaction_hash).values(
            interacts_with_polygon_bridge=sqlalchemy.true())
    with get_session_maker().begin() as session:
        session.execute(statement)


def update_transaction_with_mev(
        zero_mev_tranaction: ZeroMev.TransactionResponse):
    """Update a transaction record with MEV details.

    Parameters
    ----------
    ZeroMev.TransactionResponse
        MEV transaction details.

    """
    statement = sqlalchemy.update(TransactionModel).where(
        sqlalchemy.and_(
            TransactionModel.block_id == zero_mev_tranaction.block_number,
            TransactionModel.transaction_index ==
            zero_mev_tranaction.transactiion_index)).values(
                mev_type=zero_mev_tranaction.mev_type,
                protocol_type=zero_mev_tranaction.protocol_type,
                user_swap_count=zero_mev_tranaction.user_swap_count)
    with get_session_maker().begin() as session:
        session.execute(statement)


def delete_block_data(block_number_start: int, block_number_end: int) -> int:
    """Delete the blocks and their correpsonding transactions data.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the deletion from.
    block_number_end : int
        The number of the block to end the deletion at.

    int
        The number of deleted blocks.

    """
    delete_transactions_statement = sqlalchemy.delete(TransactionModel).where(
        sqlalchemy.and_(TransactionModel.block_id >= block_number_start,
                        TransactionModel.block_id <= block_number_end))
    delete_blocks_statement = sqlalchemy.delete(BlockModel).where(
        sqlalchemy.and_(BlockModel.block_number >= block_number_start,
                        BlockModel.block_number <= block_number_end))
    with get_session_maker().begin() as session:
        session.execute(delete_transactions_statement)
        number_of_deleted_blocks = session.execute(
            delete_blocks_statement).rowcount
        return number_of_deleted_blocks


def _block_model_to_entity(block_model: BlockModel) -> Block:
    """Maps a block database model to a block entity.

    Parameters
    ----------
    block_model : BlockModel
        The block database model.

    Returns
    -------
    Block
        The block entity.

    """
    return Block({
        'hash': hexbytes.HexBytes(block_model.hash),
        'miner': ChecksumAddress(HexAddress(HexStr(block_model.miner))),
        'timestamp': TimeStamp(block_model.timestamp),
        'size': block_model.size,
        'gasUsed': block_model.gas_used,
        'baseFeePerGas': Wei(block_model.base_fee_per_gas),
        'logsBloom': hexbytes.HexBytes(block_model.logs_bloom)
    })


def _block_entity_to_model(block: Block) -> BlockModel:
    """Maps a block entity to a block database model.

    Parameters
    ----------
    block : Block
        The block entity.

    Returns
    -------
    BlockModel
        The block database model.

    """
    return BlockModel(block_number=block['number'], hash=block['hash'],
                      miner=block['miner'], timestamp=block['timestamp'],
                      size=block['size'], gas_used=block['gasUsed'],
                      base_fee_per_gas=block['baseFeePerGas'],
                      logs_bloom=block['logsBloom'])


def _transaction_entity_to_model(transaction: Transaction) -> TransactionModel:
    """Maps a transaction entity to a transaction database model.

    Parameters
    ----------
    Transaction
        The transaction entity.

    Returns
    -------
    TransactionModel
        The transaction database model.

    """
    return TransactionModel(
        hash=transaction['hash'],
        transaction_index=transaction['transactionIndex'],
        input=transaction['input'], value=str(transaction['value']),
        from_=transaction['from'], to=transaction['to'],
        gas=transaction['gas'], gas_price=str(transaction['gasPrice'])
        if 'gasPrice' in transaction else None,
        max_fee_per_gas=str(transaction['maxFeePerGas'])
        if 'maxFeePerGas' in transaction else None,
        max_priority_fee_per_gas=str(transaction['maxPriorityFeePerGas'])
        if 'maxPriorityFeePerGas' in transaction else None,
        type=transaction['type'], block_id=transaction['blockNumber'])


def _transaction_model_to_entity(
        transaction_model: TransactionModel) -> Transaction:
    """Maps a transaction database model to a entity.

    Parameters
    ----------
    transaction_model : TransactionModel
        The transaction database model.

    Returns
    -------
    Transaction
        The transaction entity.

    """
    return Transaction({
        'blockNumber': transaction_model.block_id,  # type: ignore
        'hash': hexbytes.HexBytes(transaction_model.hash),
        'transactionIndex': transaction_model.transaction_index,
        'mev_type': MevType(transaction_model.mev_type)
        if transaction_model.mev_type is not None else None,
        'protocol_type': ProtocolType(transaction_model.protocol_type)
        if transaction_model.protocol_type is not None else None,
        'coinbase_transfer_value': int(
            transaction_model.coinbase_transfer_value)
        if transaction_model.coinbase_transfer_value is not None else None,
        'user_swap_count': transaction_model.user_swap_count,
        'interacts_with_polygon_bridge': transaction_model.
        interacts_with_polygon_bridge,
        'input': hexbytes.HexBytes(transaction_model.input),
        'value': Wei(int(transaction_model.value)),
        'from': ChecksumAddress(HexAddress(HexStr(transaction_model.from_))),
        'to': ChecksumAddress(HexAddress(HexStr(transaction_model.to)))
        if transaction_model.to is not None else ChecksumAddress(ADDRESS_ZERO),
        'gas': transaction_model.gas,
        'gasPrice': Wei(int(transaction_model.gas_price))
        if transaction_model.gas_price is not None else Wei(0),
        'maxPriorityFeePerGas': Wei(
            int(transaction_model.max_priority_fee_per_gas))
        if transaction_model.max_priority_fee_per_gas is not None else Wei(0),
        'maxFeePerGas': Wei(int(transaction_model.max_fee_per_gas))
        if transaction_model.max_fee_per_gas is not None else Wei(0),
        'type': transaction_model.type
    })
