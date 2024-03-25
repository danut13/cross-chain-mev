"""Module for accessing database records.

"""
import hexbytes
import sqlalchemy.exc

from src.api_utilities.zeromev import ZeroMev
from src.database import get_session
from src.database import get_session_maker
from src.database.exceptions import DatabaseError
from src.database.models import BlockModel
from src.database.models import TransactionModel
from src.domain import Block
from src.domain import MevType
from src.domain import PolygonBridgeInteraction
from src.domain import Transaction


def add_block(block: Block) -> None:
    """Add the given block to the database.

    Parameters
    ----------
    Block
        The block to be added.

    """
    statement = sqlalchemy.insert(BlockModel).values(
        block_number=block['number'], miner=block['miner'],
        timestamp=block['timestamp'])
    with get_session_maker().begin() as session:
        session.execute(statement)


def add_transactions(block_number: int,
                     transaction_hashes: list[bytes]) -> None:
    """Add the given transactions to the database. The block information
    for the given transaction must already exist in the database.

    Parameters
    ----------
    transaction_hashes : list of bytes
        The list of transactions.

    Raises
    ------
    DatabaseError
        If the block information for the given transaction has not
        already been added to the database.

    """
    transaction_models = []
    for i in range(0, len(transaction_hashes)):
        transaction_models.append(
            TransactionModel(transaction_hash=transaction_hashes[i],
                             transaction_index=i, block_id=block_number))
    try:
        with get_session_maker().begin() as session:
            session.bulk_save_objects(transaction_models)
    except sqlalchemy.exc.IntegrityError:
        raise DatabaseError('unkown block for the given transactions')


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


def get_all_block_numbers() -> list[int]:
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


def get_all_meved_block_numbers() -> list[int]:
    """Get the numbers of all MEVed blocks.

    Returns
    -------
    list of int
        The block numbers of the MEVed blocks.

    """
    statement = sqlalchemy.select(BlockModel.block_number).where(
        BlockModel.mev_added == sqlalchemy.true())
    with get_session() as session:
        block_numbers = session.execute(statement).scalars().all()
        session.expunge_all()
        return list(block_numbers)


def get_all_block_numbers_with_traces_processed() -> list[int]:
    """Get the numbers of all the blocks with the traces processed.

    Returns
    -------
    list of int
        The block numbers of all the blocks with the traces processed.

    """
    statement = sqlalchemy.select(BlockModel.block_number).where(
        BlockModel.traces_processed == sqlalchemy.true())
    with get_session() as session:
        block_numbers = session.execute(statement).scalars().all()
        session.expunge_all()
        return list(block_numbers)


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
    list of int
        The block numbers to be fetched.

    """
    statement = sqlalchemy.select(BlockModel.block_number).where(
        sqlalchemy.and_(BlockModel.block_number >= block_number_start,
                        BlockModel.block_number <= block_number_end))
    with get_session() as session:
        block_numbers = session.execute(statement).scalars().all()
        session.expunge_all()
        return list(block_numbers)


def get_non_meved_blocks(block_number_start: int,
                         block_number_end: int) -> list[int]:
    """The amount of blocks that have not been MEVed
    (updated with MEV details).

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the search from.
    block_number_end : int
        The number of the block to end the search at.

    Returns
    -------
    int
        The number of MEVed blocks.

    """
    statement = sqlalchemy.select(BlockModel.block_number).where(
        sqlalchemy.and_(BlockModel.block_number >= block_number_start,
                        BlockModel.block_number <= block_number_end,
                        BlockModel.mev_added == sqlalchemy.false()))
    with get_session() as session:
        block_numbers = session.execute(statement).scalars().all()
        session.expunge_all()
        return list(block_numbers)


def get_blocks_without_traces_processed(block_number_start: int,
                                        block_number_end: int) -> list[int]:
    """The amount of blocks that do not have the traces processed.

    Parameters
    ----------
    block_number_start : int
        The number of the block to start the search from.
    block_number_end : int
        The number of the block to end the search at.

    Returns
    -------
    int
        The number of blocks without the traces processed.

    """
    statement = sqlalchemy.select(BlockModel.block_number).where(
        sqlalchemy.and_(BlockModel.block_number >= block_number_start,
                        BlockModel.block_number <= block_number_end,
                        BlockModel.traces_processed == sqlalchemy.false()))
    with get_session() as session:
        block_numbers = session.execute(statement).scalars().all()
        session.expunge_all()
        return list(block_numbers)


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


def get_block_timestamp(block_number: int) -> int:
    """Get the timestamp of the given block number.

    Parameters
    ----------
    block_number : int
        The number of the block.

    Returns
    -------
    int
        The timestamp of the block.

    """
    statement = sqlalchemy.select(
        BlockModel.timestamp).where(BlockModel.block_number == block_number)
    with get_session() as session:
        return session.execute(statement).scalars().one()


def update_blocks_with_mev(block_number_start: int,
                           block_number_end: int) -> None:
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


def update_block_with_trace_processed(block_number: int) -> None:
    """Mark the block record as updated with traces processed.

    Parameters
    ----------
    block_number : int
        The number of the block.

    """
    statement = sqlalchemy.update(BlockModel).where(
        BlockModel.block_number == block_number).values(
            traces_processed=sqlalchemy.true())
    with get_session_maker().begin() as session:
        session.execute(statement)


def update_transaction_with_bridge_information(
        transaction_hash: str,
        polygon_bridge_interaction: PolygonBridgeInteraction) -> None:
    """Update a transaction record with Polygon bridge information.

    Parameters
    ----------
    transaction_hash : str
        The hash of the transaction to update.

    """
    statement = sqlalchemy.update(TransactionModel).where(
        TransactionModel.transaction_hash == transaction_hash).values(
            polygon_bridge_interaction=polygon_bridge_interaction.value)
    with get_session_maker().begin() as session:
        session.execute(statement)


def update_transaction_coinbase_transfer_value(
        transaction_hash: str, coinbase_transfer_value: str) -> None:
    """Update the transaction with the coinbase transfer value.

    Parameters
    ----------
    transaction_hash : str
        The hash of the transaction to update.
    coinbase_transfer_value : str
        The amount of Wei transfered to the builder in the transaction.

    """
    statement = sqlalchemy.update(TransactionModel).where(
        TransactionModel.transaction_hash == transaction_hash).values(
            coinbase_transfer_value=coinbase_transfer_value)
    with get_session_maker().begin() as session:
        session.execute(statement)


def update_transaction_with_mev(
        zero_mev_tranaction: ZeroMev.TransactionResponse) -> None:
    """Update a transaction record with MEV details.

    Parameters
    ----------
    ZeroMev.TransactionResponse
        MEV transaction details.

    """
    statement = sqlalchemy.update(TransactionModel).where(
        sqlalchemy.and_(
            TransactionModel.block_id == zero_mev_tranaction.block_number,
            TransactionModel.transaction_index == zero_mev_tranaction.
            transactiion_index)).values(mev_type=zero_mev_tranaction.mev_type)
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
    return Transaction(
        block_number=transaction_model.block_id,
        transaction_hash=hexbytes.HexBytes(
            transaction_model.transaction_hash).hex(),
        transaction_index=transaction_model.transaction_index,
        mev_type=MevType(transaction_model.mev_type),
        polygon_bridge_interaction=PolygonBridgeInteraction(
            transaction_model.polygon_bridge_interaction),
        coinbase_transfer_value=int(transaction_model.coinbase_transfer_value))
