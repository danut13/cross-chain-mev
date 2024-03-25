"""Module that defines the database models.

"""
import sqlalchemy.orm
import sqlalchemy.schema

from src.domain import MevType
from src.domain import PolygonBridgeInteraction


class Base(sqlalchemy.orm.DeclarativeBase):
    """Base class used for declarative class definitions.

    """
    pass


class BlockModel(Base):
    """Model class for "blocks". Each instance is a blockchain block.

    """
    __tablename__ = "blocks"

    block_number: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key=True)
    miner: sqlalchemy.orm.Mapped[str]
    timestamp: sqlalchemy.orm.Mapped[int]
    mev_added: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(
        default=False)
    traces_processed: sqlalchemy.orm.Mapped[
        bool] = sqlalchemy.orm.mapped_column(default=False)
    transactions: sqlalchemy.orm.Mapped[
        list['TransactionModel']] = sqlalchemy.orm.relationship(
            back_populates="block")


class TransactionModel(Base):
    """Model class for "transactions". Each instance is a transaction
    which corresponds to a block.

    """
    __tablename__ = "transactions"

    transaction_hash: sqlalchemy.orm.Mapped[
        bytes] = sqlalchemy.orm.mapped_column(primary_key=True)
    transaction_index: sqlalchemy.orm.Mapped[int]
    mev_type: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        default=MevType.NONE.value)
    polygon_bridge_interaction: sqlalchemy.orm.Mapped[
        int] = sqlalchemy.orm.mapped_column(
            default=PolygonBridgeInteraction.NONE.value)
    coinbase_transfer_value: sqlalchemy.orm.Mapped[
        str] = sqlalchemy.orm.mapped_column(default='0')
    block_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("blocks.block_number"), index=True)
    block: sqlalchemy.orm.Mapped['BlockModel'] = sqlalchemy.orm.relationship(
        back_populates="transactions")
