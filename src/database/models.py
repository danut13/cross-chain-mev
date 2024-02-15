"""Module that defines the database models.

"""
import typing

import sqlalchemy.orm
import sqlalchemy.schema


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
    mev_added: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(
        default=False)
    coinbase_transfer_added: sqlalchemy.orm.Mapped[
        bool] = sqlalchemy.orm.mapped_column(default=False)
    hash: sqlalchemy.orm.Mapped[bytes]
    miner: sqlalchemy.orm.Mapped[str]
    timestamp: sqlalchemy.orm.Mapped[int]
    size: sqlalchemy.orm.Mapped[int]
    gas_used: sqlalchemy.orm.Mapped[int]
    base_fee_per_gas: sqlalchemy.orm.Mapped[int]
    logs_bloom: sqlalchemy.orm.Mapped[bytes]
    transactions: sqlalchemy.orm.Mapped[
        list['TransactionModel']] = sqlalchemy.orm.relationship(
            back_populates="block")


class TransactionModel(Base):
    """Model class for "transactions". Each instance is a transaction
    which corresponds to a block.

    """
    __tablename__ = "transactions"

    hash: sqlalchemy.orm.Mapped[bytes] = sqlalchemy.orm.mapped_column(
        primary_key=True)
    transaction_index: sqlalchemy.orm.Mapped[int]
    mev_type: sqlalchemy.orm.Mapped[typing.Optional[int]]
    coinbase_transfer_value: sqlalchemy.orm.Mapped[
        str] = sqlalchemy.orm.mapped_column(default='0')
    protocol_type: sqlalchemy.orm.Mapped[typing.Optional[int]]
    user_swap_count: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        default=0)
    interacts_with_polygon_bridge: sqlalchemy.orm.Mapped[
        bool] = sqlalchemy.orm.mapped_column(default=False)
    input: sqlalchemy.orm.Mapped[bytes]
    value: sqlalchemy.orm.Mapped[str]
    from_: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        name='from')
    to: sqlalchemy.orm.Mapped[typing.Optional[str]]
    gas: sqlalchemy.orm.Mapped[int]
    gas_price: sqlalchemy.orm.Mapped[typing.Optional[str]]
    max_fee_per_gas: sqlalchemy.orm.Mapped[typing.Optional[str]]
    max_priority_fee_per_gas: sqlalchemy.orm.Mapped[typing.Optional[str]]
    type: sqlalchemy.orm.Mapped[int]
    block_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("blocks.block_number"), index=True)
    block: sqlalchemy.orm.Mapped['BlockModel'] = sqlalchemy.orm.relationship(
        back_populates="transactions")
