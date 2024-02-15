"""Package for managing and accessing the database.

"""
import typing

import sqlalchemy.orm
import sqlalchemy.schema

from src.database.models import Base

_session_maker: typing.Optional[sqlalchemy.orm.sessionmaker] = None
"""Factory object for creating database sessions."""


def get_session_maker() -> sqlalchemy.orm.sessionmaker:
    """Get the session maker for making sessions for managing
    persistence operations for ORM-mapped objects.

    Returns
    -------
    sqlalchemy.orm.sessionmaker
        The session maker.

    Raises
    ------
    DatabaseError
        If the database package has not been initialized.

    """
    if _session_maker is None:
        raise Exception('database package not yet initialized')
    return _session_maker


def get_session() -> sqlalchemy.orm.Session:
    """Get a session for managing persistence operations for ORM-mapped
    objects.

    Returns
    -------
    sqlalchemy.orm.Session
        The session.

    Raises
    ------
    DatabaseError
        If the database package has not been initialized.

    """
    return get_session_maker()()


def initialize_database() -> None:
    """Initialize the database.

    """
    sql_engine = sqlalchemy.create_engine('sqlite:///data/blockchain_data.db')
    with sql_engine.connect() as connection:
        connection.execute(sqlalchemy.text('PRAGMA foreign_keys=ON;'))
    Base.metadata.create_all(sql_engine)
    global _session_maker
    _session_maker = sqlalchemy.orm.sessionmaker(bind=sql_engine)
