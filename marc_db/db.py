import os
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.orm import sessionmaker, Session

from marc_db.models import Base


def get_marc_db_url() -> str:
    try:
        os.environ["MARC_DB_URL"]
    except KeyError:
        print("MARC_DB_URL environment variable not set, using in-memory db")
        return "sqlite:///:memory:"
    return os.environ["MARC_DB_URL"]


def create_database(database_url: Optional[str] = None):
    """
    Create the database tables that don't exist using the provided database URL.

    Parameters:
    database_url (str): The database URL.
    """
    if database_url is None:
        database_url = get_marc_db_url()
    engine = create_engine(database_url)
    Base.metadata.create_all(engine, checkfirst=True)


def get_connection(database_url: Optional[str] = None) -> Connection:
    """
    Get a connection to the database using the provided database URL.

    Parameters:
    database_url (str): The database URL.

    Returns:
    connection: The connection to the database.
    """
    if database_url is None:
        database_url = get_marc_db_url()
    engine = create_engine(database_url)
    connection = engine.connect()
    return connection


def get_session(database_url: Optional[str] = None) -> Session:
    """
    Get a session to the database using the provided database URL.

    Parameters:
    database_url (str): The database URL.

    Returns:
    session: The session to the database.
    """
    from sqlalchemy.orm import sessionmaker

    if database_url is None:
        database_url = get_marc_db_url()
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
