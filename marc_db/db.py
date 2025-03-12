import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from marc_db.models import Base


def get_marc_db_url() -> str:
    try:
        os.environ["MARC_DB_URL"]
    except KeyError:
        print("MARC_DB_URL environment variable not set, using in-memory db")
        return "sqlite:///:memory:"
    return os.environ["MARC_DB_URL"]


def create_database(database_url: str = get_marc_db_url()):
    """
    Create the database tables that don't exist using the provided database URL.

    Parameters:
    database_url (str): The database URL.
    """
    engine = create_engine(database_url)
    Base.metadata.create_all(engine, checkfirst=True)


def get_connection(database_url: str = get_marc_db_url()) -> Connection:
    """
    Get a connection to the database using the provided database URL.

    Parameters:
    database_url (str): The database URL.

    Returns:
    connection: The connection to the database.
    """
    engine = create_engine(database_url)
    connection = engine.connect()
    return connection
