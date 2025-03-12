from sqlalchemy import create_engine
from marc_db.models import Base


def create_database(database_url):
    """
    Create the database using the provided database URL.

    Parameters:
    database_url (str): The database URL.
    """
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)


def get_connection(database_url):
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
