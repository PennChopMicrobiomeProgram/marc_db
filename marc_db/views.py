from marc_db.db import get_connection
from marc_db.models import Aliquot, Isolate
from sqlalchemy.sql.expression import select


def get_isolates(n: int = None) -> list[Isolate]:
    """
    Get a list of Isolate objects from the database.

    Parameters:
    n (int): The number of isolates to return. If None, return all isolates.

    Returns:
    List[Isolate]: A list of Isolate objects.
    """
    connection = get_connection()
    query = connection.execute(select(Isolate).limit(n))
    isolates = query.fetchall()
    return isolates


def get_aliquots(n: int = None) -> list[Aliquot]:
    """
    Get a list of Aliquot objects from the database.

    Parameters:
    n (int): The number of aliquots to return. If None, return all aliquots.

    Returns:
    List[Aliquot]: A list of Aliquot objects.
    """
    connection = get_connection()
    query = connection.execute(select(Aliquot).limit(n))
    aliquots = query.fetchall()
    return aliquots