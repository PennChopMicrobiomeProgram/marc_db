from marc_db.db import get_session
from marc_db.models import Aliquot, Isolate
from sqlalchemy.orm import Session


def get_isolates(session: Session = get_session(), n: int = None) -> list[Isolate]:
    """
    Get a list of Isolate objects from the database.

    Parameters:
    n (int): The number of isolates to return. If None, return all isolates.

    Returns:
    List[Isolate]: A list of Isolate objects.
    """
    return session.query(Isolate).limit(n).all()


def get_aliquots(session: Session = get_session(), n: int = None) -> list[Aliquot]:
    """
    Get a list of Aliquot objects from the database.

    Parameters:
    n (int): The number of aliquots to return. If None, return all aliquots.

    Returns:
    List[Aliquot]: A list of Aliquot objects.
    """
    return session.query(Aliquot).limit(n).all()
