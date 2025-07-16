from marc_db.db import get_session
from marc_db.models import Aliquot, Isolate
from sqlalchemy.orm import Session


def get_isolates(
    session: Session | None = None, sample_id: str | None = None, n: int | None = None
) -> list[Isolate]:
    if session is None:
        session = get_session()
    """
    Get a list of Isolate objects from the database.

    Parameters:
    n (int): The number of isolates to return. If None, return all isolates.

    Returns:
    List[Isolate]: A list of Isolate objects.
    """
    if sample_id:
        return [session.query(Isolate).filter(Isolate.sample_id == sample_id).first()]
    return session.query(Isolate).limit(n).all()


def get_aliquots(
    session: Session | None = None, id: int | None = None, n: int | None = None
) -> list[Aliquot]:
    if session is None:
        session = get_session()
    """
    Get a list of Aliquot objects from the database.

    Parameters:
    n (int): The number of aliquots to return. If None, return all aliquots.

    Returns:
    List[Aliquot]: A list of Aliquot objects.
    """
    if id:
        return [session.query(Aliquot).filter(Aliquot.id == id).first()]
    return session.query(Aliquot).limit(n).all()
