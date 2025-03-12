import pandas as pd
from marc_db.db import get_session
from sqlalchemy.orm import Session


def ingest_xlsx(file_path: str, session: Session) -> pd.DataFrame:
    """
    Import an xlsx file to pandas DataFrame and load it into the database.

    Parameters:
    file_path (str): The path to the xlsx file.
    connection (Connection): The connection to the database.

    Returns:
    pd.DataFrame: The imported data as a pandas DataFrame.
    """
    if not session:
        # Define this here instead of as a default argument in order to avoid loading it ahead of time
        session = get_session()

    df = pd.read_excel(file_path)
    df.to_sql("data", con=session.bind, if_exists="replace", index=False)
    session.commit()
    return df
