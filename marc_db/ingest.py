import pandas as pd
from marc_db.db import get_connection
from sqlalchemy.engine import Connection


def ingest_xlsx(file_path: str, connection: Connection) -> pd.DataFrame:
    """
    Import an xlsx file to pandas DataFrame and load it into the database.

    Parameters:
    file_path (str): The path to the xlsx file.
    connection (Connection): The connection to the database.

    Returns:
    pd.DataFrame: The imported data as a pandas DataFrame.
    """
    if not connection:
        # Define this here instead of as a default argument in order to avoid loading it ahead of time
        connection = get_connection()

    df = pd.read_excel(file_path)
    df.to_sql("data", con=connection, if_exists="replace", index=False)
    return df