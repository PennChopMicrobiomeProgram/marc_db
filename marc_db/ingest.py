import pandas as pd
from marc_db.db import get_session
from marc_db.models import Isolate
from sqlalchemy.orm import Session


def ingest_tsv(file_path: str, session: Session = None) -> pd.DataFrame:
    """
    Import a tsv file to pandas DataFrame and load it into the database.

    Parameters:
    file_path (str): The path to the xlsx file.
    connection (Connection): The connection to the database.

    Returns:
    pd.DataFrame: The imported data as a pandas DataFrame.
    """
    if not session:
        # Define this here instead of as a default argument in order to avoid loading it ahead of time
        session = get_session()

    df = pd.read_csv(file_path, delimiter="\t")

    # Extract isolate_df
    isolate_df = df[
        [
            "Subject ID",
            "Specimen ID",
            "sample_source",
            "sample species",
            "special_collection",
            "Received by mARC",
            "Cryobanking",
        ]
    ].copy()
    # Rename columns to match the database schema
    isolate_df.columns = [
        "subject_id",
        "specimen_id",
        "source",
        "suspected_organism",
        "special_collection",
        "received_date",
        "cryobanking_date",
    ]
    # Convert date columns to datetime
    isolate_df["received_date"] = pd.to_datetime(
        isolate_df["received_date"], errors="coerce"
    )
    isolate_df["cryobanking_date"] = pd.to_datetime(
        isolate_df["cryobanking_date"], errors="coerce"
    )
    # Drop rows with NaN values in the date columns
    isolate_df.dropna(subset=["received_date", "cryobanking_date"], inplace=True)
    # Drop duplicates based on subject_id and specimen_id
    isolate_df.drop_duplicates(subset=["subject_id", "specimen_id"], inplace=True)
    # Insert into the database
    isolate_df.to_sql("isolates", con=session.bind, if_exists="append", index=False)
    session.commit()

    # Extract aliquot_df
    aliquot_df = df[
        ["Tube Barcode", "Box-name_position", "Subject ID", "Specimen ID"]
    ].copy()
    # Rename columns to match the database schema
    aliquot_df.columns = ["tube_barcode", "box_name", "subject_id", "specimen_id"]
    # Query the database to get the isolate_id for each row
    aliquot_df["isolate_id"] = aliquot_df.apply(
        lambda row: session.query(Isolate.id)
        .filter_by(subject_id=row["subject_id"], specimen_id=row["specimen_id"])
        .scalar(),
        axis=1,
    )
    # Insert into the database
    aliquot_df.drop(columns=["subject_id", "specimen_id"], inplace=True)
    aliquot_df.to_sql("aliquots", con=session.bind, if_exists="append", index=False)
    session.commit()

    return df
