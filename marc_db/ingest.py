import pandas as pd
from marc_db.db import get_session
from marc_db.models import Aliquot, Isolate
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

    # Expect one aliquot per row but only a single isolate per SampleID.
    # Check for duplicate isolates and ensure all isolate level fields match.
    isolate_cols = [
        "SampleID",
        "Subject ID",
        "Specimen ID",
        "sample_source",
        "sample species",
        "special_collection",
        "Received by mARC",
        "Cryobanking",
    ]
    duplicate_groups = df.duplicated(subset=["SampleID"], keep=False)
    if duplicate_groups.any():
        for sample_id, group in df[duplicate_groups].groupby("SampleID"):
            subset = group[isolate_cols].drop(columns=["SampleID"])
            if not (subset.nunique(dropna=False) <= 1).all():
                raise ValueError(
                    f"Inconsistent isolate information for sample_id {sample_id}"
                )
    # Reduce to a single row per isolate for isolate ingestion
    unique_isolates_df = (
        df[isolate_cols].drop_duplicates(subset=["SampleID"], keep="first").copy()
    )

    iso_before = session.query(Isolate).count()
    ali_before = session.query(Aliquot).count()

    # Prepare isolate dataframe from the unique isolate rows
    isolate_df = unique_isolates_df.copy()
    # Rename columns to match the database schema
    isolate_df.columns = [
        "sample_id",
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
    ).dt.date
    isolate_df["cryobanking_date"] = pd.to_datetime(
        isolate_df["cryobanking_date"], errors="coerce"
    ).dt.date
    # Insert into the database
    isolate_df.to_sql("isolates", con=session.bind, if_exists="append", index=False)
    session.commit()
    iso_after = session.query(Isolate).count()
    added_isolates = iso_after - iso_before
    failed_isolates = len(isolate_df) - added_isolates

    # Extract aliquot_df
    aliquot_df = df[["Tube Barcode", "Box-name_position", "SampleID"]].copy()
    # Rename columns to match the database schema
    aliquot_df.columns = ["tube_barcode", "box_name", "sample_id"]
    # Associate aliquots with isolates using sample_id
    aliquot_df["isolate_id"] = aliquot_df["sample_id"]
    aliquot_df.drop(columns=["sample_id"], inplace=True)
    aliquot_df.to_sql("aliquots", con=session.bind, if_exists="append", index=False)
    session.commit()
    ali_after = session.query(Aliquot).count()
    added_aliquots = ali_after - ali_before
    failed_aliquots = len(aliquot_df) - added_aliquots

    print(
        f"Isolates added: {added_isolates} success, {failed_isolates} failed; "
        f"Aliquots added: {added_aliquots} success, {failed_aliquots} failed"
    )

    return df
