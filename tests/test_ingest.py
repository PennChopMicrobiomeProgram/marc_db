import pytest
import pandas as pd
from marc_db.ingest import import_xlsx_to_dataframe


def test_import_xlsx_to_dataframe():
    # Create a sample xlsx file
    sample_data = {"Column1": [1, 2, 3], "Column2": ["A", "B", "C"]}
    df = pd.DataFrame(sample_data)
    file_path = "sample_data.xlsx"
    df.to_excel(file_path, index=False)

    # Test the import_xlsx_to_dataframe function
    imported_df = import_xlsx_to_dataframe(file_path)
    assert imported_df.equals(df)
