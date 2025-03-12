import pandas as pd


def import_xlsx_to_dataframe(file_path):
    """
    Import an xlsx file and return a pandas DataFrame.

    Parameters:
    file_path (str): The path to the xlsx file.

    Returns:
    pd.DataFrame: The imported data as a pandas DataFrame.
    """
    return pd.read_excel(file_path)
