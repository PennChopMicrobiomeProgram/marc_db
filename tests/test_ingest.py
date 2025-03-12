import pytest
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from marc_db.models import Base
from marc_db.ingest import ingest_xlsx


@pytest.fixture(scope="module")
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture(scope="module")
def connection(engine):
    connection = engine.connect()
    Base.metadata.create_all(engine)
    return connection


@pytest.fixture
def sample_data() -> pd.DataFrame:
    return pd.DataFrame({"SampleId": [1, 2, 3], "IsolateId": ["A", "B", "C"], "AliquotId": [1, 2, 3], "SubjectId": ["A", "B", "C"], "SpecimenId": [1, 2, 3], "Source": ["A", "B", "C"], "SuspectedOrganism": ["A", "B", "C"], "SpecialCollection": ["A", "B", "C"], "ReceivedDate": ["A", "B", "C"], "CryobankingDate": ["A", "B", "C"], "TubeBarcode": ["A", "B", "C"], "BoxName": ["A", "B", "C"]})


def test_ingest_xlsx(sample_data: pd.DataFrame, connection: Connection, tmpdir: Path):
    # Create a sample xlsx file
    fp = tmpdir / "sample_data.xlsx"
    sample_data.to_excel(fp, index=False)

    print(connection)
    # Ingest the xlsx file
    df = ingest_xlsx(fp, connection)
