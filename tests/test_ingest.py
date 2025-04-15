import pytest
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.orm import sessionmaker
from marc_db.models import Base
from marc_db.ingest import ingest_tsv


@pytest.fixture(scope="module")
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture(scope="module")
def session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    yield session
    session.close()


def test_ingest_xlsx(session, tmpdir: Path):
    # Ingest the xlsx file
    df = ingest_tsv(Path(__file__).parent / "test_anonymized.tsv", session)
