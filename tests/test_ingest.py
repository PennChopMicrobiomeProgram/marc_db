import pytest
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from marc_db.models import Base
from marc_db.ingest import ingest_tsv
from marc_db.views import get_isolates, get_aliquots


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


@pytest.fixture(scope="module")
def ingest(session):
    # Ingest the tsv file without error
    df = ingest_tsv(Path(__file__).parent / "test_anonymized.tsv", session)
    return df, session


def test_ingest_xlsx(ingest):
    df, _ = ingest
    assert not df.empty


def test_views(ingest):
    df, session = ingest

    assert len(get_isolates(session)) > 0
    assert len(get_aliquots(session)) > 0

    assert len(get_isolates(session, sample_id="marc.bacteremia.1")) == 1
    assert len(get_aliquots(session, id=1)) == 1
