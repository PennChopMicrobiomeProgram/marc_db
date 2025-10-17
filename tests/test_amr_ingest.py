import pytest
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from marc_db.models import Base, Assembly, Antimicrobial
from marc_db.ingest import ingest_tsv, ingest_antimicrobial_tsv


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
def ingest_data(session):
    ingest_tsv(Path(__file__).parent / "test_multi_aliquot.tsv", session)
    df = ingest_antimicrobial_tsv(
        Path(__file__).parent / "test_amr_data.tsv",
        run_number="1",
        session=session,
    )
    return df, session


def test_amr_counts(ingest_data):
    _, session = ingest_data
    assert session.query(Assembly).count() == 2
    assert session.query(Antimicrobial).count() == 8
