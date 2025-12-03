import pandas as pd
import pytest
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from marc_db.models import Base, Assembly, Antimicrobial
from marc_db.ingest import ingest_from_tsvs


data_dir = Path(__file__).parent


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
    ingest_from_tsvs(
        isolates=pd.read_csv(data_dir / "test_multi_aliquot.tsv", sep="\t"),
        assemblies=pd.read_csv(data_dir / "test_assembly_data.tsv", sep="\t"),
        antimicrobials=pd.read_csv(data_dir / "test_amr_data.tsv", sep="\t"),
        yes=True,
        session=session,
    )
    return session


def test_amr_counts(ingest_data):
    assert ingest_data.query(Assembly).count() == 2
    assert ingest_data.query(Antimicrobial).count() == 8
