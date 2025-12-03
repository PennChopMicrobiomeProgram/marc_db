import pandas as pd
import pytest
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from marc_db.ingest import ingest_from_tsvs
from marc_db.models import Base, Aliquot, Isolate
from marc_db.views import get_isolates, get_aliquots


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
def ingest(session):
    isolates_df = pd.read_csv(data_dir / "test_multi_aliquot.tsv", sep="\t")
    ingest_from_tsvs(isolates=isolates_df, yes=True, session=session)
    return session


def test_ingest_isolates(ingest):
    assert len(get_isolates(ingest)) == 2


def test_views(ingest):
    assert len(get_isolates(ingest, sample_id="sample1")) == 1
    assert len(get_aliquots(ingest, id=1)) == 1


def test_conflicting_duplicate_rows():
    engine = create_engine("sqlite:///:memory:")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    isolates_df = pd.read_csv(data_dir / "test_bad_duplicates.tsv", sep="\t")
    ingest_from_tsvs(isolates=isolates_df, yes=True, session=session)

    isolate = session.query(Isolate).filter_by(sample_id="sample1").one()
    assert isolate.subject_id == 1
    assert session.query(Aliquot).count() == 2

    session.close()
    engine.dispose()
