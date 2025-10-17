import pytest
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from marc_db.models import (
    Base,
    Isolate,
    Assembly,
    AssemblyQC,
    TaxonomicAssignment,
    Antimicrobial,
)
from marc_db.ingest import ingest_tsv, ingest_assembly_tsv


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
    df = ingest_assembly_tsv(
        Path(__file__).parent / "test_assembly_data.tsv",
        run_number="1",
        sunbeam_version="v1",
        sbx_sga_version="v1",
        config_file="cfg",
        sunbeam_output_path="/sb",
        session=session,
    )
    return df, session


def test_counts(ingest_data):
    _, session = ingest_data
    assert session.query(Assembly).count() == 2
    assert session.query(AssemblyQC).count() == 2
    assert session.query(TaxonomicAssignment).count() == 2
    assert session.query(Antimicrobial).count() == 2


def test_mash_fields(ingest_data):
    _, session = ingest_data
    tax = (
        session.query(TaxonomicAssignment)
        .order_by(TaxonomicAssignment.assembly_id)
        .all()
    )
    assert tax[0].mash_contamination == 0.05
    assert tax[0].mash_contaminated_spp == "Bacillus"
    assert tax[1].mash_contamination == 0.1
    assert tax[1].mash_contaminated_spp == "Listeria"
