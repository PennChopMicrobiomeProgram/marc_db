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
from marc_db.ingest import ingest_from_tsvs, ingest_tsv


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
    data_dir = Path(__file__).parent
    ingest_tsv(str(data_dir / "test_multi_aliquot.tsv"), session)
    df = ingest_from_tsvs(
        assemblies=str(data_dir / "test_assembly_data.tsv"),
        assembly_qcs=str(data_dir / "test_assembly_data.tsv"),
        taxonomic_assignments=str(data_dir / "test_taxonomic_assignment.tsv"),
        antimicrobials=str(data_dir / "test_amr_data.tsv"),
        run_number="1",
        sunbeam_version="v1",
        sbx_sga_version="v1",
        config_file="cfg",
        sunbeam_output_path="/sb",
        yes=True,
        session=session,
    )
    return df, session


def test_counts(ingest_data):
    _, session = ingest_data
    assert session.query(Assembly).count() == 2
    assert session.query(AssemblyQC).count() == 2
    assert session.query(TaxonomicAssignment).count() == 2
    assert session.query(Antimicrobial).count() == 8


def test_taxonomic_assignment_fields(ingest_data):
    _, session = ingest_data
    tax = (
        session.query(TaxonomicAssignment)
        .order_by(TaxonomicAssignment.assembly_id)
        .all()
    )

    assert tax[0].classification == "Escherichia coli"
    assert tax[0].comment == "ST1 schema1"
    assert tax[0].tool == "mlst"
    assert tax[1].classification == "Klebsiella pneumoniae"
    assert tax[1].comment == "ST2 schema2"
    assert tax[1].tool == "mlst"
