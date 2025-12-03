import pandas as pd
import pytest
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from marc_db.models import (
    Base,
    Assembly,
    AssemblyQC,
    TaxonomicAssignment,
    Antimicrobial,
)
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
        assembly_qcs=pd.read_csv(data_dir / "test_assembly_data.tsv", sep="\t"),
        taxonomic_assignments=pd.read_csv(
            data_dir / "test_taxonomic_assignment.tsv", sep="\t"
        ),
        antimicrobials=pd.read_csv(data_dir / "test_amr_data.tsv", sep="\t"),
        yes=True,
        session=session,
    )
    return session


def test_counts(ingest_data):
    assert ingest_data.query(Assembly).count() == 2
    assert ingest_data.query(AssemblyQC).count() == 2
    assert ingest_data.query(TaxonomicAssignment).count() == 2
    assert ingest_data.query(Antimicrobial).count() == 8


def test_taxonomic_assignment_fields(ingest_data):
    tax = (
        ingest_data.query(TaxonomicAssignment)
        .order_by(TaxonomicAssignment.assembly_id)
        .all()
    )

    assert tax[0].classification == "Escherichia coli"
    assert tax[0].comment == "ST1 schema1"
    assert tax[0].tool == "mlst"
    assert tax[1].classification == "Klebsiella pneumoniae"
    assert tax[1].comment == "ST2 schema2"
    assert tax[1].tool == "mlst"
