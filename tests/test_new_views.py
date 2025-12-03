import pytest
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
from marc_db.views import (
    get_assemblies,
    get_assembly_qc,
    get_taxonomic_assignments,
    get_antimicrobials,
)


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
def setup_data(session):
    iso = Isolate(sample_id="iso1", subject_id=1, specimen_id=1)
    session.add(iso)
    session.commit()

    assembly = Assembly(isolate_id=iso.sample_id, metagenomic_sample_id="ms1")
    session.add(assembly)
    session.commit()

    qc = AssemblyQC(assembly_id=assembly.id, contig_count=10)
    tax = TaxonomicAssignment(assembly_id=assembly.id, classification="k__Bacteria")
    amr = Antimicrobial(assembly_id=assembly.id, gene_symbol="blaCTX")
    session.add_all([qc, tax, amr])
    session.commit()
    return iso, assembly, qc, tax, amr


def test_new_views(session, setup_data):
    iso, assembly, qc, tax, amr = setup_data

    assert get_assemblies(session)[0].id == assembly.id

    qc_res, iso_id = get_assembly_qc(session)[0]
    assert qc_res.contig_count == qc.contig_count
    assert iso_id == iso.sample_id

    tax_res, iso_id = get_taxonomic_assignments(session)[0]
    assert tax_res.classification == tax.classification
    assert iso_id == iso.sample_id

    amr_res, iso_id = get_antimicrobials(session)[0]
    assert amr_res.gene_symbol == amr.gene_symbol
    assert iso_id == iso.sample_id
