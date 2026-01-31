import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from marc_db.ingest import ingest_from_tsvs
from marc_db.models import (
    Aliquot,
    Antimicrobial,
    Assembly,
    AssemblyQC,
    Base,
    Isolate,
    TaxonomicAssignment,
)
from marc_db.remove import remove_isolate


data_dir = Path(__file__).parent


def _build_session():
    engine = create_engine("sqlite:///:memory:")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    return session, engine


def test_remove_isolate_deletes_associations():
    session, engine = _build_session()
    isolates_df = pd.read_csv(data_dir / "test_multi_aliquot.tsv", sep="\t")
    assemblies_df = pd.read_csv(data_dir / "test_assembly_data.tsv", sep="\t")
    tax_df = pd.read_csv(data_dir / "test_taxonomic_assignment.tsv", sep="\t")
    amr_df = pd.read_csv(data_dir / "test_amr_data.tsv", sep="\t")

    ingest_from_tsvs(
        isolates=isolates_df,
        assemblies=assemblies_df,
        assembly_qcs=assemblies_df,
        taxonomic_assignments=tax_df,
        antimicrobials=amr_df,
        yes=True,
        session=session,
    )

    remove_isolate(sample_id="sample1", yes=True, session=session)

    remaining_sample = "sample2"
    expected_aliquots = isolates_df.loc[
        isolates_df["SampleID"] == remaining_sample
    ].shape[0]
    expected_assemblies = assemblies_df.loc[
        assemblies_df["SampleID"] == remaining_sample
    ].shape[0]
    expected_taxonomic = tax_df.loc[tax_df["SampleID"] == remaining_sample].shape[0]
    expected_amr = amr_df.loc[amr_df["SampleID"] == remaining_sample].shape[0]

    assert session.query(Isolate).count() == 1
    assert session.query(Aliquot).count() == expected_aliquots
    assert session.query(Assembly).count() == expected_assemblies

    remaining_assembly_ids = [
        asm.id
        for asm in session.query(Assembly).filter(
            Assembly.isolate_id == remaining_sample
        )
    ]
    assert session.query(AssemblyQC).filter(
        AssemblyQC.assembly_id.in_(remaining_assembly_ids)
    ).count() == expected_assemblies
    assert session.query(TaxonomicAssignment).filter(
        TaxonomicAssignment.assembly_id.in_(remaining_assembly_ids)
    ).count() == expected_taxonomic
    assert session.query(Antimicrobial).filter(
        Antimicrobial.assembly_id.in_(remaining_assembly_ids)
    ).count() == expected_amr

    session.close()
    engine.dispose()


def test_remove_isolate_cancelled_does_not_delete(capsys):
    session, engine = _build_session()
    isolates_df = pd.read_csv(data_dir / "test_multi_aliquot.tsv", sep="\t")

    ingest_from_tsvs(isolates=isolates_df, yes=True, session=session)

    remove_isolate(
        sample_id="sample1",
        yes=False,
        session=session,
        input_fn=lambda _: "n",
    )

    captured = capsys.readouterr()
    assert "Removal cancelled." in captured.out
    assert session.query(Isolate).count() == 2

    session.close()
    engine.dispose()
