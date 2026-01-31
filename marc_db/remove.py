from typing import Callable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from marc_db.db import get_session
from marc_db.models import (
    Aliquot,
    Antimicrobial,
    Assembly,
    AssemblyQC,
    Contaminant,
    Isolate,
    TaxonomicAssignment,
)


def _summarize_isolate(session: Session, sample_id: str) -> dict:
    assembly_ids = select(Assembly.id).where(Assembly.isolate_id == sample_id)
    return {
        "aliquots": session.query(Aliquot)
        .filter(Aliquot.isolate_id == sample_id)
        .count(),
        "assemblies": session.query(Assembly)
        .filter(Assembly.isolate_id == sample_id)
        .count(),
        "assembly_qc": session.query(AssemblyQC)
        .filter(AssemblyQC.assembly_id.in_(assembly_ids))
        .count(),
        "taxonomic_assignments": session.query(TaxonomicAssignment)
        .filter(TaxonomicAssignment.assembly_id.in_(assembly_ids))
        .count(),
        "contaminants": session.query(Contaminant)
        .filter(Contaminant.assembly_id.in_(assembly_ids))
        .count(),
        "antimicrobials": session.query(Antimicrobial)
        .filter(Antimicrobial.assembly_id.in_(assembly_ids))
        .count(),
    }


def remove_isolate(
    *,
    sample_id: str,
    yes: bool = False,
    session: Optional[Session] = None,
    input_fn: Callable[[str], str] = input,
):
    """Remove a single isolate and its associated records."""

    created_session = False
    if session is None:
        session = get_session()
        created_session = True

    trans = session.begin_nested() if session.in_transaction() else session.begin()
    try:
        isolate = session.get(Isolate, sample_id)
        if isolate is None:
            print(f"No isolate found with SampleID {sample_id}.")
            trans.rollback()
            return

        counts = _summarize_isolate(session, sample_id)

        if not yes:
            print(f"Isolate {sample_id} will be removed with the following records:")
            for label, count in counts.items():
                print(f"  {label.replace('_', ' ')}: {count}")
            answer = input_fn("Proceed with deletion? [y/N]: ").strip().lower()
            if answer not in {"y", "yes"}:
                trans.rollback()
                print("Removal cancelled.")
                return

        assembly_ids = select(Assembly.id).where(Assembly.isolate_id == sample_id)

        session.query(Antimicrobial).filter(
            Antimicrobial.assembly_id.in_(assembly_ids)
        ).delete(synchronize_session=False)
        session.query(Contaminant).filter(
            Contaminant.assembly_id.in_(assembly_ids)
        ).delete(synchronize_session=False)
        session.query(TaxonomicAssignment).filter(
            TaxonomicAssignment.assembly_id.in_(assembly_ids)
        ).delete(synchronize_session=False)
        session.query(AssemblyQC).filter(
            AssemblyQC.assembly_id.in_(assembly_ids)
        ).delete(synchronize_session=False)
        session.query(Assembly).filter(
            Assembly.isolate_id == sample_id
        ).delete(synchronize_session=False)
        session.query(Aliquot).filter(Aliquot.isolate_id == sample_id).delete(
            synchronize_session=False
        )
        session.query(Isolate).filter(Isolate.sample_id == sample_id).delete(
            synchronize_session=False
        )

        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        if created_session:
            session.close()
