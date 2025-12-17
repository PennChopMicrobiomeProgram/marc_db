from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Union

import pandas as pd
from sqlalchemy.orm import Session

from marc_db.db import get_session
from marc_db.models import (
    Aliquot,
    Isolate,
    Assembly,
    AssemblyQC,
    TaxonomicAssignment,
    Antimicrobial,
    Contaminant,
)


def _as_python(value):
    """Return a plain Python value suitable for SQL insertion."""
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else value


def _format_large_list(items: Iterable[str], limit: int = 10) -> str:
    items = list(items)
    if len(items) <= limit:
        return ", ".join(items)
    else:
        displayed = ", ".join(items[:limit])
        return f"{displayed}, and {len(items) - limit} more..."


def _ensure_required_columns(df: pd.DataFrame, required: Iterable[str]):
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {', '.join(missing)}")


def _load_dataframe(data: Optional[Union[pd.DataFrame, Path, str]]) -> Optional[pd.DataFrame]:
    if data is None or isinstance(data, pd.DataFrame):
        return data
    return pd.read_csv(Path(data), sep="\t")


def _ingest_isolates(df: pd.DataFrame, session: Session):
    isolate_cols = [
        "SampleID",
        "Subject ID",
        "Specimen ID",
        "sample species",
        "special_collection",
        "Received by mARC",
        "Cryobanking",
    ]
    _ensure_required_columns(df, isolate_cols + ["Tube Barcode", "Box-name_position"])

    isolates = df[isolate_cols].copy()
    isolates.columns = [
        "sample_id",
        "subject_id",
        "specimen_id",
        "suspected_organism",
        "special_collection",
        "received_date",
        "cryobanking_date",
    ]
    isolates["received_date"] = pd.to_datetime(
        isolates["received_date"], errors="coerce"
    ).dt.date
    isolates["cryobanking_date"] = pd.to_datetime(
        isolates["cryobanking_date"], errors="coerce"
    ).dt.date

    added = {}
    for _, row in isolates.iterrows():
        isolate_kwargs = row.to_dict()
        sample_id = isolate_kwargs["sample_id"]

        existing = added.get(sample_id)
        if existing:
            if existing != isolate_kwargs:
                print(f"Conflicting isolate data for SampleID {sample_id}")
            continue

        isolate = Isolate(**isolate_kwargs)
        session.add(isolate)
        added[sample_id] = isolate_kwargs

    aliquot_df = df[["Tube Barcode", "Box-name_position", "SampleID"]].copy()
    aliquot_df.columns = ["tube_barcode", "box_name", "isolate_id"]
    for _, row in aliquot_df.iterrows():
        session.add(Aliquot(**row.to_dict()))


def _ingest_assemblies(
    df: pd.DataFrame,
    session: Session,
) -> Dict[str, Assembly]:
    lookup: Dict[str, Assembly] = {}

    for row in df.itertuples():
        asm = Assembly(
            isolate_id=row.SampleID,
            metagenomic_sample_id=getattr(row, "metagenomic_sample_id", None),
            metagenomic_run_id=getattr(row, "metagenomic_run_id", None),
            nanopore_path=getattr(row, "nanopore_path", None),
            run_number=getattr(row, "run_number", None),
            sunbeam_version=getattr(row, "sunbeam_version", None),
            sbx_sga_version=getattr(row, "sbx_sga_version", None),
            sunbeam_output_path=getattr(row, "sunbeam_output_path", None),
            ncbi_id=getattr(row, "ncbi_id", None),
        )
        session.add(asm)
        lookup[str(row.SampleID)] = asm

    return lookup


def _ingest_qc_records(
    df: pd.DataFrame,
    session: Session,
    assembly_lookup: Dict[str, Assembly],
):
    for _, row in df.iterrows():
        asm = assembly_lookup.get(str(row.get("SampleID")))
        session.add(
            AssemblyQC(
                assembly_id=asm.id if asm else None,
                contig_count=_as_python(row.get("contig_count")),
                genome_size=_as_python(row.get("genome_size")),
                n50=_as_python(row.get("n50")),
                gc_content=_as_python(row.get("gc_content")),
                cds=_as_python(row.get("cds")),
                completeness=_as_python(row.get("completeness")),
                contamination=_as_python(row.get("contamination")),
                min_contig_coverage=_as_python(row.get("min_contig_coverage")),
                avg_contig_coverage=_as_python(row.get("avg_contig_coverage")),
                max_contig_coverage=_as_python(row.get("max_contig_coverage")),
            )
        )


def _ingest_taxonomic_assignments(
    df: pd.DataFrame,
    session: Session,
    assembly_lookup: Dict[str, Assembly],
):
    for _, row in df.iterrows():
        asm = assembly_lookup.get(str(row.get("SampleID")))
        session.add(
            TaxonomicAssignment(
                assembly_id=asm.id if asm else None,
                tool=_as_python(row.get("tool")),
                classification=_as_python(row.get("classification")),
                comment=_as_python(row.get("comment")),
            )
        )


def _ingest_contaminants(
    df: pd.DataFrame,
    session: Session,
    assembly_lookup: Dict[str, Assembly],
):
    for _, row in df.iterrows():
        asm = assembly_lookup.get(str(row.get("SampleID")))
        session.add(
            Contaminant(
                assembly_id=asm.id if asm else None,
                tool=_as_python(row.get("tool")),
                contaminant=_as_python(row.get("contaminant")),
                proportion=_as_python(row.get("proportion")),
            )
        )


def _ingest_amr_records(
    df: pd.DataFrame,
    session: Session,
    assembly_lookup: Dict[str, Assembly],
):
    for _, row in df.iterrows():
        asm = assembly_lookup.get(str(row.get("SampleID")))
        session.add(
            Antimicrobial(
                assembly_id=asm.id if asm else None,
                contig_id=_as_python(row.get("contig_id")),
                gene_symbol=_as_python(row.get("gene_symbol")),
                gene_name=_as_python(row.get("gene_name")),
                accession=_as_python(row.get("accession")),
            )
        )


def ingest_from_tsvs(
    *,
    isolates: Optional[pd.DataFrame] = None,
    assemblies: Optional[pd.DataFrame] = None,
    assembly_qcs: Optional[pd.DataFrame] = None,
    taxonomic_assignments: Optional[pd.DataFrame] = None,
    contaminants: Optional[pd.DataFrame] = None,
    antimicrobials: Optional[pd.DataFrame] = None,
    yes: bool = False,
    session: Optional[Session] = None,
    input_fn: Callable[[str], str] = input,
):
    """Ingest a collection of TSV files with optional confirmation.

    When ``yes`` is False, a summary of pending changes (including duplicates)
    is printed and the user is prompted for confirmation before committing.
    """

    created_session = False
    isolates = _load_dataframe(isolates)
    assemblies = _load_dataframe(assemblies)
    assembly_qcs = _load_dataframe(assembly_qcs)
    taxonomic_assignments = _load_dataframe(taxonomic_assignments)
    contaminants = _load_dataframe(contaminants)
    antimicrobials = _load_dataframe(antimicrobials)
    if session is None:
        session = get_session()
        created_session = True

    trans = session.begin_nested() if session.in_transaction() else session.begin()
    try:
        assembly_lookup: Dict[str, Assembly] = {}

        if isolates is not None:
            _ingest_isolates(isolates, session)
        if assemblies is not None:
            assembly_lookup = _ingest_assemblies(
                assemblies,
                session=session,
            )
            session.flush()
        if assembly_qcs is not None:
            _ingest_qc_records(
                assembly_qcs,
                session=session,
                assembly_lookup=assembly_lookup,
            )
        if taxonomic_assignments is not None:
            _ingest_taxonomic_assignments(
                taxonomic_assignments,
                session=session,
                assembly_lookup=assembly_lookup,
            )
        if contaminants is not None:
            _ingest_contaminants(
                contaminants,
                session=session,
                assembly_lookup=assembly_lookup,
            )
        if antimicrobials is not None:
            _ingest_amr_records(
                antimicrobials,
                session=session,
                assembly_lookup=assembly_lookup,
            )

        # If there are any incompatibilities or constraint violations, they
        # will be raised here before we ask for confirmation.
        session.flush()

        proceed = yes
        if not yes:
            answer = input_fn("Proceed with these changes? [y/N]: ").strip().lower()
            proceed = answer in {"y", "yes"}
        if proceed:
            trans.commit()
        else:
            trans.rollback()
            print("Ingest cancelled.")
    except Exception:
        trans.rollback()
        raise
    finally:
        if created_session:
            session.close()
