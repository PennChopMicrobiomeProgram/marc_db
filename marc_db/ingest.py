from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple, Callable

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


def _check_orphan_assemblies(session: Session, isolates_df: Optional[pd.DataFrame], assemblies_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if assemblies_df is None:
        return
    existing_sample_ids = set(
        iso.sample_id for iso in session.query(Isolate).all()
    )
    if isolates_df is not None:
        existing_sample_ids.update(isolates_df["SampleID"].tolist())
    
    orphans = [
        row["SampleID"]
        for _, row in assemblies_df.iterrows()
        if row["SampleID"] not in existing_sample_ids
    ]

    if orphans:
        print(
            f"Orphan assemblies found with no matching isolate: {_format_large_list(orphans)}"
        )
    return assemblies_df[~assemblies_df["SampleID"].isin(orphans)]
    

def _check_orphan_info(session: Session, assemblies: Optional[pd.DataFrame], assembly_qcs: Optional[pd.DataFrame], taxonomic_assignments: Optional[pd.DataFrame], contaminants: Optional[pd.DataFrame], antimicrobials: Optional[pd.DataFrame]):
    if all(df is None for df in [assembly_qcs, taxonomic_assignments, contaminants, antimicrobials]):
        return
    if assemblies is None:
        return
    existing_sample_ids = set(row["SampleID"] for _, row in assemblies.iterrows())
    for df in [assembly_qcs, taxonomic_assignments, contaminants, antimicrobials]:
        if df is None:
            continue
        
        orphans = [
            row["SampleID"]
            for _, row in df.iterrows()
            if row["SampleID"] not in existing_sample_ids
        ]

        if orphans:
            raise ValueError(
                f"Orphan records found with no matching isolate: {_format_large_list(orphans)}"
            )


def _format_duplicates(section: str, duplicates: Iterable[str]) -> str:
    deduped = sorted(set(duplicates))
    if not deduped:
        return ""
    header = f"  {section}: {len(deduped)} duplicate(s)"
    listed = "\n".join(f"    - {item}" for item in deduped)
    return f"{header}\n{listed}"


def _summarize(report: Dict[str, Dict[str, object]]) -> str:
    lines = ["Planned ingestion summary:"]
    for section, data in report.items():
        added = data.get("added", 0)
        lines.append(f"- {section}: {added} new record(s)")
        for extra_key in [k for k in data.keys() if k not in {"added", "duplicates"}]:
            lines.append(f"  {section} {extra_key}: {data[extra_key]}")
        dupes = _format_duplicates(section, data.get("duplicates", []))
        if dupes:
            lines.append(dupes)
    return "\n".join(lines)


def _ensure_required_columns(df: pd.DataFrame, required: Iterable[str]):
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {', '.join(missing)}")


def _ingest_isolates(
    file_path: str, session: Session, report: Dict[str, Dict[str, object]]
):
    df = pd.read_csv(file_path, delimiter="\t")
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

    duplicate_groups = df.duplicated(subset=["SampleID"], keep=False)
    if duplicate_groups.any():
        for sample_id, group in df[duplicate_groups].groupby("SampleID"):
            subset = group[isolate_cols].drop(columns=["SampleID"])
            if not (subset.nunique(dropna=False) <= 1).all():
                raise ValueError(
                    f"Inconsistent isolate information for sample_id {sample_id}"
                )

    isolate_df = (
        df[isolate_cols].drop_duplicates(subset=["SampleID"], keep="first").copy()
    )
    isolate_df.columns = [
        "sample_id",
        "subject_id",
        "specimen_id",
        "suspected_organism",
        "special_collection",
        "received_date",
        "cryobanking_date",
    ]
    isolate_df["received_date"] = pd.to_datetime(
        isolate_df["received_date"], errors="coerce"
    ).dt.date
    isolate_df["cryobanking_date"] = pd.to_datetime(
        isolate_df["cryobanking_date"], errors="coerce"
    ).dt.date

    sample_ids = isolate_df["sample_id"].tolist()
    existing_isolates = {
        iso.sample_id
        for iso in session.query(Isolate).filter(Isolate.sample_id.in_(sample_ids))
    }
    duplicates = list(existing_isolates)
    added = 0
    for _, row in isolate_df.iterrows():
        if row.sample_id in existing_isolates:
            continue
        session.add(Isolate(**row.to_dict()))
        added += 1

    aliquot_df = df[["Tube Barcode", "Box-name_position", "SampleID"]].copy()
    aliquot_df.columns = ["tube_barcode", "box_name", "isolate_id"]
    existing_aliquots = {
        (a.isolate_id, a.tube_barcode, a.box_name)
        for a in session.query(Aliquot).filter(Aliquot.isolate_id.in_(sample_ids)).all()
    }
    seen_new: set[Tuple[str, str, str]] = set()
    aliquot_added = 0
    for _, row in aliquot_df.iterrows():
        key = (row.isolate_id, row.tube_barcode, row.box_name)
        if key in existing_aliquots or key in seen_new:
            duplicates.append(
                f"aliquot {row.isolate_id}:{row.tube_barcode}:{row.box_name}"
            )
            continue
        session.add(Aliquot(**row.to_dict()))
        aliquot_added += 1
        seen_new.add(key)

    report["isolates"] = {
        "added": added,
        "aliquots_added": aliquot_added,
        "duplicates": duplicates,
    }


def _ingest_assemblies(
    file_path: str,
    *,
    session: Session,
    report: Dict[str, Dict[str, object]],
) -> Dict[str, Assembly]:
    df = pd.read_csv(file_path, sep="\t")

    new = 0
    duplicates = [
        asm
        for asm in session.query(Assembly).filter(Assembly.isolate_id.in_(set(df["SampleID"].unique().tolist()))).all()
    ]
    orphans = [
        row["SampleID"]
        for _, row in df.iterrows()
        if not session.get(Isolate, row["SampleID"])
    ]
    lookup: Dict[str, Assembly] = {}

    for row in df.itertuples():
        if row.SampleID in orphans:
            continue
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
        new += 1
    session.flush()

    report["assemblies"] = {
        "added": new,
        "duplicates": [f"{d.isolate_id} (id={d.id})" for d in duplicates],
        "orphans": orphans,
    }
    return lookup


def _ingest_qc_records(
    file_path: str,
    *,
    session: Session,
    report: Dict[str, Dict[str, object]],
    assembly_lookup: Dict[str, Assembly],
):
    df = _rename_columns(pd.read_csv(file_path, sep="\t"))
    sample_col = "SampleID" if "SampleID" in df.columns else None
    added = 0
    duplicates = []
    for _, row in df.iterrows():
        assembly_id = _lookup_assembly_id(
            row, assembly_lookup, session, sample_col, run_number
        )
        if assembly_id is None:
            duplicates.append("unmatched assembly for QC")
            continue
        if session.get(AssemblyQC, assembly_id):
            duplicates.append(f"assembly_qc {assembly_id}")
            continue
        session.add(
            AssemblyQC(
                assembly_id=assembly_id,
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
        added += 1
    report["assembly_qcs"] = {"added": added, "duplicates": duplicates}


def _ingest_taxonomic_assignments(
    file_path: str,
    *,
    session: Session,
    report: Dict[str, Dict[str, object]],
    assembly_lookup: Dict[str, Assembly],
    run_number: Optional[str] = None,
):
    df = _rename_columns(pd.read_csv(file_path, sep="\t"))
    sample_col = "SampleID" if "SampleID" in df.columns else None
    tax_cols = [
        "taxonomic_classification",
        "taxonomic_abundance",
        "mash_contamination",
        "mash_contaminated_spp",
        "st",
        "st_schema",
        "allele_assignment",
        "tool",
        "classification",
        "comment",
    ]
    added = 0
    duplicates = []
    seen = set()
    for _, row in df.iterrows():
        assembly_id = _lookup_assembly_id(
            row, assembly_lookup, session, sample_col, run_number
        )
        if assembly_id is None:
            duplicates.append("unmatched assembly for taxonomic assignment")
            continue
        values = tuple(_as_python(row.get(col)) for col in tax_cols)
        key = (assembly_id, values)
        if key in seen:
            duplicates.append(f"taxonomic_assignment {assembly_id}")
            continue
        existing = (
            session.query(TaxonomicAssignment)
            .filter(TaxonomicAssignment.assembly_id == assembly_id)
            .filter(TaxonomicAssignment.taxonomic_classification == values[0])
            .first()
        )
        if existing:
            duplicates.append(f"taxonomic_assignment {assembly_id}")
            continue
        seen.add(key)
        session.add(
            TaxonomicAssignment(
                assembly_id=assembly_id,
                taxonomic_classification=values[0],
                taxonomic_abundance=values[1],
                mash_contamination=values[2],
                mash_contaminated_spp=values[3],
                st=values[4],
                st_schema=values[5],
                allele_assignment=values[6],
                tool=values[7],
                classification=values[8],
                comment=values[9],
            )
        )
        added += 1
    report["taxonomic_assignments"] = {"added": added, "duplicates": duplicates}


def _ingest_contaminants(
    file_path: str,
    *,
    session: Session,
    report: Dict[str, Dict[str, object]],
    assembly_lookup: Dict[str, Assembly],
    run_number: Optional[str] = None,
):
    df = _rename_columns(pd.read_csv(file_path, sep="\t"))
    sample_col = "SampleID" if "SampleID" in df.columns else None
    added = 0
    duplicates = []
    seen = set()
    for _, row in df.iterrows():
        assembly_id = _lookup_assembly_id(
            row, assembly_lookup, session, sample_col, run_number
        )
        if assembly_id is None:
            duplicates.append("unmatched assembly for contaminant")
            continue
        values = (
            assembly_id,
            _as_python(row.get("tool")),
            _as_python(row.get("confidence")),
            _as_python(row.get("classification")),
        )
        if values in seen:
            duplicates.append(f"contaminant {assembly_id}")
            continue
        seen.add(values)
        session.add(
            Contaminant(
                assembly_id=assembly_id,
                tool=values[1],
                confidence=values[2],
                classification=values[3],
            )
        )
        added += 1
    report["contaminants"] = {"added": added, "duplicates": duplicates}


def _ingest_amr_records(
    file_path: str,
    *,
    session: Session,
    report: Dict[str, Dict[str, object]],
    assembly_lookup: Dict[str, Assembly],
    run_number: Optional[str] = None,
):
    df = _rename_columns(pd.read_csv(file_path, sep="\t"))
    sample_col = "SampleID" if "SampleID" in df.columns else None
    rename_map = {
        "Contig ID": "contig_id",
        "Gene Symbol": "gene_symbol",
        "Gene Name": "gene_name",
        "Accession of Closest Sequence": "accession",
        "Element Type": "element_type",
        "Resistance Product": "resistance_product",
    }
    df.rename(
        columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True
    )

    gene_cols = [
        "contig_id",
        "gene_symbol",
        "gene_name",
        "accession",
        "element_type",
        "resistance_product",
    ]
    added = 0
    duplicates = []
    seen = set()
    for _, row in df.iterrows():
        assembly_id = _lookup_assembly_id(
            row, assembly_lookup, session, sample_col, run_number
        )
        if assembly_id is None:
            duplicates.append("unmatched assembly for antimicrobial")
            continue
        values = tuple(_as_python(row.get(col)) for col in gene_cols)
        key = (assembly_id,) + values
        if key in seen:
            duplicates.append(f"antimicrobial {assembly_id}")
            continue
        seen.add(key)
        session.add(
            Antimicrobial(
                assembly_id=assembly_id,
                contig_id=values[0],
                gene_symbol=values[1],
                gene_name=values[2],
                accession=values[3],
                element_type=values[4],
                resistance_product=values[5],
            )
        )
        added += 1
    report["antimicrobials"] = {"added": added, "duplicates": duplicates}


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
) -> Dict[str, Dict[str, object]]:
    """Ingest a collection of TSV files with optional confirmation.

    When ``yes`` is False, a summary of pending changes (including duplicates)
    is printed and the user is prompted for confirmation before committing.
    """

    created_session = False
    if session is None:
        session = get_session()
        created_session = True
    
    _check_orphan_assemblies(session, assemblies)
    _check_oprphan_info(session, assembly_qcs, taxonomic_assignments, contaminants, antimicrobials)

    trans = session.begin_nested() if session.in_transaction() else session.begin()
    try:
        assembly_lookup: Dict[str, Assembly] = {}

        if isolates:
            _ingest_isolates(isolates, session, report)
        if assemblies:
            assembly_lookup = _ingest_assemblies(
                assemblies,
                session=session,
                report=report,
            )
        if assembly_qcs:
            _ingest_qc_records(
                assembly_qcs,
                session=session,
                report=report,
                assembly_lookup=assembly_lookup,
            )
        if taxonomic_assignments:
            _ingest_taxonomic_assignments(
                taxonomic_assignments,
                session=session,
                report=report,
                assembly_lookup=assembly_lookup,
            )
        if contaminants:
            _ingest_contaminants(
                contaminants,
                session=session,
                report=report,
                assembly_lookup=assembly_lookup,
            )
        if antimicrobials:
            _ingest_amr_records(
                antimicrobials,
                session=session,
                report=report,
                assembly_lookup=assembly_lookup,
            )

        summary = _summarize(report)
        print(summary)
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

    return report

