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


def _derive_sunbeam_output_path(
    config_file: Optional[str], sunbeam_output_path: Optional[str]
) -> Optional[str]:
    """Return the sunbeam output path, deriving it from ``config_file`` if needed."""

    if sunbeam_output_path:
        return sunbeam_output_path

    if config_file:
        cfg_path = Path(config_file)
        return str(cfg_path.parent / "sunbeam_output")

    return None


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    rename_map = {
        "Number of Contigs": "contig_count",
        "Genome Size": "genome_size",
        "N50": "n50",
        "GC Content": "gc_content",
        "CDS": "cds",
        "CheckM_Completeness": "completeness",
        "CheckM_Contamination": "contamination",
        "Coverage": "avg_contig_coverage",
        "Schema": "st_schema",
        "ST": "st",
        "Alleles": "allele_assignment",
        "Mash_Contamination": "mash_contamination",
        "Contaminated_Spp": "mash_contaminated_spp",
        "Taxonomic_Abundance": "taxonomic_abundance",
        "Taxonomic_Classification": "taxonomic_classification",
        "Sample": "SampleID",
        "Run": "run_number",
    }
    df.rename(
        columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True
    )
    return df


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


def _parse_sample_column(df: pd.DataFrame) -> str:
    for candidate in ["SampleID", "Sample", "sample_id"]:
        if candidate in df.columns:
            return candidate
    raise ValueError("Could not find a sample identifier column (SampleID or Sample)")


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


def _find_assembly_key(
    isolate_id: str, run_number: Optional[str]
) -> Tuple[str, Optional[str]]:
    return (isolate_id, run_number or "")


def _ingest_assemblies(
    file_path: str,
    *,
    session: Session,
    report: Dict[str, Dict[str, object]],
    run_number: Optional[str] = None,
    sunbeam_version: Optional[str] = None,
    sbx_sga_version: Optional[str] = None,
    metagenomic_sample_id: Optional[str] = None,
    metagenomic_run_id: Optional[str] = None,
    config_file: Optional[str] = None,
    sunbeam_output_path: Optional[str] = None,
    include_qc: bool = True,
    include_tax: bool = True,
    include_amr: bool = True,
) -> Dict[str, Assembly]:
    df = _rename_columns(pd.read_csv(file_path, sep="\t"))
    sample_col = _parse_sample_column(df)

    sample_ids = df[sample_col].unique().tolist()
    resolved_output_path = _derive_sunbeam_output_path(config_file, sunbeam_output_path)
    existing_keys = {
        _find_assembly_key(asm.isolate_id, asm.run_number): asm
        for asm in session.query(Assembly).filter(Assembly.isolate_id.in_(sample_ids))
    }

    added = 0
    duplicates = []
    assembly_lookup: Dict[str, Assembly] = {}

    gene_cols = [
        "contig_id",
        "gene_symbol",
        "gene_name",
        "accession",
        "element_type",
        "resistance_product",
    ]
    qc_cols = [
        "contig_count",
        "genome_size",
        "n50",
        "gc_content",
        "cds",
        "completeness",
        "contamination",
        "min_contig_coverage",
        "avg_contig_coverage",
        "max_contig_coverage",
    ]
    tax_cols = [
        "taxonomic_classification",
        "taxonomic_abundance",
        "mash_contamination",
        "mash_contaminated_spp",
        "st",
        "st_schema",
        "allele_assignment",
    ]

    for sample_id, group in df.groupby(sample_col):
        key = _find_assembly_key(
            sample_id, _as_python(group.iloc[0].get("run_number", run_number))
        )
        if key in existing_keys:
            duplicates.append(f"assembly {sample_id} run {key[1] or '(none)'}")
            assembly_lookup[sample_id] = existing_keys[key]
            continue

        asm = Assembly(
            isolate_id=sample_id,
            metagenomic_sample_id=_as_python(
                group.iloc[0].get("metagenomic_sample_id", metagenomic_sample_id)
            ),
            metagenomic_run_id=_as_python(
                group.iloc[0].get("metagenomic_run_id", metagenomic_run_id)
            ),
            nanopore_path=_as_python(group.iloc[0].get("nanopore_path")),
            run_number=_as_python(group.iloc[0].get("run_number", run_number)),
            sunbeam_version=_as_python(
                group.iloc[0].get("sunbeam_version", sunbeam_version)
            ),
            sbx_sga_version=_as_python(
                group.iloc[0].get("sbx_sga_version", sbx_sga_version)
            ),
            sunbeam_output_path=_as_python(
                group.iloc[0].get("sunbeam_output_path", resolved_output_path)
            ),
            ncbi_id=_as_python(group.iloc[0].get("ncbi_id")),
        )
        session.add(asm)
        session.flush()
        added += 1
        assembly_lookup[sample_id] = asm

        first = group.iloc[0]
        if include_qc and any(col in group.columns for col in qc_cols):
            existing_qc = session.get(AssemblyQC, asm.id)
            if existing_qc:
                duplicates.append(f"assembly_qc {asm.id}")
            else:
                session.add(
                    AssemblyQC(
                        assembly_id=asm.id,
                        contig_count=_as_python(first.get("contig_count")),
                        genome_size=_as_python(first.get("genome_size")),
                        n50=_as_python(first.get("n50")),
                        gc_content=_as_python(first.get("gc_content")),
                        cds=_as_python(first.get("cds")),
                        completeness=_as_python(first.get("completeness")),
                        contamination=_as_python(first.get("contamination")),
                        min_contig_coverage=_as_python(
                            first.get("min_contig_coverage")
                        ),
                        avg_contig_coverage=_as_python(
                            first.get("avg_contig_coverage")
                        ),
                        max_contig_coverage=_as_python(
                            first.get("max_contig_coverage")
                        ),
                    )
                )

        if include_tax and any(col in group.columns for col in tax_cols):
            seen_tax = set()
            for _, row in group.iterrows():
                tax_values = tuple(_as_python(row.get(col)) for col in tax_cols)
                if tax_values in seen_tax:
                    duplicates.append(f"taxonomic_assignment for {asm.id}")
                    continue
                if all(value is None for value in tax_values):
                    continue
                seen_tax.add(tax_values)
                session.add(
                    TaxonomicAssignment(
                        assembly_id=asm.id,
                        taxonomic_classification=tax_values[0],
                        taxonomic_abundance=tax_values[1],
                        mash_contamination=tax_values[2],
                        mash_contaminated_spp=tax_values[3],
                        st=tax_values[4],
                        st_schema=tax_values[5],
                        allele_assignment=tax_values[6],
                    )
                )

        if include_amr and any(col in group.columns for col in gene_cols):
            seen_amr: set[Tuple] = set()
            for _, row in group.iterrows():
                if not any(pd.notna(row.get(c)) for c in gene_cols):
                    continue
                amr_tuple = tuple(_as_python(row.get(col)) for col in gene_cols)
                if amr_tuple in seen_amr:
                    duplicates.append(f"antimicrobial for {asm.id}")
                    continue
                seen_amr.add(amr_tuple)
                session.add(
                    Antimicrobial(
                        assembly_id=asm.id,
                        contig_id=amr_tuple[0],
                        gene_symbol=amr_tuple[1],
                        gene_name=amr_tuple[2],
                        accession=amr_tuple[3],
                        element_type=amr_tuple[4],
                        resistance_product=amr_tuple[5],
                    )
                )

    report["assemblies"] = {"added": added, "duplicates": duplicates}
    return assembly_lookup


def _lookup_assembly_id(
    row: pd.Series,
    assembly_lookup: Dict[str, Assembly],
    session: Session,
    sample_col: Optional[str] = None,
    run_number: Optional[str] = None,
) -> Optional[int]:
    if "assembly_id" in row:
        return int(row["assembly_id"])
    if sample_col and sample_col in row:
        sample_id = row[sample_col]
        if sample_id in assembly_lookup:
            return assembly_lookup[sample_id].id
        assemblies = (
            session.query(Assembly).filter(Assembly.isolate_id == sample_id).all()
        )
        if len(assemblies) == 1:
            return assemblies[0].id
        if len(assemblies) > 1:
            match = [
                asm
                for asm in assemblies
                if (asm.run_number or "") == (run_number or "")
            ]
            if len(match) == 1:
                return match[0].id
    return None


def _ingest_qc_records(
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
    isolates: Optional[str] = None,
    assemblies: Optional[str] = None,
    assembly_qcs: Optional[str] = None,
    taxonomic_assignments: Optional[str] = None,
    contaminants: Optional[str] = None,
    antimicrobials: Optional[str] = None,
    yes: bool = False,
    session: Optional[Session] = None,
    run_number: Optional[str] = None,
    sunbeam_version: Optional[str] = None,
    sbx_sga_version: Optional[str] = None,
    metagenomic_sample_id: Optional[str] = None,
    metagenomic_run_id: Optional[str] = None,
    config_file: Optional[str] = None,
    sunbeam_output_path: Optional[str] = None,
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
    report: Dict[str, Dict[str, object]] = {}

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
                run_number=run_number,
                sunbeam_version=sunbeam_version,
                sbx_sga_version=sbx_sga_version,
                metagenomic_sample_id=metagenomic_sample_id,
                metagenomic_run_id=metagenomic_run_id,
                config_file=config_file,
                sunbeam_output_path=sunbeam_output_path,
                include_qc=not assembly_qcs,
                include_tax=not taxonomic_assignments,
                include_amr=not antimicrobials,
            )
        if assembly_qcs:
            _ingest_qc_records(
                assembly_qcs,
                session=session,
                report=report,
                assembly_lookup=assembly_lookup,
                run_number=run_number,
            )
        if taxonomic_assignments:
            _ingest_taxonomic_assignments(
                taxonomic_assignments,
                session=session,
                report=report,
                assembly_lookup=assembly_lookup,
                run_number=run_number,
            )
        if contaminants:
            _ingest_contaminants(
                contaminants,
                session=session,
                report=report,
                assembly_lookup=assembly_lookup,
                run_number=run_number,
            )
        if antimicrobials:
            _ingest_amr_records(
                antimicrobials,
                session=session,
                report=report,
                assembly_lookup=assembly_lookup,
                run_number=run_number,
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


def ingest_tsv(file_path: str, session: Optional[Session] = None) -> pd.DataFrame:
    """Backward compatible wrapper for isolate/aliquot ingestion."""

    ingest_from_tsvs(isolates=file_path, yes=True, session=session)
    return pd.read_csv(file_path, delimiter="\t")


def ingest_assembly_tsv(
    file_path: str,
    *,
    metagenomic_sample_id: Optional[str] = None,
    metagenomic_run_id: Optional[str] = None,
    run_number: Optional[str] = None,
    sunbeam_version: Optional[str] = None,
    sbx_sga_version: Optional[str] = None,
    config_file: Optional[str] = None,
    sunbeam_output_path: Optional[str] = None,
    session: Optional[Session] = None,
) -> pd.DataFrame:
    """Backward compatible wrapper around :func:`ingest_from_tsvs`."""

    ingest_from_tsvs(
        assemblies=file_path,
        antimicrobials=file_path,
        assembly_qcs=file_path,
        taxonomic_assignments=file_path,
        run_number=run_number,
        sunbeam_version=sunbeam_version,
        sbx_sga_version=sbx_sga_version,
        metagenomic_sample_id=metagenomic_sample_id,
        metagenomic_run_id=metagenomic_run_id,
        config_file=config_file,
        sunbeam_output_path=sunbeam_output_path,
        yes=True,
        session=session,
    )
    return pd.read_csv(file_path, sep="\t")


def ingest_antimicrobial_tsv(
    file_path: str,
    *,
    metagenomic_sample_id: Optional[str] = None,
    metagenomic_run_id: Optional[str] = None,
    run_number: Optional[str] = None,
    sunbeam_version: Optional[str] = None,
    sbx_sga_version: Optional[str] = None,
    config_file: Optional[str] = None,
    sunbeam_output_path: Optional[str] = None,
    session: Optional[Session] = None,
) -> pd.DataFrame:
    """Backward compatible wrapper around :func:`ingest_from_tsvs`."""

    ingest_from_tsvs(
        antimicrobials=file_path,
        assemblies=file_path,
        run_number=run_number,
        sunbeam_version=sunbeam_version,
        sbx_sga_version=sbx_sga_version,
        metagenomic_sample_id=metagenomic_sample_id,
        metagenomic_run_id=metagenomic_run_id,
        config_file=config_file,
        sunbeam_output_path=sunbeam_output_path,
        yes=True,
        session=session,
    )
    return pd.read_csv(file_path, sep="\t")
