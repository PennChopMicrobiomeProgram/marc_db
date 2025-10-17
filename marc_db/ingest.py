import pandas as pd
from marc_db.db import get_session
from marc_db.models import (
    Aliquot,
    Isolate,
    Assembly,
    AssemblyQC,
    TaxonomicAssignment,
    Antimicrobial,
)
from sqlalchemy.orm import Session
from typing import Optional


def _as_python(value):
    """Return a plain Python value suitable for SQL insertion."""
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else value


def ingest_tsv(file_path: str, session: Optional[Session] = None) -> pd.DataFrame:
    """
    Import a tsv file to pandas DataFrame and load it into the database.

    Parameters:
    file_path (str): The path to the xlsx file.
    connection (Connection): The connection to the database.

    Returns:
    pd.DataFrame: The imported data as a pandas DataFrame.
    """
    if not session:
        # Define this here instead of as a default argument in order to avoid loading it ahead of time
        session = get_session()

    df = pd.read_csv(file_path, delimiter="\t")

    # Expect one aliquot per row but only a single isolate per SampleID.
    # Check for duplicate isolates and ensure all isolate level fields match.
    isolate_cols = [
        "SampleID",
        "Subject ID",
        "Specimen ID",
        "sample species",
        "special_collection",
        "Received by mARC",
        "Cryobanking",
    ]
    duplicate_groups = df.duplicated(subset=["SampleID"], keep=False)
    if duplicate_groups.any():
        for sample_id, group in df[duplicate_groups].groupby("SampleID"):
            subset = group[isolate_cols].drop(columns=["SampleID"])
            if not (subset.nunique(dropna=False) <= 1).all():
                raise ValueError(
                    f"Inconsistent isolate information for sample_id {sample_id}"
                )
    # Reduce to a single row per isolate for isolate ingestion
    unique_isolates_df = (
        df[isolate_cols].drop_duplicates(subset=["SampleID"], keep="first").copy()
    )

    iso_before = session.query(Isolate).count()
    ali_before = session.query(Aliquot).count()

    # Prepare isolate dataframe from the unique isolate rows
    isolate_df = unique_isolates_df.copy()
    # Rename columns to match the database schema
    isolate_df.columns = [
        "sample_id",
        "subject_id",
        "specimen_id",
        "suspected_organism",
        "special_collection",
        "received_date",
        "cryobanking_date",
    ]
    # Convert date columns to datetime
    isolate_df["received_date"] = pd.to_datetime(
        isolate_df["received_date"], errors="coerce"
    ).dt.date
    isolate_df["cryobanking_date"] = pd.to_datetime(
        isolate_df["cryobanking_date"], errors="coerce"
    ).dt.date
    # Insert into the database
    isolate_df.to_sql("isolates", con=session.bind, if_exists="append", index=False)
    session.commit()
    iso_after = session.query(Isolate).count()
    added_isolates = iso_after - iso_before
    failed_isolates = len(isolate_df) - added_isolates

    # Extract aliquot_df
    aliquot_df = df[["Tube Barcode", "Box-name_position", "SampleID"]].copy()
    # Rename columns to match the database schema
    aliquot_df.columns = ["tube_barcode", "box_name", "sample_id"]
    # Associate aliquots with isolates using sample_id
    aliquot_df["isolate_id"] = aliquot_df["sample_id"]
    aliquot_df.drop(columns=["sample_id"], inplace=True)
    aliquot_df.to_sql("aliquots", con=session.bind, if_exists="append", index=False)
    session.commit()
    ali_after = session.query(Aliquot).count()
    added_aliquots = ali_after - ali_before
    failed_aliquots = len(aliquot_df) - added_aliquots

    print(
        f"Isolates added: {added_isolates} success, {failed_isolates} failed; "
        f"Aliquots added: {added_aliquots} success, {failed_aliquots} failed"
    )

    return df


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
    """Ingest assembly related data from ``file_path``.

    The TSV must contain a ``Sample`` column referencing existing
    ``Isolate.sample_id`` values. All assembly level metadata is supplied via
    function arguments. Rows are interpreted as antimicrobial gene entries with
    accompanying QC and taxonomic information which are collapsed per sample.
    """

    if session is None:
        session = get_session()

    df = pd.read_csv(file_path, sep="\t")

    # Normalize column names from common pipeline outputs
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
    }
    df.rename(
        columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True
    )

    for sample, g in df.groupby("Sample"):
        asm = Assembly(
            isolate_id=sample,
            metagenomic_sample_id=metagenomic_sample_id,
            metagenomic_run_id=metagenomic_run_id,
            run_number=run_number,
            sunbeam_version=sunbeam_version,
            sbx_sga_version=sbx_sga_version,
            config_file=config_file,
            sunbeam_output_path=sunbeam_output_path,
        )
        session.add(asm)
        session.flush()  # populate asm.id

        first = g.iloc[0]
        qc = AssemblyQC(
            assembly_id=asm.id,
            contig_count=_as_python(first.get("contig_count")),
            genome_size=_as_python(first.get("genome_size")),
            n50=_as_python(first.get("n50")),
            gc_content=_as_python(first.get("gc_content")),
            cds=_as_python(first.get("cds")),
            completeness=_as_python(first.get("completeness")),
            contamination=_as_python(first.get("contamination")),
            min_contig_coverage=_as_python(first.get("min_contig_coverage")),
            avg_contig_coverage=_as_python(first.get("avg_contig_coverage")),
            max_contig_coverage=_as_python(first.get("max_contig_coverage")),
        )
        session.add(qc)

        tax = TaxonomicAssignment(
            assembly_id=asm.id,
            taxonomic_classification=_as_python(first.get("taxonomic_classification")),
            taxonomic_abundance=_as_python(first.get("taxonomic_abundance")),
            mash_contamination=_as_python(first.get("mash_contamination")),
            mash_contaminated_spp=_as_python(first.get("mash_contaminated_spp")),
            st=_as_python(first.get("st")),
            st_schema=_as_python(first.get("st_schema")),
            allele_assignment=_as_python(first.get("allele_assignment")),
        )
        session.add(tax)

        gene_cols = [
            "contig_id",
            "gene_symbol",
            "gene_name",
            "accession",
            "element_type",
            "resistance_product",
        ]

        has_gene_cols = any(col in g.columns for col in gene_cols)

        if has_gene_cols:
            for _, row in g.iterrows():
                if not any(pd.notna(row.get(c)) for c in gene_cols):
                    continue
                amr = Antimicrobial(
                    assembly_id=asm.id,
                    contig_id=_as_python(row.get("contig_id")),
                    gene_symbol=_as_python(row.get("gene_symbol")),
                    gene_name=_as_python(row.get("gene_name")),
                    accession=_as_python(row.get("accession")),
                    element_type=_as_python(row.get("element_type")),
                    resistance_product=_as_python(row.get("resistance_product")),
                )
                session.add(amr)

    session.commit()
    return df


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
    """Ingest antimicrobial gene data from ``file_path``.

    Each row represents a gene call for a given sample. A new ``Assembly``
    record is created for each unique sample and all antimicrobial gene rows are
    linked to that assembly.
    """

    if session is None:
        session = get_session()

    df = pd.read_csv(file_path, sep="\t")
    df.dropna(how="all", inplace=True)

    df.columns = df.columns.str.strip()
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

    for sample, g in df.groupby("Sample"):
        asm = Assembly(
            isolate_id=sample,
            metagenomic_sample_id=metagenomic_sample_id,
            metagenomic_run_id=metagenomic_run_id,
            run_number=run_number,
            sunbeam_version=sunbeam_version,
            sbx_sga_version=sbx_sga_version,
            config_file=config_file,
            sunbeam_output_path=sunbeam_output_path,
        )
        session.add(asm)
        session.flush()

        for _, row in g.iterrows():
            amr = Antimicrobial(
                assembly_id=asm.id,
                contig_id=_as_python(row.get("contig_id")),
                gene_symbol=_as_python(row.get("gene_symbol")),
                gene_name=_as_python(row.get("gene_name")),
                accession=_as_python(row.get("accession")),
                element_type=_as_python(row.get("element_type")),
                resistance_product=_as_python(row.get("resistance_product")),
            )
            session.add(amr)

    session.commit()
    return df
