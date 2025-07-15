from sqlalchemy import (
    Column,
    Integer,
    Text,
    Date,
    Float,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Isolate(Base):
    __tablename__ = "isolates"

    sample_id = Column(Text, primary_key=True)
    subject_id = Column(Integer, nullable=False)
    specimen_id = Column(Integer, nullable=False)
    source = Column(Text)
    suspected_organism = Column(Text, default="unknown")
    special_collection = Column(Text)
    received_date = Column(Date, nullable=True)
    cryobanking_date = Column(Date, nullable=True)


class Aliquot(Base):
    __tablename__ = "aliquots"
    __tableargs__ = (
        UniqueConstraint(
            "isolate_id",
            "tube_barcode",
            "box_name",
            name="uq_aliquots_isolate_id_tube_barcode_box_name",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    isolate_id = Column(Text, ForeignKey("isolates.sample_id"), nullable=False)
    tube_barcode = Column(Text)
    box_name = Column(Text)


class Assembly(Base):
    __tablename__ = "assemblies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    isolate_id = Column(Text, ForeignKey("isolates.sample_id"), nullable=False)
    metagenomic_sample_id = Column(Text)
    metagenomic_run_number = Column(Text)
    run_number = Column(Text)
    sunbeam_version = Column(Text)
    sbx_sga_version = Column(Text)
    config_file = Column(Text)
    assembly_fasta_path = Column(Text)


class AssemblyQC(Base):
    __tablename__ = "assembly_qc"

    assembly_id = Column(Integer, ForeignKey("assemblies.id"), primary_key=True)
    contig_count = Column(Integer)
    genome_size = Column(Integer)
    n50 = Column(Integer)
    gc_content = Column(Float)
    cds = Column(Integer)
    completeness = Column(Float)
    contamination = Column(Float)
    min_contig_coverage = Column(Float)
    avg_contig_coverage = Column(Float)
    max_contig_coverage = Column(Float)


class TaxonomicAssignment(Base):
    __tablename__ = "taxonomic_assignments"

    assembly_id = Column(Integer, ForeignKey("assemblies.id"), primary_key=True)
    taxonomic_classification = Column(Text)
    st = Column(Text)
    st_schema = Column(Text)
    allele_assignment = Column(Text)


class Antimicrobial(Base):
    __tablename__ = "antimicrobials"

    assembly_id = Column(Integer, ForeignKey("assemblies.id"), primary_key=True)
    contig_id = Column(Text)
    gene_symbol = Column(Text)
    gene_name = Column(Text)
    accession = Column(Text)
    element_type = Column(Text)
    resistance_product = Column(Text)
