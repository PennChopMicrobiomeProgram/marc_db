import random
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from marc_db.db import get_session
from marc_db.models import (
    Aliquot,
    Antimicrobial,
    Assembly,
    AssemblyQC,
    Isolate,
    TaxonomicAssignment,
)
from sqlalchemy.orm import Session


ORGANISMS = [
    "K. pneumoniae",
    "E. coli",
    "P. aeruginosa",
    "A. baumannii",
    "S. aureus",
    "E. faecium",
]
SPECIAL_COLLECTIONS = ["none", "blood", "urine", "respiratory", "wound"]
BOX_NAMES = [f"box-{letter}{number}" for letter in "ABC" for number in range(1, 5)]
GENE_SYMBOLS = ["blaKPC", "blaNDM", "blaOXA", "mcr-1", "aadA", "tetA"]
GENE_PRODUCTS = ["beta-lactamase", "colistin resistance", "aminoglycoside resistance", "tetracycline resistance"]


def _random_date(rng: random.Random, start_year: int = 2020, end_year: int = 2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    days_between = (end - start).days
    return (start + timedelta(days=rng.randrange(days_between))).date()


def _create_isolate(rng: random.Random, sample_index: int) -> Isolate:
    sample_id = f"sample{sample_index}"
    return Isolate(
        sample_id=sample_id,
        subject_id=rng.randint(1, 200),
        specimen_id=rng.randint(1, 20),
        suspected_organism=rng.choice(ORGANISMS),
        special_collection=rng.choice(SPECIAL_COLLECTIONS),
        received_date=_random_date(rng),
        cryobanking_date=_random_date(rng),
    )


def _create_aliquots(
    rng: random.Random, isolate_id: str, count: int, aliquot_index: int
) -> Tuple[List[Aliquot], int]:
    aliquots: List[Aliquot] = []
    for _ in range(count):
        tube_id = f"{isolate_id}-T{aliquot_index}-{rng.randint(1000, 9999)}"
        box_name = rng.choice(BOX_NAMES)
        aliquots.append(
            Aliquot(
                isolate_id=isolate_id,
                tube_barcode=tube_id,
                box_name=box_name,
            )
        )
        aliquot_index += 1
    return aliquots, aliquot_index


def _create_assembly_bundle(
    rng: random.Random, isolate_id: str
) -> Tuple[Assembly, AssemblyQC, TaxonomicAssignment, List[Antimicrobial]]:
    assembly = Assembly(
        isolate_id=isolate_id,
        metagenomic_sample_id=f"MG-{rng.randint(1000, 9999)}",
        metagenomic_run_id=f"RUN-{rng.randint(1000, 9999)}",
        nanopore_path=f"/data/nanopore/{isolate_id}",
        run_number=str(rng.randint(1, 50)),
        sunbeam_version=f"{rng.randint(1, 3)}.{rng.randint(0, 9)}",
        sbx_sga_version=f"{rng.randint(1, 3)}.{rng.randint(0, 9)}",
        sunbeam_output_path=f"/data/sunbeam/{isolate_id}",
    )

    assembly_qc = AssemblyQC(
        assembly=assembly,
        contig_count=rng.randint(50, 250),
        genome_size=rng.randint(2000000, 7000000),
        n50=rng.randint(20000, 50000),
        gc_content=round(rng.uniform(30.0, 70.0), 2),
        cds=rng.randint(1000, 5500),
        completeness=round(rng.uniform(85.0, 100.0), 2),
        contamination=round(rng.uniform(0.0, 5.0), 2),
        min_contig_coverage=round(rng.uniform(10.0, 50.0), 2),
        avg_contig_coverage=round(rng.uniform(30.0, 80.0), 2),
        max_contig_coverage=round(rng.uniform(60.0, 150.0), 2),
    )

    tax_assignment = TaxonomicAssignment(
        assembly=assembly,
        taxonomic_classification=rng.choice(ORGANISMS),
        taxonomic_abundance=round(rng.uniform(50.0, 100.0), 2),
        mash_contamination=round(rng.uniform(0.0, 5.0), 2),
        mash_contaminated_spp=rng.choice(ORGANISMS),
        st=str(rng.randint(1, 500)),
        st_schema=rng.choice(["mlst", "cgmlst"]),
        allele_assignment=";".join(
            f"gene{idx}:{rng.randint(1, 10)}" for idx in range(1, 4)
        ),
    )

    antimicrobials: List[Antimicrobial] = []
    for amr_index in range(rng.randint(1, 3)):
        symbol = rng.choice(GENE_SYMBOLS)
        antimicrobials.append(
            Antimicrobial(
                assembly=assembly,
                contig_id=f"contig_{amr_index+1}",
                gene_symbol=symbol,
                gene_name=f"{symbol} gene",
                accession=f"ACC{rng.randint(10000, 99999)}",
                element_type=rng.choice(["plasmid", "chromosome"]),
                resistance_product=rng.choice(GENE_PRODUCTS),
            )
        )

    return assembly, assembly_qc, tax_assignment, antimicrobials


def _build_mock_dataset(
    num_isolates: int = 75,
    min_aliquots_per_isolate: int = 3,
    max_aliquots_per_isolate: int = 5,
    seed: int = 1337,
):
    rng = random.Random(seed)
    isolates: List[Isolate] = []
    aliquots: List[Aliquot] = []
    assemblies: List[Assembly] = []
    assembly_qcs: List[AssemblyQC] = []
    tax_assignments: List[TaxonomicAssignment] = []
    antimicrobials: List[Antimicrobial] = []

    aliquot_index = 1
    for isolate_index in range(1, num_isolates + 1):
        isolate = _create_isolate(rng, isolate_index)
        isolates.append(isolate)

        aliquot_count = rng.randint(min_aliquots_per_isolate, max_aliquots_per_isolate)
        aliquot_batch, aliquot_index = _create_aliquots(
            rng, isolate.sample_id, aliquot_count, aliquot_index
        )
        aliquots.extend(aliquot_batch)

        assembly, assembly_qc, tax_assignment, amr_records = _create_assembly_bundle(
            rng, isolate.sample_id
        )
        assemblies.append(assembly)
        assembly_qcs.append(assembly_qc)
        tax_assignments.append(tax_assignment)
        antimicrobials.extend(amr_records)

    return isolates, aliquots, assemblies, assembly_qcs, tax_assignments, antimicrobials


def fill_mock_db(session: Optional[Session] = None):
    if session is None:
        session = get_session()
    # Check that db is an empty test db
    assert (
        len(session.query(Isolate).all()) == 0
    ), "Database is not empty, I can only add test data to an empty database"

    data = _build_mock_dataset()
    all_records: List = sum((list(group) for group in data), [])
    session.add_all(all_records)
    session.commit()
