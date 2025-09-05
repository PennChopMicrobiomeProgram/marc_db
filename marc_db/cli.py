import argparse
import sys
from marc_db import __version__
from marc_db.db import create_database, get_session, get_marc_db_url
from marc_db.ingest import ingest_tsv, ingest_assembly_tsv, ingest_antimicrobial_tsv
from marc_db.mock import fill_mock_db


def main():
    usage_str = "%(prog)s [-h/--help,-v/--version] <subcommand>"
    description_str = (
        "subcommands:\n"
        "  init         \tInitialize a new database.\n"
        "  mock_db      \tFill mock values into an empty db (for testing).\n"
        "  ingest       \tIngest data from a file into the database.\n"
        "  ingest_assembly\tIngest assembly- or antimicrobial data.\n"
    )

    parser = argparse.ArgumentParser(
        prog="marc_db",
        usage=usage_str,
        description=description_str,
        epilog="",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )

    parser.add_argument("command", help=argparse.SUPPRESS, nargs="?")
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=__version__,
    )
    parser.add_argument(
        "--db",
        help="Database URL to use instead of MARC_DB_URL",
        default=None,
    )

    args, remaining = parser.parse_known_args()

    db_url = args.db or get_marc_db_url()

    if args.command == "init":
        create_database(db_url)
    elif args.command == "mock_db":
        fill_mock_db(get_session(db_url))
    elif args.command == "ingest":
        parser_ingest = argparse.ArgumentParser(
            prog="marc_db ingest",
            usage="%(prog)s <file_path>",
            description="Ingest data from a tsv into the database.",
        )
        parser_ingest.add_argument("file_path", help="Path to the file to ingest.")
        args_ingest = parser_ingest.parse_args(remaining)
        ingest_tsv(args_ingest.file_path, get_session(db_url))
    elif args.command == "ingest_assembly":
        parser_asm = argparse.ArgumentParser(
            prog="marc_db ingest_assembly",
            usage="%(prog)s [assembly_tsv] [amr_tsv] [options]",
            description=(
                "Ingest assembly related data and optionally antimicrobial data "
                "from TSV files into the database."
            ),
        )
        parser_asm.add_argument(
            "assembly_file",
            nargs="?",
            help="Assembly TSV file to ingest.",
        )
        parser_asm.add_argument(
            "amr_file",
            nargs="?",
            help="Antimicrobial TSV file to ingest.",
        )
        parser_asm.add_argument("--metagenomic-sample-id")
        parser_asm.add_argument("--metagenomic-run-id")
        parser_asm.add_argument("--run-number")
        parser_asm.add_argument("--sunbeam-version")
        parser_asm.add_argument("--sbx-sga-version")
        parser_asm.add_argument("--config-file")
        parser_asm.add_argument("--sunbeam-output-path")
        args_asm = parser_asm.parse_args(remaining)
        create_database(db_url)
        if args_asm.assembly_file:
            ingest_assembly_tsv(
                args_asm.assembly_file,
                metagenomic_sample_id=args_asm.metagenomic_sample_id,
                metagenomic_run_id=args_asm.metagenomic_run_id,
                run_number=args_asm.run_number,
                sunbeam_version=args_asm.sunbeam_version,
                sbx_sga_version=args_asm.sbx_sga_version,
                config_file=args_asm.config_file,
                sunbeam_output_path=args_asm.sunbeam_output_path,
                session=get_session(db_url),
            )
        if args_asm.amr_file:
            ingest_antimicrobial_tsv(
                args_asm.amr_file,
                metagenomic_sample_id=args_asm.metagenomic_sample_id,
                metagenomic_run_id=args_asm.metagenomic_run_id,
                run_number=args_asm.run_number,
                sunbeam_version=args_asm.sunbeam_version,
                sbx_sga_version=args_asm.sbx_sga_version,
                config_file=args_asm.config_file,
                sunbeam_output_path=args_asm.sunbeam_output_path,
                session=get_session(db_url),
            )
    else:
        parser.print_help()
        sys.stderr.write("Unrecognized command.\n")
