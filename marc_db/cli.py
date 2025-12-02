import argparse
import sys
from marc_db import __version__
from marc_db.db import create_database, get_session, get_marc_db_url
from marc_db.ingest import ingest_from_tsvs
from marc_db.mock import fill_mock_db


def main():
    usage_str = "%(prog)s [-h/--help,-v/--version] <subcommand>"
    description_str = (
        "subcommands:\n"
        "  init         \tInitialize a new database.\n"
        "  mock_db      \tFill mock values into an empty db (for testing).\n"
        "  ingest       \tIngest data from TSV files into the database.\n"
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
            usage="%(prog)s [--isolates FILE] [--assemblies FILE] [--assembly-qcs FILE] "
            "[--taxonomic-assignments FILE] [--contaminants FILE] [--antimicrobials FILE]",
            description=(
                "Ingest isolates, assemblies, QC, taxonomic assignments, contaminants, "
                "and antimicrobials from TSV files."
            ),
        )
        parser_ingest.add_argument(
            "--isolates", help="TSV containing isolates/aliquots."
        )
        parser_ingest.add_argument("--assemblies", help="TSV containing assemblies.")
        parser_ingest.add_argument(
            "--assembly-qcs", help="TSV containing assembly QC records."
        )
        parser_ingest.add_argument(
            "--taxonomic-assignments", help="TSV containing taxonomic assignments."
        )
        parser_ingest.add_argument(
            "--contaminants", help="TSV containing contaminant calls."
        )
        parser_ingest.add_argument(
            "--antimicrobials", help="TSV containing antimicrobial calls."
        )
        parser_ingest.add_argument(
            "--yes", action="store_true", help="Skip confirmation prompt."
        )
        args_ingest = parser_ingest.parse_args(remaining)
        create_database(db_url)
        ingest_from_tsvs(
            isolates=args_ingest.isolates,
            assemblies=args_ingest.assemblies,
            assembly_qcs=args_ingest.assembly_qcs,
            taxonomic_assignments=args_ingest.taxonomic_assignments,
            contaminants=args_ingest.contaminants,
            antimicrobials=args_ingest.antimicrobials,
            yes=args_ingest.yes,
            session=get_session(db_url),
        )
    else:
        parser.print_help()
        sys.stderr.write("Unrecognized command.\n")
