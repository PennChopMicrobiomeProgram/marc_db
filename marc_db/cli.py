import argparse
import sys
from marc_db import __version__
from marc_db.db import create_database
from marc_db.ingest import ingest_tsv
from marc_db.mock import fill_mock_db


def main():
    usage_str = "%(prog)s [-h/--help,-v/--version] <subcommand>"
    description_str = (
        "subcommands:\n"
        "  init         \tInitialize a new database.\n"
        "  mock_db      \tFill mock values into an empty db (for testing).\n"
        "  ingest       \tIngest data from a file into the database.\n"
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

    args, remaining = parser.parse_known_args()

    if args.command == "init":
        create_database()
    elif args.command == "mock_db":
        fill_mock_db()
    elif args.command == "ingest":
        parser_ingest = argparse.ArgumentParser(
            prog="marc_db ingest",
            usage="%(prog)s <file_path>",
            description="Ingest data from a tsv into the database.",
        )
        parser_ingest.add_argument("file_path", help="Path to the file to ingest.")
        args_ingest = parser_ingest.parse_args(remaining)
        ingest_tsv(args_ingest.file_path)
    else:
        parser.print_help()
        sys.stderr.write("Unrecognized command.\n")
