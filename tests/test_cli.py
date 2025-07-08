import subprocess
import sys
from pathlib import Path

def test_cli_db_arg():
    file_path = Path(__file__).parent / "test_multi_aliquot.tsv"
    result = subprocess.run(
        [sys.executable, "-m", "marc_db.cli", "--db", "sqlite:///:memory:", "ingest", str(file_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0
