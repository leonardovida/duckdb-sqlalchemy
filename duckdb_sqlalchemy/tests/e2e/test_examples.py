"""Runnable examples in examples/ execute successfully as scripts."""

import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
# MotherDuck examples need a token and network; their offline engine
# construction is covered in test_connection_fixes.py.
LOCAL_EXAMPLES = [
    "partitioned_parquet",
    "snapshot_live_join",
    "snapshot_schema_evolution",
    "sqlalchemy_example",
]


@pytest.mark.skipif(not EXAMPLES.is_dir(), reason="examples directory not available")
@pytest.mark.parametrize("name", LOCAL_EXAMPLES)
def test_local_example_runs(name: str, tmp_path: Path) -> None:
    completed = subprocess.run(
        [sys.executable, str(EXAMPLES / f"{name}.py")],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip()
