"""Runnable examples in examples/ execute successfully as scripts."""

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pytest

EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
LOCAL_EXAMPLES = ["local_quickstart", "partitioned_parquet", "snapshot_live_join"]
# Each MotherDuck example creates a uniquely named database and drops it, so
# the live runs are safe to repeat. A line each run must print:
MOTHERDUCK_EXAMPLES = {
    "motherduck_quickstart": "EMEA: 162.25",
    "motherduck_concurrent_writers": "counter = 20 (expected 20)",
    "motherduck_read_scaling": "shared pool served 8 queries",
    "motherduck_dive": "marked version 2 ready",
}
TOKEN_VARIABLES = ("MOTHERDUCK_TOKEN", "motherduck_token")

requires_examples = pytest.mark.skipif(
    not EXAMPLES.is_dir(), reason="examples directory not available"
)


def _run(
    name: str, cwd: Path, env: Optional[Dict[str, str]] = None, timeout: int = 120
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(EXAMPLES / f"{name}.py")],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@requires_examples
@pytest.mark.parametrize("name", LOCAL_EXAMPLES)
def test_local_example_runs(name: str, tmp_path: Path) -> None:
    completed = _run(name, tmp_path)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip()


@requires_examples
def test_every_example_is_covered() -> None:
    covered: List[str] = [*LOCAL_EXAMPLES, *MOTHERDUCK_EXAMPLES]
    assert sorted(p.stem for p in EXAMPLES.glob("*.py")) == sorted(covered)


@requires_examples
@pytest.mark.parametrize("name", sorted(MOTHERDUCK_EXAMPLES))
def test_motherduck_example_explains_a_missing_token(name: str, tmp_path: Path) -> None:
    env = {k: v for k, v in os.environ.items() if k not in TOKEN_VARIABLES}
    completed = _run(name, tmp_path, env=env)
    assert completed.returncode != 0
    assert "Set MOTHERDUCK_TOKEN" in completed.stderr


@pytest.mark.remote_data
@pytest.mark.skipif(
    not (os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")),
    reason="Set MOTHERDUCK_TOKEN to run the MotherDuck examples",
)
@requires_examples
@pytest.mark.parametrize("name", sorted(MOTHERDUCK_EXAMPLES))
def test_motherduck_example_runs(name: str, tmp_path: Path) -> None:
    completed = _run(name, tmp_path, timeout=600)
    assert completed.returncode == 0, completed.stderr
    assert MOTHERDUCK_EXAMPLES[name] in completed.stdout
