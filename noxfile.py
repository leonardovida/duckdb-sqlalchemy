from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import github_action_utils as gha
import nox

nox.options.default_venv_backend = "uv"
nox.options.error_on_external_run = True


@contextmanager
def group(title: str) -> Generator[None, None, None]:
    try:
        gha.start_group(title)
        yield
    except Exception as e:
        gha.end_group()
        gha.error(f"{title} failed with {e}")
        raise
    else:
        gha.end_group()


def install_dev(session: nox.Session) -> None:
    session.install("-e", ".[dev,devtools]")


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _require_tracked_file(session: nox.Session, relpath: str) -> Path:
    path = _repo_root() / relpath
    if not path.exists():
        session.skip(f"Missing {relpath} (scripts/ not tracked in this workspace)")
    return path


# TODO: "0.5.1", "0.6.1", "0.7.1", "0.8.1"
# TODO: 3.11, 3.12, 3.13
@nox.session(py=["3.10", "3.13"])
@nox.parametrize("duckdb", ["1.0.0", "1.4.3"])
@nox.parametrize("sqlalchemy", ["1.3", "1.4", "2.0.45"])
def tests(session: nox.Session, duckdb: str, sqlalchemy: str) -> None:
    tests_core(session, duckdb, sqlalchemy)


@nox.session(py=["3.13"])
def nightly(session: nox.Session) -> None:
    session.skip("DuckDB nightly installs are broken right now")
    tests_core(session, "master", "1.4")


def tests_core(session: nox.Session, duckdb: str, sqlalchemy: str) -> None:
    with group(f"{session.name} - Install"):
        install_dev(session)
        operator = "==" if sqlalchemy.count(".") == 2 else "~="
        session.install(f"sqlalchemy{operator}{sqlalchemy}")
        if duckdb == "master":
            session.install("duckdb", "--pre", "-U")
        else:
            session.install(f"duckdb=={duckdb}")
    with group(f"{session.name} Test"):
        session.run(
            "pytest",
            "--junitxml=results.xml",
            "--cov",
            "--cov-report",
            "xml:coverage.xml",
            "--verbose",
            "-rs",
            "--remote-data",
            env={
                "SQLALCHEMY_WARN_20": "true",
            },
        )


@nox.session(py=["3.13"])
def mypy(session: nox.Session) -> None:
    install_dev(session)
    session.run("mypy", "duckdb_engine/")


@nox.session(py=["3.13"])
def regression_versions(session: nox.Session) -> None:
    _require_tracked_file(session, "scripts/run_duckdb_engine_version_regression.py")
    session.run(
        "python", "scripts/run_duckdb_engine_version_regression.py", silent=False
    )


@nox.session(py=["3.13"])
def regression_duckdb_versions(session: nox.Session) -> None:
    _require_tracked_file(session, "scripts/run_duckdb_version_matrix.py")
    session.run("python", "scripts/run_duckdb_version_matrix.py", silent=False)


@nox.session(py=["3.13"])
def perf_motherduck(session: nox.Session) -> None:
    _require_tracked_file(session, "scripts/perf/run_motherduck_perf_suite.py")
    session.run("python", "scripts/perf/run_motherduck_perf_suite.py", silent=False)
