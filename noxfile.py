import os
from contextlib import contextmanager
from typing import Generator

import nox
from packaging.version import Version

nox.options.default_venv_backend = "uv"
nox.options.error_on_external_run = True
nox.options.sessions = ["ty"]

_IN_GITHUB_ACTIONS = os.environ.get("GITHUB_ACTIONS") == "true"


@contextmanager
def group(title: str) -> Generator[None, None, None]:
    if not _IN_GITHUB_ACTIONS:
        yield
        return
    print(f"::group::{title}", flush=True)
    try:
        yield
    except Exception as e:
        print("::endgroup::", flush=True)
        print(f"::error::{title} failed with {e}", flush=True)
        raise
    else:
        print("::endgroup::", flush=True)


@nox.session(py=["3.10", "3.11", "3.12", "3.13", "3.14"])
# Keep the matrix aligned with the active DuckDB and SQLAlchemy release lines we validate.
@nox.parametrize(
    "duckdb",
    [
        "1.3.0",
        "1.3.1",
        "1.3.2",
        "1.4.0",
        "1.4.1",
        "1.4.2",
        "1.4.4",
        "1.4.5",
        "1.5.0",
        "1.5.1",
        "1.5.2",
        "1.5.3",
        "1.5.4",
        "1.5.5",
        "1.5.6",
    ],
)
@nox.parametrize("sqlalchemy", ["2.0.0", "2.0.52", "2.1.1"])
def tests(session: nox.Session, duckdb: str, sqlalchemy: str) -> None:
    if session.python == "3.14" and sqlalchemy == "2.0.0":
        session.skip("SQLAlchemy 2.0.0 is not compatible with Python 3.14")
    if session.python == "3.10" and sqlalchemy.startswith("2.1"):
        session.skip("SQLAlchemy 2.1 requires Python 3.11 or newer")
    if session.python == "3.14" and Version(duckdb) < Version("1.4.2"):
        session.skip("DuckDB before 1.4.2 has no Python 3.14 wheels")
    tests_core(session, duckdb, sqlalchemy)


@nox.session(py=["3.11"])
def nightly(session: nox.Session) -> None:
    tests_core(session, "master", "2.1.1", remote_data=False)


def tests_core(
    session: nox.Session,
    duckdb: str,
    sqlalchemy: str,
    *,
    remote_data: bool = True,
) -> None:
    with group(f"{session.name} - Install"):
        session.install("-e", ".[dev]")
        operator = "==" if sqlalchemy.count(".") == 2 else "~="
        session.install(f"sqlalchemy{operator}{sqlalchemy}")
        if duckdb == "master":
            session.install("duckdb", "--pre", "-U")
        else:
            session.install(f"duckdb=={duckdb}")
            # pandas 3 needs SQLAlchemy 2.0.36+, and DuckDB before 1.4.4
            # cannot read its default "str" dtype.
            if Version(sqlalchemy) < Version("2.0.36") or Version(duckdb) < Version(
                "1.4.4"
            ):
                session.install("pandas<3")
    with group(f"{session.name} Test"):
        pytest_args = [
            "pytest",
            "--junitxml=results.xml",
            "--cov",
            "--cov-report",
            "xml:coverage.xml",
            "--verbose",
            "-rs",
        ]
        if remote_data:
            pytest_args.append("--remote-data")
        session.run(*pytest_args)


@nox.session(py=["3.11"])
def ty(session: nox.Session) -> None:
    session.install("-e", ".[dev]")
    session.run(
        "ty",
        "check",
        "--exclude",
        "duckdb_sqlalchemy/tests/**",
        "duckdb_sqlalchemy/",
    )
