from contextlib import contextmanager
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


# TODO: "0.5.1", "0.6.1", "0.7.1", "0.8.1"
@nox.session(py=["3.10", "3.11", "3.12", "3.13"])
@nox.parametrize(
    "duckdb",
    [
        "1.1.3",
        "1.2.0",
        "1.2.1",
        "1.2.2",
        "1.3.0",
        "1.3.1",
        "1.3.2",
        "1.4.0",
        "1.4.1",
        "1.4.2",
        "1.4.3",
    ],
)
@nox.parametrize("sqlalchemy", ["1.3", "1.4", "2.0.45"])
def tests(session: nox.Session, duckdb: str, sqlalchemy: str) -> None:
    tests_core(session, duckdb, sqlalchemy)


@nox.session(py=["3.9"])
def nightly(session: nox.Session) -> None:
    session.skip("DuckDB nightly installs are broken right now")
    tests_core(session, "master", "1.4")


def tests_core(session: nox.Session, duckdb: str, sqlalchemy: str) -> None:
    if session.python is not None:
        major, minor = (int(part) for part in session.python.split(".")[:2])
        if (major, minor) >= (3, 12) and sqlalchemy == "1.3":
            session.skip("SQLAlchemy 1.3 is not supported on Python >= 3.12")
        if (major, minor) >= (3, 13) and sqlalchemy == "1.4":
            session.skip("SQLAlchemy 1.4 is not supported on Python >= 3.13")
    with group(f"{session.name} - Install"):
        session.install("-e", ".[dev]")
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


@nox.session(py=["3.11"])
def ty(session: nox.Session) -> None:
    session.install("-e", ".[dev]")
    session.run("ty", "check", "duckdb_sqlalchemy/")
