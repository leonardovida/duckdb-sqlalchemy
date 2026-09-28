"""Regression tests for connection URL, pooling, and token handling fixes."""

from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs

import duckdb
import pytest

import duckdb_sqlalchemy
from duckdb_sqlalchemy import (
    Dialect,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"
requires_examples = pytest.mark.skipif(
    not EXAMPLES_DIR.is_dir(), reason="examples directory not available"
)


def _query(database: str) -> Dict[str, Any]:
    _, _, query = database.partition("?")
    return parse_qs(query)


class _DummyConn:
    def __init__(self, fail_on: str = "") -> None:
        self.fail_on = fail_on
        self.closed = False

    def execute(self, *args: Any, **kwargs: Any) -> "_DummyConn":
        if self.fail_on == "execute":
            raise duckdb.IOException("boom")
        return self

    def register_filesystem(self, filesystem: Any) -> None:
        if self.fail_on == "register_filesystem":
            raise RuntimeError("boom")

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def captured_connect(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    captured: Dict[str, Any] = {}

    def fake_connect(*cargs: Any, **cparams: Any) -> _DummyConn:
        captured.clear()
        captured.update(cparams)
        return _DummyConn()

    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "connect", fake_connect)
    return captured


# 9. Connection setup failures close the DuckDB connection.


@pytest.mark.parametrize("fail_on", ["execute", "register_filesystem"])
def test_connect_closes_connection_on_setup_failure(
    monkeypatch: pytest.MonkeyPatch, fail_on: str
) -> None:
    conns = []

    def fake_connect(*cargs: Any, **cparams: Any) -> _DummyConn:
        conn = _DummyConn(fail_on=fail_on)
        conns.append(conn)
        return conn

    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "connect", fake_connect)

    with pytest.raises(Exception, match="boom"):
        Dialect().connect(
            database=":memory:",
            preload_extensions=["json"],
            register_filesystems=[object()],
        )

    assert len(conns) == 1
    assert conns[0].closed is True


def test_connect_releases_file_when_config_fails(tmp_path: Path) -> None:
    db_path = str(tmp_path / "locked.db")
    duckdb.connect(db_path).close()

    with pytest.raises(duckdb.Error) as excinfo:
        Dialect().connect(database=db_path, config={"not_a_real_setting": "x"})

    # Keep the traceback (and so any leaked connection) alive: a leaked
    # connection holds the database instance and file lock, and DuckDB rejects
    # reopening it with a different configuration.
    conn = duckdb.connect(db_path, config={"access_mode": "read_only"})
    conn.close()
    assert excinfo.value is not None
