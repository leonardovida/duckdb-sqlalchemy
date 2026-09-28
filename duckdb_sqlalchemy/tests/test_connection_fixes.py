"""Regression tests for connection URL, pooling, and token handling fixes."""

from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs

import duckdb
import pytest
from sqlalchemy import create_engine, pool, text

import duckdb_sqlalchemy
from duckdb_sqlalchemy import (
    URL,
    Dialect,
    DuckDBEngineWarning,
)
from duckdb_sqlalchemy.url import make_url

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


# 4. Empty database is :memory: and shares one connection.


@pytest.mark.parametrize("url", ["duckdb://", "duckdb:///"])
def test_empty_database_uses_singleton_pool(url: str) -> None:
    engine = create_engine(url)
    try:
        assert isinstance(engine.pool, pool.SingletonThreadPool)
        with engine.connect() as conn:
            conn.execute(text("create table shared as select 1 as i"))
            conn.commit()
        with engine.connect() as conn:
            assert conn.execute(text("select i from shared")).scalar() == 1
    finally:
        engine.dispose()


# 5. Pool override aliases and unknown values.


def test_queuepool_override_alias() -> None:
    url = URL(database="md:my_db", query={"duckdb_sqlalchemy_pool": "QueuePool"})
    assert Dialect.get_pool_class(url) is pool.QueuePool


def test_unknown_pool_override_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DUCKDB_SQLALCHEMY_POOL", raising=False)
    url = URL(database="md:my_db", query={"pool": "bogus"})

    with pytest.warns(DuckDBEngineWarning, match="'bogus'") as recorded:
        assert Dialect.get_pool_class(url) is pool.NullPool

    message = str(recorded[0].message)
    for valid in ("null", "nullpool", "queue", "queuepool", "singleton"):
        assert valid in message


def test_unknown_pool_override_from_env_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DUCKDB_SQLALCHEMY_POOL", "bogus")

    with pytest.warns(DuckDBEngineWarning, match="Valid values"):
        assert Dialect.get_pool_class(URL(database="local.db")) is pool.QueuePool


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


# 10. duckdb_sqlalchemy.make_url is deprecated.


def test_make_url_is_deprecated_but_still_works() -> None:
    with pytest.warns(DeprecationWarning, match="duckdb_sqlalchemy.URL"):
        url = make_url(database="local.db", threads=4)

    assert url == URL(database="local.db", threads=4)
    assert duckdb_sqlalchemy.make_url is make_url
