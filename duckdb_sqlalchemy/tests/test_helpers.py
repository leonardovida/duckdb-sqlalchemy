import socket
import warnings
from urllib.parse import parse_qs

import pytest
import sqlalchemy
from packaging.version import Version
from pytest import raises
from sqlalchemy import create_engine, select

from duckdb_sqlalchemy import (
    URL,
    Dialect,
    make_url,
    pragma_storage_info,
    quack_query,
    read_csv_auto,
    read_parquet,
)
from duckdb_sqlalchemy.config import get_core_config


def test_url_helper_round_trip() -> None:
    url = URL(database=":memory:", read_only=True)
    assert url.drivername == "duckdb"
    assert url.database == ":memory:"
    assert str(url.query["read_only"]).lower() == "true"

    base = make_url(database=":memory:")
    rendered = base.render_as_string(hide_password=False)
    assert rendered.startswith("duckdb:///")


def test_url_helper_coerces_iterables_and_drops_none() -> None:
    url = URL(
        database=":memory:",
        extension=["httpfs", "parquet"],
        read_only=False,
        token=None,
    )
    rendered = url.render_as_string(hide_password=False)
    _, query = rendered.split("?", 1)
    assert parse_qs(query) == {
        "extension": ["httpfs", "parquet"],
        "read_only": ["false"],
    }


def test_read_parquet_helper() -> None:
    if Version(sqlalchemy.__version__) < Version("1.4.0"):
        with raises(NotImplementedError):
            read_parquet("data/events.parquet", columns=["event_id"])
        return

    parquet = read_parquet("data/events.parquet", columns=["event_id"])
    stmt = select(parquet.c.event_id).select_from(parquet)
    sql = str(stmt.compile(dialect=Dialect(), compile_kwargs={"literal_binds": True}))
    assert "read_parquet" in sql


def test_read_csv_auto_helper_executes_named_parameters(tmp_path) -> None:
    csv_path = tmp_path / "events.csv"
    csv_path.write_text("event_id\n1\n2\n")

    csv = read_csv_auto(str(csv_path), columns=["event_id"], header=True)
    stmt = select(csv.c.event_id).select_from(csv)

    engine = create_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        assert conn.execute(stmt).scalars().all() == [1, 2]


def test_pragma_storage_info_helper_executes_with_default_columns() -> None:
    storage_info = pragma_storage_info("events")
    stmt = select(storage_info.c.column_name, storage_info.c.segment_type).select_from(
        storage_info
    )

    engine = create_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        conn.exec_driver_sql("CREATE TABLE events AS SELECT 1 AS event_id")
        conn.commit()
        rows = conn.execute(stmt).all()

    assert ("event_id", "INTEGER") in rows


def test_pragma_storage_info_helper_compiles_include_segment_info() -> None:
    storage_info = pragma_storage_info("events", include_segment_info=True)
    stmt = select(storage_info.c.segment_info).select_from(storage_info)
    sql = str(stmt.compile(dialect=Dialect(), compile_kwargs={"literal_binds": True}))

    assert "pragma_storage_info" in sql
    assert '"include_segment_info" := true' in sql


def test_quack_query_helper_executes_against_local_server() -> None:
    duckdb = pytest.importorskip("duckdb", minversion="1.5.3")

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    uri = f"quack:127.0.0.1:{port}"
    token = "MY_QUACK_TOKEN_01234567890ABCDEF"
    server = duckdb.connect()
    server_started = False
    try:
        server.execute("CREATE TABLE hello AS SELECT 'world' AS value")
        try:
            server.execute("CALL quack_serve(?, token = ?)", [uri, token]).fetchall()
        except Exception as exc:
            pytest.skip(f"quack extension unavailable: {exc}")
        server_started = True

        remote = quack_query(
            uri,
            "SELECT value FROM hello",
            columns=["value"],
            token=token,
        )
        engine = create_engine("duckdb:///:memory:")
        with engine.connect() as conn:
            assert conn.execute(select(remote.c.value)).scalars().all() == ["world"]
    finally:
        if server_started:
            server.execute("CALL quack_stop(?)", [uri])
        server.close()


def test_motherduck_config_env_and_ttl(monkeypatch) -> None:
    get_core_config()

    captured = {}

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def register_filesystem(self, filesystem):
            return None

        def close(self):
            return None

    def fake_connect(*cargs, **cparams):
        captured.update(cparams)
        return DummyConn()

    import duckdb_sqlalchemy

    monkeypatch.setenv("motherduck_token", "token123")
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "connect", fake_connect)

    dialect = Dialect()
    dialect.connect(
        database="md:my_db",
        url_config={"dbinstance_inactivity_ttl": "1h"},
        config={},
    )

    assert captured["config"]["motherduck_token"] == "token123"
    database, query = captured["database"].split("?", 1)
    assert database == "md:my_db"
    assert parse_qs(query)["dbinstance_inactivity_ttl"] == ["1h"]
    assert "motherduck_dbinstance_inactivity_ttl" not in captured["config"]


def test_motherduck_oauth_token_stays_in_connect_config(monkeypatch) -> None:
    get_core_config()

    captured = {}

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def register_filesystem(self, filesystem):
            return None

        def close(self):
            return None

    def fake_connect(*cargs, **cparams):
        captured.update(cparams)
        return DummyConn()

    import duckdb_sqlalchemy

    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "connect", fake_connect)

    dialect = Dialect()
    dialect.connect(
        database="md:my_db",
        config={"oauth_token": "oauth123"},
    )

    assert captured["config"]["motherduck_oauth_token"] == "oauth123"
    assert "oauth_token" not in captured["config"]


def test_motherduck_legacy_transport_aliases_move_to_startup_url(monkeypatch) -> None:
    get_core_config()

    captured = {}

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def register_filesystem(self, filesystem):
            return None

        def close(self):
            return None

    def fake_connect(*cargs, **cparams):
        captured.update(cparams)
        return DummyConn()

    import duckdb_sqlalchemy

    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "connect", fake_connect)

    dialect = Dialect()
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        dialect.connect(
            database="md:my_db",
            config={
                "motherduck_host": "api.staging.motherduck.com",
                "motherduck_port": 443,
                "motherduck_use_tls": True,
                "threads": 4,
            },
        )

    database, query = captured["database"].split("?", 1)
    assert database == "md:my_db"
    assert parse_qs(query) == {
        "host": ["api.staging.motherduck.com"],
        "port": ["443"],
        "tls": ["true"],
    }
    assert captured["config"]["threads"] == 4
    assert "motherduck_host" not in captured["config"]
    assert "motherduck_port" not in captured["config"]
    assert "motherduck_use_tls" not in captured["config"]
    assert [str(w.message) for w in recorded] == [
        "`motherduck_host` is deprecated; use `host` instead.",
        "`motherduck_port` is deprecated; use `port` instead.",
        "`motherduck_use_tls` is deprecated; use `tls` instead.",
    ]
