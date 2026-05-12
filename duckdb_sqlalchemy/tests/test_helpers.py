import warnings
from urllib.parse import parse_qs

import sqlalchemy
from packaging.version import Version
from pytest import raises
from sqlalchemy import create_engine, select

from duckdb_sqlalchemy import URL, Dialect, make_url, read_csv_auto, read_parquet
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
