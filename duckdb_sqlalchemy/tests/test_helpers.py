from urllib.parse import parse_qs

import sqlalchemy
from packaging.version import Version
from pytest import raises
from sqlalchemy import select

from duckdb_sqlalchemy import URL, Dialect, make_url, read_parquet
from duckdb_sqlalchemy.config import get_core_config


def test_url_helper_round_trip() -> None:
    url = URL(database=":memory:", read_only=True)
    assert url.drivername == "duckdb"
    assert url.database == ":memory:"
    assert str(url.query["read_only"]).lower() == "true"

    base = make_url(database=":memory:")
    rendered = base.render_as_string(hide_password=False)
    assert rendered.startswith("duckdb:///")


def test_read_parquet_helper() -> None:
    if Version(sqlalchemy.__version__) < Version("1.4.0"):
        with raises(NotImplementedError):
            read_parquet("data/events.parquet", columns=["event_id"])
        return

    parquet = read_parquet("data/events.parquet", columns=["event_id"])
    stmt = select(parquet.c.event_id).select_from(parquet)
    sql = str(stmt.compile(dialect=Dialect(), compile_kwargs={"literal_binds": True}))
    assert "read_parquet" in sql


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
