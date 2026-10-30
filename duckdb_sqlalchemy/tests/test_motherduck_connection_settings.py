"""MotherDuck connection settings: connect timeouts, intervals, session SETs."""

import warnings
from datetime import timedelta
from typing import Any, Dict, List
from urllib.parse import parse_qs

import pytest
from sqlalchemy.engine import make_url as sa_make_url

import duckdb_sqlalchemy
from duckdb_sqlalchemy import Dialect, MotherDuckURL
from duckdb_sqlalchemy import motherduck as md
from duckdb_sqlalchemy._query import format_interval
from duckdb_sqlalchemy.config import apply_config, get_core_config


class _RecordingConn:
    def __init__(self) -> None:
        self.statements: List[str] = []

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> "_RecordingConn":
        self.statements.append(statement)
        return self

    def close(self) -> None:
        pass


@pytest.fixture
def captured_connect(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    captured: Dict[str, Any] = {}
    get_core_config()  # cached; it opens a real in-memory connection once

    def fake_connect(*cargs: Any, **cparams: Any) -> _RecordingConn:
        conn = _RecordingConn()
        captured.clear()
        captured.update(cparams, conn=conn)
        return conn

    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "connect", fake_connect)
    return captured


def _database_query(database: str) -> Dict[str, List[str]]:
    return parse_qs(database.partition("?")[2])


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (timedelta(seconds=30), "30s"),
        (timedelta(minutes=15), "900s"),
        (timedelta(milliseconds=1500), "1500ms"),
        (timedelta(microseconds=250), "250us"),
        (timedelta(0), "0s"),
        (timedelta(seconds=-5), "-5s"),
    ],
)
def test_format_interval_renders_duckdb_interval_strings(
    value: timedelta, expected: str
) -> None:
    assert format_interval(value) == expected


def test_format_interval_round_trips_through_duckdb() -> None:
    import duckdb

    for value in (timedelta(seconds=30), timedelta(milliseconds=1500)):
        rendered = format_interval(value)
        [(parsed,)] = duckdb.sql(f"SELECT '{rendered}'::INTERVAL").fetchall()
        assert parsed == value


def test_motherduck_url_routes_connection_timeout_into_database_string() -> None:
    url = MotherDuckURL(
        database="md:analytics",
        connection_timeout=timedelta(seconds=30),
        dbinstance_inactivity_ttl=timedelta(minutes=5),
    )

    assert url.query == {
        "connection_timeout": "30s",
        "dbinstance_inactivity_ttl": "300s",
    }
    _, kwargs = Dialect().create_connect_args(url)
    assert _database_query(kwargs["database"]) == {
        "connection_timeout": ["30s"],
        "dbinstance_inactivity_ttl": ["300s"],
    }
    assert kwargs["url_config"] == {}


def test_url_string_connection_timeout_is_a_startup_parameter() -> None:
    url = sa_make_url("duckdb:///md:analytics?connection_timeout=10s&threads=4")

    _, kwargs = Dialect().create_connect_args(url)

    assert kwargs["database"] == "md:analytics?connection_timeout=10s"
    assert kwargs["url_config"] == {"threads": "4"}


def test_prefixed_connection_timeout_config_moves_without_warning(
    captured_connect: Dict[str, Any],
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Dialect().connect(
            database="md:analytics",
            config={"motherduck_connection_timeout": "45s", "threads": 2},
        )

    assert captured_connect["database"] == "md:analytics?connection_timeout=45s"
    assert "motherduck_connection_timeout" not in captured_connect["config"]
    assert not any(
        "connection_timeout" in statement
        for statement in captured_connect["conn"].statements
    )


def test_connection_timeout_stays_config_for_local_databases() -> None:
    url = sa_make_url("duckdb:///local.db?connection_timeout=10s")

    _, kwargs = Dialect().create_connect_args(url)

    assert kwargs["database"] == "local.db"
    assert kwargs["url_config"] == {"connection_timeout": "10s"}


@pytest.mark.parametrize(
    "url",
    [
        "duckdb:///md:analytics?motherduck_enable_server_side_temp_tables=true",
        MotherDuckURL(
            database="md:analytics", motherduck_enable_server_side_temp_tables=True
        ),
    ],
)
def test_server_side_temp_tables_are_set_on_each_connection(
    captured_connect: Dict[str, Any], url: Any
) -> None:
    dialect = Dialect()
    cargs, cparams = dialect.create_connect_args(sa_make_url(str(url)))

    dialect.connect(*cargs, **cparams)

    # The extension ignores this key in the ``md:`` string and refuses it as
    # global config, so it must arrive as a session SET after connecting.
    assert captured_connect["database"] == "md:analytics"
    assert "motherduck_enable_server_side_temp_tables" not in captured_connect["config"]
    assert captured_connect["conn"].statements == [
        "SET motherduck_enable_server_side_temp_tables = 'true'"
    ]


def test_core_config_routes_connection_timeout_but_not_temp_tables() -> None:
    core = get_core_config()

    assert {"connection_timeout", "motherduck_connection_timeout"} <= core
    # A connection-level setting: it must reach the dialect's SET step.
    assert md.MOTHERDUCK_ENABLE_SERVER_SIDE_TEMP_TABLES_KEY not in core


def test_server_side_temp_tables_key_is_not_a_path_parameter() -> None:
    config = {md.MOTHERDUCK_ENABLE_SERVER_SIDE_TEMP_TABLES_KEY: True}

    assert md.extract_path_query_from_config(config) == {}
    assert config == {md.MOTHERDUCK_ENABLE_SERVER_SIDE_TEMP_TABLES_KEY: True}


def test_apply_config_renders_timedelta_as_interval() -> None:
    conn = _RecordingConn()

    apply_config(
        Dialect(),
        conn,
        {"motherduck_background_catalog_refresh_inactivity_timeout": timedelta(1)},
    )

    assert conn.statements == [
        "SET motherduck_background_catalog_refresh_inactivity_timeout = '86400s'"
    ]


def test_connect_type_error_does_not_echo_the_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sqlalchemy import create_engine

    monkeypatch.setenv("MOTHERDUCK_TOKEN", "dummy-secret-token")
    engine = create_engine(
        "duckdb:///md:my_db", connect_args={"check_same_thread": False}
    )
    with pytest.raises(TypeError) as raised:
        engine.connect()
    message = str(raised.value)
    assert "dummy-secret-token" not in message
    assert "check_same_thread" in message
    engine.dispose()


@pytest.mark.parametrize(
    "url", ["duckdb://somehost/md:my_db", "duckdb://user:secret@/local.duckdb"]
)
def test_urls_with_host_or_credentials_are_rejected(url: str) -> None:
    from sqlalchemy import create_engine
    from sqlalchemy.exc import ArgumentError

    with pytest.raises(ArgumentError, match="three slashes") as raised:
        create_engine(url)
    assert "secret" not in str(raised.value)


def test_token_on_a_local_database_is_set_under_the_extension_name(
    captured_connect: Dict[str, Any],
) -> None:
    dialect = Dialect()
    dialect.connect(
        database="local.duckdb",
        config={"token": "dummy", "dbinstance_inactivity_ttl": "5m"},
    )
    statements = captured_connect["conn"].statements
    assert "SET motherduck_token = 'dummy'" in statements
    assert not any(" token " in s or "SET dbinstance" in s for s in statements)


def test_motherduck_url_requires_a_motherduck_database() -> None:
    with pytest.raises(ValueError, match="md:analytics"):
        MotherDuckURL(database="analytics", attach_mode="single")
    with pytest.raises(ValueError, match="md:"):
        md.create_motherduck_engine(database="my_db")


def test_motherduck_url_keyword_alias_overrides_embedded_query() -> None:
    with pytest.warns(DeprecationWarning, match="motherduck_session_name"):
        url = MotherDuckURL(
            database="md:db?session_name=embedded",
            motherduck_session_name="kwarg",
        )
    assert url.query == {"session_name": "kwarg"}


def test_create_motherduck_engine_forwards_engine_arguments() -> None:
    from sqlalchemy.pool import StaticPool

    pool = StaticPool(lambda: None)
    engine = md.create_motherduck_engine(database="md:db", future=True, pool=pool)
    assert engine.pool is pool
    assert dict(engine.url.query) == {}


def test_create_motherduck_engine_forwards_skip_autocommit_rollback() -> None:
    from sqlalchemy.engine.default import DefaultDialect

    if not hasattr(DefaultDialect(), "skip_autocommit_rollback"):
        pytest.skip("skip_autocommit_rollback needs SQLAlchemy 2.0.43 or newer")
    engine = md.create_motherduck_engine(
        database="md:db", skip_autocommit_rollback=True
    )
    assert dict(engine.url.query) == {}
    assert engine.dialect.skip_autocommit_rollback is True
