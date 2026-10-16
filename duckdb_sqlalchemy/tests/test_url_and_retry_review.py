"""Credential redaction, path-param precedence and retry guard edge cases."""

from typing import Any, Dict, List
from urllib.parse import parse_qs

import pytest

import duckdb_sqlalchemy
from duckdb_sqlalchemy import (
    Dialect,
    create_engine_from_paths,
    create_motherduck_engine,
)
from duckdb_sqlalchemy._statements import _is_idempotent_statement, _top_level_sql_words
from duckdb_sqlalchemy.config import get_core_config


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


def _connect(engine: Any) -> None:
    # Call the pool's creator directly, without SQLAlchemy's first-connect
    # queries, which the recording connection cannot answer.
    engine.pool._creator()


def test_engine_helpers_keep_tokens_out_of_the_engine_url(
    captured_connect: Dict[str, Any],
) -> None:
    engine = create_motherduck_engine(
        database="md:db", motherduck_token="SECRET1", attach_mode="single"
    )
    assert "SECRET1" not in repr(engine)
    assert "SECRET1" not in engine.url.render_as_string(hide_password=False)
    _connect(engine)
    assert captured_connect["config"]["motherduck_token"] == "SECRET1"
    assert captured_connect["database"] == "md:db?attach_mode=single"

    paths = create_engine_from_paths(["md:db?motherduck_token=SECRET2"])
    assert "SECRET2" not in repr(paths)
    _connect(paths)
    assert captured_connect["database"] == "md:db"
    assert captured_connect["config"]["motherduck_token"] == "SECRET2"


def test_url_path_params_win_over_connect_args(
    captured_connect: Dict[str, Any],
) -> None:
    from sqlalchemy import create_engine

    engine = create_engine(
        "duckdb:///md:db?session_name=url&memory_limit=1GB",
        connect_args={"config": {"session_name": "ca", "memory_limit": "2GB"}},
    )
    _connect(engine)
    assert captured_connect["database"] == "md:db?session_name=url"
    assert captured_connect["config"]["memory_limit"] == "1GB"


def test_path_params_give_one_instance_cache_key(
    captured_connect: Dict[str, Any],
) -> None:
    databases = []
    for database in ("md:db?user=a&host=h", "md:db?host=h&user=a"):
        Dialect().connect(database=database)
        databases.append(captured_connect["database"])
    assert databases == ["md:db?host=h&user=a"] * 2


def test_bare_paths_split_their_query_string(captured_connect: Dict[str, Any]) -> None:
    engine = create_engine_from_paths(["local.db?threads=2", "md:db?session_name=s"])
    _connect(engine)
    assert captured_connect["database"] == "local.db"
    assert captured_connect["config"]["threads"] == "2"
    _connect(engine)
    assert parse_qs(captured_connect["database"].partition("?")[2]) == {
        "session_name": ["s"]
    }


def test_motherduck_settings_on_a_local_database_are_set_after_preload(
    captured_connect: Dict[str, Any],
) -> None:
    Dialect().connect(
        database=":memory:",
        preload_extensions=["motherduck"],
        config={"motherduck_token": "abc"},
    )
    assert "motherduck_token" not in captured_connect["config"]
    statements = captured_connect["conn"].statements
    assert statements.index("LOAD motherduck") < statements.index(
        "SET motherduck_token = 'abc'"
    )


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT $ä$'$ä$; INSERT INTO t VALUES (1); SELECT '$ä$'",
        "SELECT a$b FROM t",
        "EXPLAIN ANALYSE INSERT INTO t VALUES (2)",
        "SELECT * FROM ducklake_add_data_files('lake', 't', 'f.parquet')",
        "SELECT * FROM postgres_execute('db', 'DELETE FROM t')",
        "SELECT * FROM mysql_execute('db', 'DELETE FROM t')",
    ],
)
def test_retry_guard_declines_statements_with_side_effects(statement: str) -> None:
    assert not _is_idempotent_statement(statement)


def test_retry_guard_still_accepts_placeholders() -> None:
    assert _is_idempotent_statement("SELECT $1, $2")


def test_top_level_words_skip_non_ascii_dollar_strings() -> None:
    assert _top_level_sql_words("SELECT $ä$ insert $ä$ FROM t") == [
        "select",
        "from",
        "t",
    ]
