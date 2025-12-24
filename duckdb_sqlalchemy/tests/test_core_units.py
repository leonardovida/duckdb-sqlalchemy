from typing import cast
from urllib.parse import parse_qs

import duckdb
import pytest
from sqlalchemy import Integer, String, pool
from sqlalchemy import exc as sa_exc

from duckdb_sqlalchemy import (
    URL,
    ConnectionWrapper,
    CursorWrapper,
    Dialect,
    MotherDuckURL,
    _apply_motherduck_defaults,
    _looks_like_motherduck,
    _normalize_motherduck_config,
    _supports,
    create_engine_from_paths,
    olap,
    stable_session_hint,
)
from duckdb_sqlalchemy import datatypes as dt
from duckdb_sqlalchemy.config import TYPES, apply_config, get_core_config


def _cursor(conn: object) -> CursorWrapper:
    wrapper = ConnectionWrapper(cast(duckdb.DuckDBPyConnection, conn))
    return CursorWrapper(cast(duckdb.DuckDBPyConnection, conn), wrapper)


def test_url_coerces_types_and_overrides() -> None:
    url = URL(
        database=":memory:",
        query={"access_mode": "read_only", "flag": True, "drop": None},
        access_mode="read_write",
        enabled=False,
        list_val=[1, "two"],
        tuple_val=("a", 2),
        empty=None,
    )

    assert url.query["access_mode"] == "read_write"
    assert url.query["flag"] == "true"
    assert url.query["enabled"] == "false"
    assert url.query["list_val"] == ("1", "two")
    assert url.query["tuple_val"] == ("a", "2")
    assert "drop" not in url.query
    assert "empty" not in url.query


def test_create_connect_args_moves_user_query_param() -> None:
    dialect = Dialect()
    url = URL(
        database="md:my_db",
        query={
            "user": "alice",
            "session_hint": "hint",
            "attach_mode": "single",
            "cache_buster": "123",
            "motherduck_dbinstance_inactivity_ttl": "15m",
            "memory_limit": "1GB",
        },
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ()
    database, query = kwargs["database"].split("?", 1)
    assert database == "md:my_db"
    assert parse_qs(query) == {
        "user": ["alice"],
        "session_hint": ["hint"],
        "attach_mode": ["single"],
        "cache_buster": ["123"],
        "dbinstance_inactivity_ttl": ["15m"],
    }
    assert kwargs["url_config"] == {"memory_limit": "1GB"}


def test_create_connect_args_strips_pool_override() -> None:
    dialect = Dialect()
    url = URL(
        database=":memory:",
        query={"duckdb_sqlalchemy_pool": "queue", "threads": "4"},
    )

    _, kwargs = dialect.create_connect_args(url)

    assert kwargs["url_config"] == {"threads": "4"}


def test_pool_override_from_url() -> None:
    url = URL(database="md:my_db", query={"duckdb_sqlalchemy_pool": "queue"})
    assert Dialect.get_pool_class(url) is pool.QueuePool


def test_create_engine_from_paths_requires_paths() -> None:
    with pytest.raises(ValueError):
        create_engine_from_paths([])


def test_is_disconnect_matches_patterns() -> None:
    dialect = Dialect()
    assert dialect.is_disconnect(RuntimeError("connection closed"), None, None)
    assert not dialect.is_disconnect(RuntimeError("syntax error"), None, None)


def test_retry_on_transient_select() -> None:
    dialect = Dialect()

    class DummyCursor:
        def __init__(self) -> None:
            self.calls = 0

        def execute(self, statement, parameters=None):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("HTTP Error: 503 Service Unavailable")
            return None

    class DummyContext:
        execution_options = {
            "duckdb_retry_on_transient": True,
            "duckdb_retry_count": 1,
        }

    cursor = DummyCursor()
    dialect.do_execute(cursor, "select 1", (), context=DummyContext())
    assert cursor.calls == 2


def test_motherduck_url_builder_moves_path_params() -> None:
    url = MotherDuckURL(
        database="md:my_db",
        session_hint="team-a",
        attach_mode="single",
        motherduck_dbinstance_inactivity_ttl="15m",
        query={"memory_limit": "1GB"},
    )

    database, query = url.database.split("?", 1)
    assert database == "md:my_db"
    assert parse_qs(query) == {
        "session_hint": ["team-a"],
        "attach_mode": ["single"],
        "dbinstance_inactivity_ttl": ["15m"],
    }
    assert url.query == {"memory_limit": "1GB"}


def test_stable_session_hint_is_deterministic() -> None:
    hint1 = stable_session_hint("user-123", salt="salt", length=8)
    hint2 = stable_session_hint("user-123", salt="salt", length=8)
    assert hint1 == hint2
    assert len(hint1) == 8


def test_apply_config_uses_literal_processors() -> None:
    dialect = Dialect()

    class DummyConn:
        def __init__(self) -> None:
            self.executed = []

        def execute(self, statement: str) -> None:
            self.executed.append(statement)

    conn = DummyConn()
    ext = {"memory_limit": "1GB", "threads": 4, "enable_profiling": True}

    apply_config(dialect, conn, ext)

    processors = {k: v.literal_processor(dialect=dialect) for k, v in TYPES.items()}
    expected = [
        f"SET {key} = {processors[type(value)](value)}" for key, value in ext.items()
    ]

    assert conn.executed == expected


def test_get_core_config_includes_motherduck_keys() -> None:
    core = get_core_config()

    expected = {
        "motherduck_token",
        "attach_mode",
        "saas_mode",
        "session_hint",
        "access_mode",
        "dbinstance_inactivity_ttl",
        "motherduck_dbinstance_inactivity_ttl",
    }
    assert expected.issubset(core)


def test_looks_like_motherduck_detection() -> None:
    assert _looks_like_motherduck("md:db", {}) is True
    assert _looks_like_motherduck("motherduck:db", {}) is True
    assert _looks_like_motherduck("local.db", {"motherduck_token": "x"}) is True
    assert _looks_like_motherduck("local.db", {}) is False


def test_apply_motherduck_defaults_env_token(monkeypatch: pytest.MonkeyPatch) -> None:
    config = {}
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "token123")
    monkeypatch.delenv("motherduck_token", raising=False)

    _apply_motherduck_defaults(config, "md:my_db")

    assert config["motherduck_token"] == "token123"


def test_apply_motherduck_defaults_skips_non_md(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = {}
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "token123")
    monkeypatch.delenv("motherduck_token", raising=False)

    _apply_motherduck_defaults(config, "local.db")

    assert "motherduck_token" not in config


def test_apply_motherduck_defaults_requires_string() -> None:
    with pytest.raises(TypeError):
        _apply_motherduck_defaults({"motherduck_token": 123}, "md:db")


def test_normalize_motherduck_config_alias() -> None:
    config = {"dbinstance_inactivity_ttl": "1h"}
    _normalize_motherduck_config(config)
    assert config["motherduck_dbinstance_inactivity_ttl"] == "1h"

    config = {
        "dbinstance_inactivity_ttl": "1h",
        "motherduck_dbinstance_inactivity_ttl": "2h",
    }
    _normalize_motherduck_config(config)
    assert config["motherduck_dbinstance_inactivity_ttl"] == "2h"


def test_has_comment_support_false_on_parser_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _supports.has_comment_support.cache_clear()

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *args, **kwargs):
            raise _supports.duckdb.ParserException("nope")

    monkeypatch.setattr(_supports.duckdb, "connect", lambda *_: DummyConn())

    assert _supports.has_comment_support() is False


def test_has_comment_support_true_when_no_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _supports.has_comment_support.cache_clear()

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *args, **kwargs):
            return None

    monkeypatch.setattr(_supports.duckdb, "connect", lambda *_: DummyConn())

    assert _supports.has_comment_support() is True


def test_identifier_preparer_separate_and_format_schema() -> None:
    preparer = Dialect().identifier_preparer

    assert preparer._separate("db.schema") == ("db", "schema")
    assert preparer._separate('"my db"."my schema"') == ("my db", "my schema")
    assert preparer._separate("schema") == (None, "schema")

    formatted = preparer.format_schema('"my db".main')
    assert formatted == '"my db".main'


def test_table_function_error_without_table_valued(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyFunc:
        def __getattr__(self, name):
            def _fn(*args, **kwargs):
                return object()

            return _fn

    monkeypatch.setattr(olap, "func", DummyFunc())

    with pytest.raises(NotImplementedError):
        olap.table_function("read_parquet", "path", columns=["col"])


def test_table_function_returns_function_call(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {}

    class DummyFunc:
        def __getattr__(self, name):
            def _fn(*args, **kwargs):
                called["name"] = name
                called["args"] = args
                called["kwargs"] = kwargs
                return "ok"

            return _fn

    monkeypatch.setattr(olap, "func", DummyFunc())

    result = olap.table_function("read_csv", "file.csv", header=True)

    assert result == "ok"
    assert called["name"] == "read_csv"
    assert called["args"] == ("file.csv",)
    assert called["kwargs"] == {"header": True}


def test_cursorwrapper_execute_basic_paths() -> None:
    class DummyConn:
        def __init__(self) -> None:
            self.calls = []

        def commit(self) -> None:
            self.calls.append(("commit",))

        def register(self, name, df) -> None:
            self.calls.append(("register", name, df))

        def execute(self, *args):
            self.calls.append(("execute", args))

    conn = DummyConn()
    cursor = _cursor(conn)
    df = object()

    cursor.execute("COMMIT")
    cursor.execute("register", ("view", df))
    cursor.execute("select 1")
    cursor.execute("select ?", (1,))

    assert ("commit",) in conn.calls
    assert ("register", "view", df) in conn.calls
    assert ("execute", ("select 1",)) in conn.calls
    assert ("execute", ("select ?", (1,))) in conn.calls


def test_cursorwrapper_execute_show_isolation_level() -> None:
    class DummyConn:
        def __init__(self) -> None:
            self.calls = []

        def execute(self, statement: str) -> None:
            self.calls.append(statement)

    conn = DummyConn()
    cursor = _cursor(conn)

    cursor.execute("show transaction isolation level")

    assert conn.calls == ["select 'read committed' as transaction_isolation"]


def test_cursorwrapper_executemany_coerces_to_list() -> None:
    class DummyConn:
        def __init__(self) -> None:
            self.calls = []

        def executemany(self, *args):
            self.calls.append(args)

    conn = DummyConn()
    cursor = _cursor(conn)
    params = ({"a": 1}, {"a": 2})

    cursor.executemany("insert", params)  # type: ignore[arg-type]

    assert conn.calls[0][0] == "insert"
    assert conn.calls[0][1] == list(params)


def test_cursorwrapper_execute_handles_specific_runtime_errors() -> None:
    class CommitConn:
        def commit(self) -> None:
            raise RuntimeError(
                "TransactionContext Error: cannot commit - no transaction is active"
            )

    cursor = _cursor(CommitConn())
    cursor.execute("commit")

    class NotImplementedConn:
        def execute(self, *args, **kwargs):
            raise RuntimeError("Not implemented Error: nope")

    cursor = _cursor(NotImplementedConn())
    with pytest.raises(NotImplementedError):
        cursor.execute("select 1")


def test_cursorwrapper_description_handles_unhashable_type_code() -> None:
    class DummyConn:
        description = [("col", ["complex"])]

    cursor = _cursor(DummyConn())

    assert cursor.description == [("col", "['complex']")]


def test_connectionwrapper_close_marks_closed() -> None:
    class DummyConn:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    conn = DummyConn()
    wrapper = ConnectionWrapper(cast(duckdb.DuckDBPyConnection, conn))
    assert wrapper.closed is False

    wrapper.close()

    assert wrapper.closed is True
    assert conn.closed is True


def test_map_processors(monkeypatch: pytest.MonkeyPatch) -> None:
    map_type = dt.Map(String, Integer)
    bind = map_type.bind_processor(Dialect())

    assert bind({"a": 1, "b": 2}) == {"key": ["a", "b"], "value": [1, 2]}
    assert bind(None) is None

    result = map_type.result_processor(Dialect(), None)({"key": ["a"], "value": [1]})
    if dt.IS_GT_1:
        assert result == {"key": ["a"], "value": [1]}
    else:
        assert result == {"a": 1}

    monkeypatch.setattr(dt, "IS_GT_1", False)
    legacy = dt.Map(String, Integer).result_processor(Dialect(), None)
    assert legacy({"key": ["a"], "value": [1]}) == {"a": 1}
    assert legacy(None) == {}


def test_struct_or_union_requires_fields() -> None:
    dialect = Dialect()
    compiler = dialect.type_compiler
    preparer = dialect.identifier_preparer

    with pytest.raises(sa_exc.CompileError):
        dt.struct_or_union(dt.Struct(), compiler, preparer)

    struct = dt.Struct({"first name": String, "age": Integer})
    rendered = dt.struct_or_union(struct, compiler, preparer)
    assert rendered.startswith("(")
    assert rendered.endswith(")")
    assert '"first name"' in rendered
