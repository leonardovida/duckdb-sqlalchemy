import importlib.metadata
import warnings
from pathlib import Path
from typing import Any, Optional, Tuple, cast
from urllib.parse import parse_qs

import duckdb
import pytest
from sqlalchemy import Integer, String, create_engine, pool, text
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import URL as SAURL

import duckdb_sqlalchemy
from duckdb_sqlalchemy import (
    URL,
    ConnectionWrapper,
    CursorWrapper,
    Dialect,
    MotherDuckURL,
    _apply_motherduck_defaults,
    _is_idempotent_statement,
    _is_transient_error,
    _looks_like_motherduck,
    _normalize_execution_options,
    _normalize_motherduck_config,
    _parse_register_params,
    _pool_override_from_url,
    _supports,
    create_engine_from_paths,
    olap,
    stable_session_hint,
    stable_session_name,
)
from duckdb_sqlalchemy import datatypes as dt
from duckdb_sqlalchemy import motherduck as md
from duckdb_sqlalchemy.bulk import copy_from_csv, copy_from_rows
from duckdb_sqlalchemy.config import TYPES, ConfigValue, apply_config, get_core_config


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
            "host": "custom.motherduck.com",
            "region_host": "us-east-1.aws.motherduck.com",
            "port": 443,
            "tls": False,
            "grpc_local_subchannel_pool": True,
            "session_name": "hint",
            "attach_mode": "single",
            "cache_buster": "123",
            "motherduck_dbinstance_inactivity_ttl": "15m",
            "memory_limit": "1GB",
        },
    )

    with pytest.warns(DeprecationWarning, match="dbinstance_inactivity_ttl"):
        args, kwargs = dialect.create_connect_args(url)

    assert args == ()
    database, query = kwargs["database"].split("?", 1)
    assert database == "md:my_db"
    assert parse_qs(query) == {
        "user": ["alice"],
        "host": ["custom.motherduck.com"],
        "region_host": ["us-east-1.aws.motherduck.com"],
        "port": ["443"],
        "tls": ["false"],
        "grpc_local_subchannel_pool": ["true"],
        "session_name": ["hint"],
        "attach_mode": ["single"],
        "cache_buster": ["123"],
        "dbinstance_inactivity_ttl": ["15m"],
    }
    assert kwargs["url_config"] == {"memory_limit": "1GB"}


def test_connect_keeps_token_in_config_and_moves_transport_options(monkeypatch) -> None:
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

    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "connect", fake_connect)
    monkeypatch.setattr(
        duckdb_sqlalchemy, "get_core_config", lambda: {"threads", "token"}
    )

    dialect = Dialect()
    dialect.connect(
        database="md:my_db",
        config={
            "host": "custom.motherduck.com",
            "region_host": "us-east-1.aws.motherduck.com",
            "port": 443,
            "tls": False,
            "grpc_local_subchannel_pool": True,
            "token": "legacy-token",
            "threads": 4,
        },
    )

    database, query = captured["database"].split("?", 1)
    assert database == "md:my_db"
    assert parse_qs(query) == {
        "host": ["custom.motherduck.com"],
        "region_host": ["us-east-1.aws.motherduck.com"],
        "port": ["443"],
        "tls": ["false"],
        "grpc_local_subchannel_pool": ["true"],
    }
    assert captured["config"]["token"] == "legacy-token"
    assert captured["config"]["threads"] == 4
    assert captured["config"]["custom_user_agent"].startswith("duckdb-sqlalchemy/1.5.2")


def test_create_connect_args_defaults_to_memory_before_path_query() -> None:
    dialect = Dialect()
    url = SAURL.create("duckdb", query={"access_mode": "read_only", "threads": "4"})

    args, kwargs = dialect.create_connect_args(url)

    assert args == ()
    assert kwargs["database"] == ":memory:?access_mode=read_only"
    assert kwargs["url_config"] == {"threads": "4"}


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
    with pytest.warns(DeprecationWarning, match="dbinstance_inactivity_ttl"):
        url = MotherDuckURL(
            database="md:my_db",
            session_name="team-a",
            attach_mode="single",
            motherduck_dbinstance_inactivity_ttl="15m",
            query={"memory_limit": "1GB"},
        )

    assert url.database is not None
    database, query = url.database.split("?", 1)
    assert database == "md:my_db"
    assert parse_qs(query) == {
        "session_name": ["team-a"],
        "attach_mode": ["single"],
        "dbinstance_inactivity_ttl": ["15m"],
    }
    assert url.query == {"memory_limit": "1GB"}


def test_stable_session_name_is_deterministic() -> None:
    name1 = stable_session_name("user-123", salt="salt", length=8)
    name2 = stable_session_name("user-123", salt="salt", length=8)
    assert name1 == name2
    assert len(name1) == 8


def test_stable_session_hint_warns_and_delegates() -> None:
    with pytest.warns(DeprecationWarning, match="stable_session_name"):
        hint = stable_session_hint("user-123", salt="salt", length=8)

    assert hint == stable_session_name("user-123", salt="salt", length=8)


def test_runtime_version_matches_installed_metadata() -> None:
    assert duckdb_sqlalchemy.__version__ == importlib.metadata.version(
        "duckdb-sqlalchemy"
    )


def test_apply_config_uses_literal_processors() -> None:
    dialect = Dialect()

    class DummyConn:
        def __init__(self) -> None:
            self.executed = []

        def execute(self, statement: str) -> None:
            self.executed.append(statement)

    conn = DummyConn()
    ext: dict[str, ConfigValue] = {
        "memory_limit": "1GB",
        "threads": 4,
        "enable_profiling": True,
    }

    apply_config(dialect, conn, ext)

    processors = {k: v.literal_processor(dialect=dialect) for k, v in TYPES.items()}
    expected = []
    for key, value in ext.items():
        processor = processors[type(value)]
        assert processor is not None
        expected.append(f"SET {key} = {processor(value)}")

    assert conn.executed == expected


@pytest.mark.parametrize("factory", [dt.Struct, dt.Union])
def test_fields_types_share_field_normalization_and_cache_key(factory: Any) -> None:
    field_items = [("name", String), ("age", Integer)]

    field_type = factory(field_items)

    assert field_type.fields == field_items
    assert field_type._fields is not None
    assert [key for key, _ in field_type._fields] == ["name", "age"]
    assert isinstance(field_type._fields[0][1], String)
    assert isinstance(field_type._fields[1][1], Integer)
    assert field_type._static_cache_key == (
        type(field_type),
        (
            "fields",
            (
                ("name", String()._static_cache_key),
                ("age", Integer()._static_cache_key),
            ),
        ),
    )


def test_get_core_config_includes_motherduck_keys() -> None:
    core = get_core_config()

    expected = {
        "token",
        "motherduck_token",
        "motherduck_oauth_token",
        "oauth_token",
        "motherduck_host",
        "host",
        "motherduck_region_host",
        "region_host",
        "motherduck_port",
        "port",
        "motherduck_use_tls",
        "tls",
        "motherduck_grpc_local_subchannel_pool",
        "grpc_local_subchannel_pool",
        "slt",
        "attach_mode",
        "saas_mode",
        "session_name",
        "motherduck_session_name",
        "session_hint",
        "motherduck_session_hint",
        "access_mode",
        "dbinstance_inactivity_ttl",
        "motherduck_dbinstance_inactivity_ttl",
    }
    assert expected.issubset(core)


def test_looks_like_motherduck_detection() -> None:
    assert _looks_like_motherduck("md:db", {}) is True
    assert _looks_like_motherduck("motherduck:db", {}) is True
    assert _looks_like_motherduck("local.db", {"token": "x"}) is True
    assert _looks_like_motherduck("local.db", {"motherduck_token": "x"}) is True
    assert _looks_like_motherduck("local.db", {"motherduck_oauth_token": "x"}) is True
    assert (
        _looks_like_motherduck("local.db", {"motherduck_host": "api.motherduck.com"})
        is True
    )
    assert _looks_like_motherduck("local.db", {"host": "custom.motherduck.com"}) is True
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
    with pytest.warns(DeprecationWarning, match="dbinstance_inactivity_ttl"):
        _normalize_motherduck_config(config)
    assert config["motherduck_dbinstance_inactivity_ttl"] == "2h"


def test_normalize_motherduck_config_warns_on_deprecated_alias() -> None:
    config = {"motherduck_dbinstance_inactivity_ttl": "1h"}

    with pytest.warns(DeprecationWarning, match="dbinstance_inactivity_ttl"):
        _normalize_motherduck_config(config)

    assert config["motherduck_dbinstance_inactivity_ttl"] == "1h"


def test_normalize_motherduck_config_normalizes_oauth_alias() -> None:
    config = {"oauth_token": "oauth123"}

    _normalize_motherduck_config(config)

    assert config == {"motherduck_oauth_token": "oauth123"}


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


def test_identifier_preparer_reserved_words_are_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    duckdb_sqlalchemy._get_reserved_words.cache_clear()
    calls = 0

    class DummyCursor:
        def execute(self, statement: str):
            nonlocal calls
            calls += 1
            return self

        def fetchall(self):
            return [("select",)]

    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "cursor", lambda: DummyCursor())

    Dialect().identifier_preparer
    Dialect().identifier_preparer

    assert calls == 1


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


def test_get_default_isolation_level_is_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        Dialect().get_default_isolation_level(object())


def test_engine_connect_does_not_probe_isolation_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    original_execute = duckdb_sqlalchemy.CursorWrapper.execute

    def tracked_execute(
        self: CursorWrapper,
        statement: str,
        parameters: Optional[Tuple[Any, ...]] = None,
        context: Optional[Any] = None,
    ) -> None:
        calls.append(statement)
        return original_execute(self, statement, parameters, context)

    monkeypatch.setattr(duckdb_sqlalchemy.CursorWrapper, "execute", tracked_execute)

    engine = create_engine("duckdb:///:memory:isolation_lifecycle")
    with engine.connect() as conn:
        assert conn.execute(text("select 1")).scalar() == 1

    assert "select 1" in calls
    assert "show transaction isolation level" not in calls


def test_engine_url_without_database_keeps_memory_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)

    engine = create_engine("duckdb://?access_mode=read_only")
    with engine.connect() as conn:
        assert conn.execute(text("select current_database()")).scalar() == "memory"

    assert list(tmp_path.iterdir()) == []


def test_cursorwrapper_executemany_coerces_to_list() -> None:
    class DummyConn:
        def __init__(self) -> None:
            self.calls = []

        def executemany(self, *args):
            self.calls.append(args)

    conn = DummyConn()
    cursor = _cursor(conn)
    params = ({"a": 1}, {"a": 2})

    cursor.executemany("insert", cast(Any, params))

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


def test_cursorwrapper_execute_preserves_runtime_errors_without_message() -> None:
    class BrokenConn:
        def execute(self, *args, **kwargs):
            raise RuntimeError()

    cursor = _cursor(BrokenConn())
    with pytest.raises(RuntimeError):
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
    assert bind({}) == {"key": [], "value": []}
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


def test_duckdb_reflection_filters_share_schema_database_builder() -> None:
    dialect = Dialect()

    stmt, params = dialect._duckdb_reflection_stmt(
        "duckdb_tables",
        "table_name",
        schema='"analytics db"."reporting"',
        filter_names=["orders"],
        include_internal_filter=True,
    )

    assert "AND internal = false" in stmt.text
    assert "AND schema_name = :schema_name" in stmt.text
    assert "AND database_name = :database_name" in stmt.text
    assert "AND table_name IN :filter_names" in stmt.text
    assert params == {
        "schema_name": "reporting",
        "database_name": "analytics db",
        "filter_names": ["orders"],
    }

    ordered_stmt, ordered_params = dialect._duckdb_reflection_stmt(
        "duckdb_tables",
        "table_name",
        filter_names=["orders"],
        suffix="ORDER BY table_name",
    )

    assert ordered_stmt.text.rstrip().endswith("ORDER BY table_name")
    assert ordered_params == {"filter_names": ["orders"]}
    assert ordered_stmt._bindparams["filter_names"].expanding

    class DummyResult:
        def first(self) -> tuple[int]:
            return (1,)

    class DummyConnection:
        def __init__(self) -> None:
            self.statement: Any = None
            self.params: Optional[dict[str, Any]] = None

        def execute(self, statement: Any, params: dict[str, Any]) -> DummyResult:
            self.statement = statement
            self.params = params
            return DummyResult()

    connection = DummyConnection()

    assert dialect._duckdb_table_exists(
        cast(Any, connection), "orders", '"analytics db"."reporting"'
    )
    assert connection.statement is not None
    assert "AND table_name = :table_name" in connection.statement.text
    assert "AND schema_name = :schema_name" in connection.statement.text
    assert "AND database_name = :database_name" in connection.statement.text
    assert connection.params == {
        "table_name": "orders",
        "schema_name": "reporting",
        "database_name": "analytics db",
    }


def test_reflection_fallback_returns_empty_only_for_existing_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dialect = Dialect()
    connection = object()
    seen: list[tuple[object, str, Optional[str]]] = []

    def existing_table(conn: object, table_name: str, schema: Optional[str]) -> bool:
        seen.append((conn, table_name, schema))
        return True

    def missing_table(conn: object, table_name: str, schema: Optional[str]) -> bool:
        seen.append((conn, table_name, schema))
        return False

    def unsupported_reflection() -> list[Any]:
        raise sa_exc.NoSuchTableError("orders")

    monkeypatch.setattr(dialect, "_duckdb_table_exists", existing_table)
    assert (
        dialect._get_reflection_or_empty_for_existing_table(
            unsupported_reflection,
            cast(Any, connection),
            "orders",
            "main",
        )
        == []
    )
    assert seen == [(connection, "orders", "main")]

    monkeypatch.setattr(dialect, "_duckdb_table_exists", missing_table)
    with pytest.raises(sa_exc.NoSuchTableError):
        dialect._get_reflection_or_empty_for_existing_table(
            unsupported_reflection,
            cast(Any, connection),
            "orders",
            "main",
        )


def test_parse_duckdb_enum_labels_unquotes_escaped_strings() -> None:
    labels, enum_name = Dialect()._parse_duckdb_enum_labels(
        "ENUM('alpha', 'it''s fine')",
        None,
    )

    assert labels == ["alpha", "it's fine"]
    assert enum_name is None


def test_reflect_duckdb_index_expressions_parses_columns_and_expressions() -> None:
    expressions, column_names = Dialect()._reflect_duckdb_index_expressions(
        "['name', '\"display name\"', 'lower(name)']"
    )

    assert expressions == ["'name'", "'\"display name\"'", "'lower(name)'"]
    assert column_names == ["name", "display name", None]


def test_apply_config_rejects_invalid_key_no_side_effect() -> None:
    conn = duckdb.connect(":memory:")
    dialect = Dialect()
    with pytest.raises(ValueError, match="invalid config key"):
        apply_config(
            dialect,
            conn,
            {"threads = 1; CREATE TABLE pwned_cfg(i INTEGER); --": "x"},
        )

    found = conn.execute(
        "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name='pwned_cfg'"
    ).fetchone()
    assert found is not None
    assert found[0] == 0


def test_connect_rejects_invalid_extension_before_execute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_core_config()

    class DummyConn:
        def __init__(self) -> None:
            self.executed: list[str] = []

        def execute(self, statement: str) -> None:
            self.executed.append(statement)

        def register_filesystem(self, filesystem: object) -> None:
            return None

    dummy = DummyConn()
    monkeypatch.setattr(duckdb_sqlalchemy.duckdb, "connect", lambda *a, **k: dummy)

    with pytest.raises(ValueError, match="invalid extension name"):
        Dialect().connect(
            database=":memory:",
            preload_extensions=["sqlite; CREATE TABLE pwned_ext(i INTEGER); --"],
            config={},
        )

    assert dummy.executed == []


def test_copy_from_csv_rejects_invalid_table_and_option_key(
    tmp_path: Path,
) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE safe(i INTEGER)")
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("1\n")

    with pytest.raises(ValueError, match="invalid table identifier"):
        copy_from_csv(
            conn,
            "safe FROM 'x'; CREATE TABLE pwned_bulk(i INTEGER); --",
            csv_path,
        )

    with pytest.raises(ValueError, match="invalid COPY option key"):
        bad_options: dict[str, Any] = {
            "header); CREATE TABLE pwned_opt(i INTEGER); --": True
        }
        copy_from_csv(
            conn,
            "safe",
            csv_path,
            **bad_options,
        )

    found = conn.execute(
        "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name IN ('pwned_bulk', 'pwned_opt')"
    ).fetchone()
    assert found is not None
    assert found[0] == 0


def test_copy_from_rows_mapping_infers_columns_and_chunks() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE safe(i INTEGER, label VARCHAR)")

    copy_from_rows(
        conn,
        "safe",
        [
            {"i": 1, "label": "one"},
            {"i": 2, "label": "two"},
            {"i": 3, "label": "three"},
        ],
        chunk_size=2,
        include_header=True,
    )

    rows = conn.execute("SELECT i, label FROM safe ORDER BY i").fetchall()
    assert rows == [(1, "one"), (2, "two"), (3, "three")]


def test_copy_from_rows_sequence_uses_explicit_columns_and_chunks() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE safe(i INTEGER, label VARCHAR)")

    copy_from_rows(
        conn,
        "safe",
        [(1, "one"), (2, "two"), (3, "three")],
        columns=["i", "label"],
        chunk_size=2,
        include_header=True,
    )

    rows = conn.execute("SELECT i, label FROM safe ORDER BY i").fetchall()
    assert rows == [(1, "one"), (2, "two"), (3, "three")]


def test_copy_rows_as_sequences_infers_mapping_columns() -> None:
    rows, columns = duckdb_sqlalchemy.bulk._copy_rows_as_sequences(
        {"id": 1, "label": "one"},
        [{"id": 2, "label": "two"}],
        None,
    )

    assert columns == ["id", "label"]
    assert list(rows) == [[1, "one"], [2, "two"]]


def test_copy_rows_as_sequences_uses_explicit_mapping_columns() -> None:
    rows, columns = duckdb_sqlalchemy.bulk._copy_rows_as_sequences(
        {"id": 1, "label": "one"},
        [{"id": 2}],
        ["label", "id"],
    )

    assert columns == ["label", "id"]
    assert list(rows) == [["one", 1], [None, 2]]


def test_copy_from_rows_closes_rotated_tempfiles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = []
    copied = []

    class TrackingTempFile:
        def __init__(self, path: Path) -> None:
            self.name = str(path)
            self._fh = path.open("w", newline="")
            self.closed = False
            created.append(self)

        def write(self, data: str) -> int:
            return self._fh.write(data)

        def flush(self) -> None:
            self._fh.flush()

        def close(self) -> None:
            if not self.closed:
                self._fh.close()
                self.closed = True

    def fake_named_temporary_file(
        mode: str,
        newline: str,
        suffix: str,
        delete: bool,
    ) -> TrackingTempFile:
        path = tmp_path / f"chunk-{len(created)}{suffix}"
        return TrackingTempFile(path)

    def fake_copy_from_csv(connection: object, table: object, path: str, **kwargs):
        copied.append(Path(path))
        assert Path(path).exists()

    monkeypatch.setattr(
        duckdb_sqlalchemy.bulk.tempfile,
        "NamedTemporaryFile",
        fake_named_temporary_file,
    )
    monkeypatch.setattr(duckdb_sqlalchemy.bulk, "copy_from_csv", fake_copy_from_csv)

    copy_from_rows(
        object(),
        "safe",
        [(1,), (2,)],
        columns=["id"],
        chunk_size=1,
    )

    assert len(created) == 2
    assert len(copied) == 2
    assert all(temp.closed for temp in created)
    assert all(not Path(temp.name).exists() for temp in created)


def test_parse_register_params_dict_and_tuple() -> None:
    view_name, df = _parse_register_params({"view_name": "v", "df": "data"})
    assert view_name == "v"
    assert df == "data"

    view_name, df = _parse_register_params(("v2", "data2"))
    assert view_name == "v2"
    assert df == "data2"


def test_parse_register_params_errors() -> None:
    with pytest.raises(ValueError):
        _parse_register_params(None)

    with pytest.raises(ValueError):
        _parse_register_params({"name": "v"})

    with pytest.raises(ValueError):
        _parse_register_params(("only-one",))


def test_normalize_execution_options_insertmanyvalues() -> None:
    original = {"duckdb_insertmanyvalues_page_size": 123}
    with pytest.warns(DeprecationWarning, match="insertmanyvalues_page_size"):
        normalized = _normalize_execution_options(original)
    assert normalized["insertmanyvalues_page_size"] == 123
    assert "insertmanyvalues_page_size" not in original

    already = {
        "duckdb_insertmanyvalues_page_size": 5,
        "insertmanyvalues_page_size": 10,
    }
    normalized = _normalize_execution_options(already)
    assert normalized["insertmanyvalues_page_size"] == 10


def test_normalize_execution_options_canonical_value_skips_warning() -> None:
    original = {
        "duckdb_insertmanyvalues_page_size": 5,
        "insertmanyvalues_page_size": 10,
    }

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        normalized = _normalize_execution_options(original)

    assert normalized["insertmanyvalues_page_size"] == 10
    assert recorded == []


def test_idempotent_statement_detection() -> None:
    assert _is_idempotent_statement("SELECT 1")
    assert _is_idempotent_statement("  show tables")
    assert _is_idempotent_statement("pragma version")
    assert _is_idempotent_statement("-- comment\nSELECT 1")
    assert _is_idempotent_statement(
        "WITH recent AS (SELECT 1 AS value) SELECT value FROM recent"
    )
    assert _is_idempotent_statement(
        "/* comment */ WITH RECURSIVE seq(n) AS "
        "(SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3) "
        "SELECT n FROM seq"
    )
    assert not _is_idempotent_statement("insert into t values (1)")
    assert not _is_idempotent_statement(
        "WITH source AS (SELECT 1 AS value) INSERT INTO t SELECT value FROM source"
    )


def test_transient_error_detection() -> None:
    assert _is_transient_error(RuntimeError("HTTP Error: 503 Service Unavailable"))
    assert not _is_transient_error(RuntimeError("connection reset by peer"))
    assert not _is_transient_error(RuntimeError("HTTP Error: 503 connection reset"))


def test_pool_override_from_url_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    url = URL(database=":memory:", query={"duckdb_sqlalchemy_pool": ["Queue"]})
    assert _pool_override_from_url(url) == "queue"

    monkeypatch.setenv("DUCKDB_SQLALCHEMY_POOL", "Null")
    url = URL(database=":memory:")
    assert _pool_override_from_url(url) == "null"


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ("queue", pool.QueuePool),
        ("singleton", pool.SingletonThreadPool),
        ("singletonthreadpool", pool.SingletonThreadPool),
        ("null", pool.NullPool),
        ("nullpool", pool.NullPool),
    ],
)
def test_pool_class_uses_override_aliases(
    override: str, expected: type[pool.Pool]
) -> None:
    url = URL(database="md:my_db", query={"duckdb_sqlalchemy_pool": override})
    assert Dialect.get_pool_class(url) is expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (SAURL.create("duckdb", database=":memory:"), pool.SingletonThreadPool),
        (SAURL.create("duckdb", database=":memory:named"), pool.QueuePool),
        (SAURL.create("duckdb"), pool.QueuePool),
        (SAURL.create("duckdb", database="local.db"), pool.QueuePool),
        (SAURL.create("duckdb", database="md:my_db"), pool.NullPool),
    ],
)
def test_pool_class_defaults(url: SAURL, expected: type[pool.Pool]) -> None:
    assert Dialect.get_pool_class(url) is expected


def test_pool_class_for_empty_database() -> None:
    url = SAURL.create("duckdb")
    assert Dialect.get_pool_class(url) is pool.QueuePool


def test_apply_config_handles_none_path_decimal() -> None:
    import decimal
    from pathlib import Path

    dialect = Dialect()

    class DummyConn:
        def __init__(self) -> None:
            self.executed = []

        def execute(self, statement: str) -> None:
            self.executed.append(statement)

    conn = DummyConn()
    ext = {
        "memory_limit": None,
        "data_path": Path("/tmp/data"),
        "ratio": decimal.Decimal("1.5"),
    }

    apply_config(
        dialect,
        conn,
        cast(dict[str, ConfigValue], ext),
    )

    string_processor = String().literal_processor(dialect=dialect)
    expected = [
        "SET memory_limit = NULL",
        f"SET data_path = {string_processor(str(Path('/tmp/data')))}",
        f"SET ratio = {string_processor(str(decimal.Decimal('1.5')))}",
    ]
    assert conn.executed == expected


def test_apply_config_stringifies_unknown_values() -> None:
    dialect = Dialect()

    class DummyConn:
        def __init__(self) -> None:
            self.executed = []

        def execute(self, statement: str) -> None:
            self.executed.append(statement)

    class UnknownValue:
        def __str__(self) -> str:
            return "custom-value"

    conn = DummyConn()
    apply_config(
        dialect,
        conn,
        cast(
            dict[str, ConfigValue],
            {"custom_option": UnknownValue()},
        ),
    )

    string_processor = String().literal_processor(dialect=dialect)
    assert conn.executed == [f"SET custom_option = {string_processor('custom-value')}"]


def test_motherduck_helpers() -> None:
    url = md.MotherDuckURL(
        database="md:db",
        query={"memory_limit": "1GB"},
        path_query={"user": "alice", "session_name": "team"},
    )
    assert url.database is not None
    assert url.database.startswith("md:db?")
    assert url.query == {"memory_limit": "1GB"}

    appended = md.append_query_to_database("md:db?user=alice", {"session_hint": "s"})
    assert appended == "md:db?user=alice&session_hint=s"

    normalized = md._normalize_path_item("duckdb:///tmp.db")
    assert normalized.drivername == "duckdb"

    normalized = md._normalize_path_item("md:db")
    assert normalized.database == "md:db"


def test_motherduck_url_coerces_path_and_query_values() -> None:
    url = md.MotherDuckURL(
        database="md:db",
        query={"saas_mode": False, "token": None},
        path_query={
            "session_name": "team",
            "attach_mode": ("single", "workspace"),
            "cache_buster": None,
        },
    )
    assert url.database is not None
    database, query = url.database.split("?", 1)
    assert database == "md:db"
    assert parse_qs(query) == {
        "session_name": ["team"],
        "attach_mode": ["single", "workspace"],
    }
    assert url.query == {"saas_mode": "false"}


def test_motherduck_url_merges_and_overrides_across_inputs() -> None:
    url = md.MotherDuckURL(
        database="md:db",
        query={"memory_limit": "256MB", "saas_mode": True, "drop": None},
        path_query={"session_name": "old-team", "user": "alice"},
        session_name="new-team",
        attach_mode=("single", "workspace"),
        memory_limit="1GB",
        cache_buster=None,
    )

    assert url.database is not None
    database, query = url.database.split("?", 1)
    assert database == "md:db"
    assert parse_qs(query) == {
        "session_name": ["new-team"],
        "user": ["alice"],
        "attach_mode": ["single", "workspace"],
    }
    assert url.query == {"memory_limit": "1GB", "saas_mode": "true"}


def test_motherduck_url_normalizes_deprecated_session_aliases() -> None:
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        url = md.MotherDuckURL(
            database="md:db",
            path_query={"session_hint": "old-team"},
            motherduck_session_hint="legacy-team",
            motherduck_session_name="prefixed-team",
        )

    assert url.database == "md:db?session_name=old-team"
    assert [str(w.message) for w in recorded] == [
        "`session_hint` is deprecated; use `session_name` instead.",
        "`motherduck_session_hint` is deprecated; use `session_name` instead.",
        "`motherduck_session_name` is deprecated; use `session_name` instead.",
    ]


def test_motherduck_url_prefers_canonical_ttl_over_alias() -> None:
    with pytest.warns(DeprecationWarning, match="dbinstance_inactivity_ttl"):
        url = md.MotherDuckURL(
            database="md:db",
            path_query={"motherduck_dbinstance_inactivity_ttl": "10m"},
            dbinstance_inactivity_ttl="15m",
        )

    assert url.database == "md:db?dbinstance_inactivity_ttl=15m"


def test_motherduck_url_normalizes_deprecated_path_aliases() -> None:
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        url = md.MotherDuckURL(
            database="md:db",
            motherduck_session_hint="team-a",
            motherduck_attach_mode="single",
            motherduck_saas_mode=True,
            cachebust="abc123",
        )

    assert url.database is not None
    database, query = url.database.split("?", 1)
    assert database == "md:db"
    assert parse_qs(query) == {
        "session_name": ["team-a"],
        "attach_mode": ["single"],
        "saas_mode": ["true"],
        "cache_buster": ["abc123"],
    }
    assert url.query == {}
    messages = [str(w.message) for w in recorded]
    assert messages == [
        "`motherduck_session_hint` is deprecated; use `session_name` instead.",
        "`motherduck_attach_mode` is deprecated; use `attach_mode` instead.",
        "`motherduck_saas_mode` is deprecated; use `saas_mode` instead.",
        "`cachebust` is deprecated; use `cache_buster` instead.",
    ]


def test_split_url_query_partitions_and_ignores_dialect_keys() -> None:
    query = {
        "user": "alice",
        "host": "localhost",
        "port": 1984,
        "tls": "off",
        "motherduck_dbinstance_inactivity_ttl": "15m",
        "memory_limit": "1GB",
        "duckdb_sqlalchemy_pool": "queue",
    }

    with pytest.warns(DeprecationWarning, match="dbinstance_inactivity_ttl"):
        path_query, url_config = md.split_url_query(query)

    assert path_query == {
        "user": "alice",
        "host": "localhost",
        "port": "1984",
        "tls": "off",
        "dbinstance_inactivity_ttl": "15m",
    }
    assert url_config == {"memory_limit": "1GB"}
    assert query["duckdb_sqlalchemy_pool"] == "queue"


def test_split_url_query_normalizes_motherduck_setting_aliases() -> None:
    query = {
        "motherduck_session_hint": "team-a",
        "motherduck_attach_mode": "single",
        "motherduck_saas_mode": False,
        "cachebust": "abc123",
    }

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        path_query, url_config = md.split_url_query(query)

    assert path_query == {
        "session_name": "team-a",
        "attach_mode": "single",
        "saas_mode": "false",
        "cache_buster": "abc123",
    }
    assert url_config == {}
    assert [str(w.message) for w in recorded] == [
        "`motherduck_session_hint` is deprecated; use `session_name` instead.",
        "`motherduck_attach_mode` is deprecated; use `attach_mode` instead.",
        "`motherduck_saas_mode` is deprecated; use `saas_mode` instead.",
        "`cachebust` is deprecated; use `cache_buster` instead.",
    ]


def test_split_url_query_normalizes_legacy_motherduck_transport_aliases() -> None:
    query = {
        "motherduck_host": "api.staging.motherduck.com",
        "motherduck_region_host": "regional-api.staging.motherduck.com",
        "motherduck_port": 443,
        "motherduck_use_tls": True,
        "motherduck_grpc_local_subchannel_pool": False,
    }

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        path_query, url_config = md.split_url_query(query)

    assert path_query == {
        "host": "api.staging.motherduck.com",
        "region_host": "regional-api.staging.motherduck.com",
        "port": "443",
        "tls": "true",
        "grpc_local_subchannel_pool": "false",
    }
    assert url_config == {}
    assert [str(w.message) for w in recorded] == [
        "`motherduck_host` is deprecated; use `host` instead.",
        "`motherduck_region_host` is deprecated; use `region_host` instead.",
        "`motherduck_port` is deprecated; use `port` instead.",
        "`motherduck_use_tls` is deprecated; use `tls` instead.",
        "`motherduck_grpc_local_subchannel_pool` is deprecated; use `grpc_local_subchannel_pool` instead.",
    ]


def test_extract_path_query_from_config_mutates_and_normalizes_aliases() -> None:
    config = {
        "host": "localhost",
        "port": 1984,
        "tls": "off",
        "session_name": "team-a",
        "motherduck_dbinstance_inactivity_ttl": "10m",
        "threads": 4,
    }

    with pytest.warns(DeprecationWarning, match="dbinstance_inactivity_ttl"):
        path_query = md.extract_path_query_from_config(config)

    assert path_query == {
        "host": "localhost",
        "port": "1984",
        "tls": "off",
        "session_name": "team-a",
        "dbinstance_inactivity_ttl": "10m",
    }
    assert config == {"threads": 4}


def test_extract_path_query_from_config_handles_motherduck_aliases() -> None:
    config = {
        "motherduck_session_hint": "team-a",
        "motherduck_attach_mode": "workspace",
        "motherduck_saas_mode": True,
        "cachebust": "abc123",
        "threads": 4,
    }

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        path_query = md.extract_path_query_from_config(config)

    assert path_query == {
        "session_name": "team-a",
        "attach_mode": "workspace",
        "saas_mode": "true",
        "cache_buster": "abc123",
    }
    assert config == {"threads": 4}
    assert [str(w.message) for w in recorded] == [
        "`motherduck_session_hint` is deprecated; use `session_name` instead.",
        "`motherduck_attach_mode` is deprecated; use `attach_mode` instead.",
        "`motherduck_saas_mode` is deprecated; use `saas_mode` instead.",
        "`cachebust` is deprecated; use `cache_buster` instead.",
    ]


def test_extract_path_query_from_config_handles_legacy_transport_aliases() -> None:
    config = {
        "motherduck_host": "api.staging.motherduck.com",
        "motherduck_port": 443,
        "motherduck_use_tls": True,
        "threads": 4,
    }

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        path_query = md.extract_path_query_from_config(config)

    assert path_query == {
        "host": "api.staging.motherduck.com",
        "port": "443",
        "tls": "true",
    }
    assert config == {"threads": 4}
    assert [str(w.message) for w in recorded] == [
        "`motherduck_host` is deprecated; use `host` instead.",
        "`motherduck_port` is deprecated; use `port` instead.",
        "`motherduck_use_tls` is deprecated; use `tls` instead.",
    ]


def test_merge_and_copy_connect_args() -> None:
    base = {"config": {"threads": 2}, "url_config": {"memory_limit": "1GB"}}
    extra = {"config": {"threads": 4}, "url_config": {"s3_region": "us-east-1"}}
    merged = md._merge_connect_args(base, extra)

    assert merged["config"] == {"threads": 4}
    assert merged["url_config"] == {"memory_limit": "1GB", "s3_region": "us-east-1"}
    assert base["config"] == {"threads": 2}
    assert base["url_config"] == {"memory_limit": "1GB"}
    assert merged["config"] is not base["config"]
    assert merged["url_config"] is not base["url_config"]
    assert merged["config"] is not extra["config"]
    assert merged["url_config"] is not extra["url_config"]

    copied = md._copy_connect_params(merged)
    copied["config"]["threads"] = 1
    copied["url_config"]["memory_limit"] = "2GB"
    assert merged["config"]["threads"] == 4
    assert merged["url_config"]["memory_limit"] == "1GB"


def test_create_engine_from_paths_driver_mismatch() -> None:
    url1 = SAURL.create("duckdb", database=":memory:")
    url2 = SAURL.create("sqlite", database=":memory:")
    with pytest.raises(ValueError):
        create_engine_from_paths([url1, url2])
