"""Regression tests for connection URL, pooling, and token handling fixes."""

import importlib.util
import warnings
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, cast
from urllib.parse import parse_qs

import duckdb
import pytest
from sqlalchemy import create_engine, pool, text
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import URL as SAURL
from sqlalchemy.engine import make_url as sa_make_url

import duckdb_sqlalchemy
from duckdb_sqlalchemy import (
    URL,
    Dialect,
    DuckDBEngineWarning,
    MotherDuckURL,
    _apply_motherduck_defaults,
    create_engine_from_paths,
    create_motherduck_engine,
)
from duckdb_sqlalchemy import motherduck as md
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


# 1. MotherDuck path keys stay DuckDB config for local databases.


@pytest.mark.parametrize(
    "database", ["local.db", "/tmp/data/local.db", ":memory:", ":memory:named"]
)
def test_create_connect_args_keeps_path_keys_as_config_for_local(
    database: str,
) -> None:
    url = SAURL.create(
        "duckdb",
        database=database,
        query={"access_mode": "read_only", "user": "alice", "threads": "4"},
    )

    _, kwargs = Dialect().create_connect_args(url)

    assert kwargs["database"] == database
    assert kwargs["url_config"] == {
        "access_mode": "read_only",
        "user": "alice",
        "threads": "4",
    }


@pytest.mark.parametrize("prefix", ["md:", "motherduck:", "MD:"])
def test_create_connect_args_moves_path_keys_for_motherduck(prefix: str) -> None:
    url = sa_make_url(
        f"duckdb:///{prefix}my_db?attach_mode=single&access_mode=read_only&threads=4"
    )

    _, kwargs = Dialect().create_connect_args(url)

    database, _, _ = kwargs["database"].partition("?")
    assert database == f"{prefix}my_db"
    assert _query(kwargs["database"]) == {
        "attach_mode": ["single"],
        "access_mode": ["read_only"],
    }
    assert kwargs["url_config"] == {"threads": "4"}


def test_local_file_url_access_mode_opens_existing_file_read_only(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "local.db"
    writer = duckdb.connect(str(db_path))
    writer.execute("create table t as select 42 as i")
    writer.close()

    engine = create_engine(f"duckdb:///{db_path}?access_mode=read_only")
    try:
        with engine.connect() as conn:
            assert conn.execute(text("select i from t")).scalar() == 42
            with pytest.raises(sa_exc.DBAPIError, match="read-only"):
                conn.execute(text("insert into t values (1)"))
    finally:
        engine.dispose()

    assert sorted(p.name for p in tmp_path.iterdir()) == ["local.db"]


def test_connect_config_path_keys_stay_config_for_local(
    captured_connect: Dict[str, Any],
) -> None:
    Dialect().connect(
        database="local.db",
        config={"access_mode": "read_only", "threads": 2},
    )

    assert captured_connect["database"] == "local.db"
    assert captured_connect["config"]["access_mode"] == "read_only"


# 2. MotherDuck detection is based on the database string only.


def test_local_file_with_motherduck_keys_uses_local_pool() -> None:
    url = sa_make_url("duckdb:///local.db?access_mode=read_only&user=alice")
    assert Dialect.get_pool_class(url) is pool.QueuePool


def test_local_file_with_path_keys_does_not_get_env_token(
    monkeypatch: pytest.MonkeyPatch, captured_connect: Dict[str, Any]
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "env-token")

    Dialect().connect(database="local.db", config={"access_mode": "read_only"})

    assert "motherduck_token" not in captured_connect["config"]


# 3. Environment token injection only for MotherDuck without explicit credentials.


@pytest.mark.parametrize(
    "key", ["token", "motherduck_token", "motherduck_oauth_token", "oauth_token", "slt"]
)
def test_env_token_not_injected_with_explicit_credential(
    monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "env-token")
    config: Dict[str, Any] = {key: "explicit"}

    _apply_motherduck_defaults(config, "md:my_db")

    assert config == {key: "explicit"}


def test_env_token_not_injected_with_credential_in_database_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "env-token")
    config: Dict[str, Any] = {}

    _apply_motherduck_defaults(config, "md:my_db?attach_mode=single&slt=explicit")

    assert config == {}


def test_env_token_injected_for_motherduck_without_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "env-token")
    config: Dict[str, Any] = {"threads": 1}

    _apply_motherduck_defaults(config, "MD:my_db?attach_mode=single")

    assert config["motherduck_token"] == "env-token"


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


# 6. MotherDuckURL keeps path params in URL.query and round-trips.


def test_motherduck_url_round_trips_through_make_url() -> None:
    url = MotherDuckURL(
        database="md:my_db", attach_mode="single", session_name="abc", threads=4
    )

    assert url.database == "md:my_db"
    assert str(url).count("?") == 1
    parsed = sa_make_url(str(url))
    assert parsed.database == "md:my_db"
    assert dict(parsed.query) == dict(url.query)
    assert parsed.query["session_name"] == "abc"

    dialect = Dialect()
    _, original_kwargs = dialect.create_connect_args(url)
    _, parsed_kwargs = dialect.create_connect_args(parsed)
    for kwargs in (original_kwargs, parsed_kwargs):
        database, _, _ = kwargs["database"].partition("?")
        assert database == "md:my_db"
        assert _query(kwargs["database"]) == {
            "attach_mode": ["single"],
            "session_name": ["abc"],
        }
        assert kwargs["url_config"] == {"threads": "4"}


def test_motherduck_url_connect_args_match_previous_database_string() -> None:
    url = MotherDuckURL(
        database="md:my_db",
        query={"memory_limit": "1GB"},
        path_query={"user": "alice"},
        session_name="team",
        attach_mode="single",
    )

    _, kwargs = Dialect().create_connect_args(url)

    # Same database string the builder used to embed in ``URL.database``.
    assert (
        kwargs["database"] == "md:my_db?user=alice&session_name=team&attach_mode=single"
    )
    assert kwargs["url_config"] == {"memory_limit": "1GB"}


def test_motherduck_url_moves_embedded_database_query() -> None:
    url = MotherDuckURL(
        database="md:my_db?attach_mode=single&threads=2", attach_mode="workspace"
    )

    assert url.database == "md:my_db"
    assert dict(url.query) == {"threads": "2", "attach_mode": "workspace"}


# 7. No duplicated path keys; case-insensitive prefix validation.


def test_append_query_to_database_overrides_existing_keys() -> None:
    database = md.append_query_to_database(
        "md:db?attach_mode=single&user=alice", {"attach_mode": "workspace"}
    )

    assert database == "md:db?user=alice&attach_mode=workspace"


def test_connect_path_keys_do_not_duplicate_url_path_keys(
    captured_connect: Dict[str, Any],
) -> None:
    Dialect().connect(
        database="md:db?attach_mode=single",
        config={"attach_mode": "workspace"},
    )

    assert _query(captured_connect["database"]) == {"attach_mode": ["workspace"]}


@pytest.mark.parametrize("database", ["MD:a,b", "MotherDuck:a,b", "md:a,b?x=1"])
def test_validate_motherduck_database_name_is_case_insensitive(
    database: str,
) -> None:
    with pytest.raises(ValueError, match="commas"):
        md.validate_motherduck_database_name(database)


def test_validate_allows_commas_in_local_paths() -> None:
    md.validate_motherduck_database_name("local,file.db")


# 8. create_motherduck_engine / create_engine_from_paths engine kwargs.


def test_create_motherduck_engine_routes_engine_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "fake")

    engine = create_motherduck_engine(
        database="md:my_db",
        attach_mode="single",
        threads=4,
        pool_size=3,
        max_overflow=2,
        pool_timeout=5,
        echo=True,
        execution_options={"duckdb_arraysize": 10},
    )

    assert isinstance(engine.pool, pool.QueuePool)
    assert engine.pool.size() == 3
    assert engine.echo is True
    assert engine.get_execution_options()["duckdb_arraysize"] == 10
    assert dict(engine.url.query) == {"threads": "4", "attach_mode": "single"}
    _, kwargs = engine.dialect.create_connect_args(engine.url)
    assert kwargs["database"] == "md:my_db?attach_mode=single"
    assert kwargs["url_config"] == {"threads": "4"}


def test_create_motherduck_engine_keeps_nullpool_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "fake")

    engine = create_motherduck_engine(database="md:my_db", pool_recycle=60)

    assert isinstance(engine.pool, pool.NullPool)


def test_create_engine_from_paths_with_pool_sizing_uses_queue_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "fake")
    calls = []

    def fake_connect(self: Any, *cargs: Any, **cparams: Any) -> object:
        calls.append(cparams)
        return object()

    monkeypatch.setattr(duckdb_sqlalchemy.Dialect, "connect", fake_connect)

    engine = create_engine_from_paths(
        ["md:db?user=1", "md:db?user=2"], pool_size=2, max_overflow=1
    )

    assert isinstance(engine.pool, pool.QueuePool)
    assert engine.pool.size() == 2
    creator = cast(Any, engine.pool)._creator
    creator()
    creator()
    creator()
    assert [c["database"] for c in calls] == [
        "md:db?user=1",
        "md:db?user=2",
        "md:db?user=1",
    ]


def _load_example(name: str) -> ModuleType:
    path = EXAMPLES_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"example_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@requires_examples
def test_example_multi_instance_pool_builds_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "fake")
    module = _load_example("motherduck_multi_instance_pool")

    engine = module.build_engine()

    assert isinstance(engine.pool, pool.QueuePool)
    assert engine.pool.size() == 5
    assert engine.url.database == "md:my_db?attach_mode=single&user=1"


@requires_examples
def test_example_read_scaling_per_user_builds_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "fake")
    module = _load_example("motherduck_read_scaling_per_user")

    engine = module.get_engine_for_user("user-123")

    assert isinstance(engine.pool, pool.QueuePool)
    assert engine.pool.size() == 5
    assert engine.url.database == "md:my_db"
    _, kwargs = engine.dialect.create_connect_args(engine.url)
    assert _query(kwargs["database"]) == {
        "attach_mode": ["single"],
        "access_mode": ["read_only"],
        "session_name": [engine.url.query["session_name"]],
    }
    assert kwargs["url_config"] == {}


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


# 11. Token values stay out of the database string and our messages.


def test_motherduck_url_keeps_tokens_out_of_database() -> None:
    url = MotherDuckURL(database="md:my_db", slt="secret-slt", motherduck_token="tok")

    assert url.database == "md:my_db"
    assert "secret-slt" not in (url.database or "")


def test_warnings_do_not_include_token_values(
    monkeypatch: pytest.MonkeyPatch, captured_connect: Dict[str, Any]
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "env-secret")
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        url = MotherDuckURL(
            database="md:my_db",
            slt="secret-slt",
            motherduck_session_hint="team",
            query={"pool": "bogus", "motherduck_token": "secret-token"},
        )
        Dialect.get_pool_class(url)
        _, kwargs = Dialect().create_connect_args(url)
        Dialect().connect(**kwargs)

    assert recorded
    for warning in recorded:
        message = str(warning.message)
        for secret in ("secret-slt", "secret-token", "env-secret"):
            assert secret not in message
