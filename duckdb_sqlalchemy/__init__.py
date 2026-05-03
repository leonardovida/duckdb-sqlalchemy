import os
import re
import time
import uuid
import warnings
from collections import defaultdict
from functools import lru_cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    cast,
)

import duckdb
import sqlalchemy
from packaging.version import Version
from sqlalchemy import pool, select, sql, text, util
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.dialects.postgresql.base import (
    PGDialect,
    PGIdentifierPreparer,
    PGInspector,
)
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.reflection import ReflectionDefaults, cache
from sqlalchemy.engine.url import URL as SAURL
from sqlalchemy.exc import InvalidRequestError, NoSuchTableError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import bindparam
from sqlalchemy.sql.selectable import Select

from ._bulk_insert import build_bulk_insert_data as _build_bulk_insert_data
from ._supports import has_comment_support
from ._validation import validate_extension_name
from .bulk import copy_from_csv, copy_from_parquet, copy_from_rows
from .capabilities import get_capabilities
from .config import apply_config, get_core_config
from .datatypes import ISCHEMA_NAMES, register_extension_types
from .motherduck import (
    DIALECT_QUERY_KEYS,
    MOTHERDUCK_CONFIG_KEYS,
    MotherDuckURL,
    _normalize_config_aliases,
    append_query_to_database,
    create_engine_from_paths,
    create_motherduck_engine,
    extract_path_query_from_config,
    split_url_query,
    stable_session_hint,
    stable_session_name,
    validate_motherduck_database_name,
)
from .olap import md_user_info, read_csv, read_csv_auto, read_parquet, table_function
from .url import URL, make_url

try:
    from sqlalchemy.dialects.postgresql import base as _pg_base
except ImportError:  # pragma: no cover - fallback for older SQLAlchemy
    _PGExecutionContext = DefaultExecutionContext
else:
    _PGExecutionContext = getattr(
        _pg_base, "PGExecutionContext", DefaultExecutionContext
    )

try:
    __version__ = package_version("duckdb-sqlalchemy")
except PackageNotFoundError:  # pragma: no cover - source tree import fallback
    __version__ = "1.5.2.1"
sqlalchemy_version = sqlalchemy.__version__
SQLALCHEMY_VERSION = Version(sqlalchemy_version)
SQLALCHEMY_2 = SQLALCHEMY_VERSION >= Version("2.0.0")
duckdb_version: str = duckdb.__version__
_capabilities = get_capabilities(duckdb_version)
supports_attach: bool = _capabilities.supports_attach
supports_user_agent: bool = _capabilities.supports_user_agent

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.engine.interfaces import (  # noqa: F401
        ReflectedCheckConstraint,
        ReflectedForeignKeyConstraint,
        ReflectedIndex,
        ReflectedPrimaryKeyConstraint,
        ReflectedUniqueConstraint,
    )

    from .capabilities import DuckDBCapabilities

register_extension_types()


__all__ = [
    "Dialect",
    "ConnectionWrapper",
    "CursorWrapper",
    "DBAPI",
    "DuckDBEngineWarning",
    "insert",  # reexport of sqlalchemy.dialects.postgresql.insert
    "MotherDuckURL",
    "URL",
    "create_engine_from_paths",
    "create_motherduck_engine",
    "make_url",
    "stable_session_hint",
    "stable_session_name",
    "table_function",
    "read_parquet",
    "read_csv",
    "read_csv_auto",
    "md_user_info",
    "copy_from_parquet",
    "copy_from_csv",
    "copy_from_rows",
    "checkpoint",
]


def checkpoint(connection: Any, *, force: bool = False, commit: bool = True) -> None:
    """Run CHECKPOINT with explicit transaction handling.

    SQLAlchemy 2.x connections autobegin transactions on first use. Running
    ``CHECKPOINT`` inside that transaction can leave the connection in an
    aborted state after local writes. This helper mirrors the raw DuckDB
    workflow by committing before and after the checkpoint when requested.
    """

    statement = "FORCE CHECKPOINT" if force else "CHECKPOINT"

    if hasattr(connection, "exec_driver_sql"):
        if connection.in_nested_transaction():
            raise InvalidRequestError(
                "checkpoint() does not support nested transactions"
            )
        if commit and connection.in_transaction():
            connection.commit()
        connection.exec_driver_sql(statement)
        if commit and connection.in_transaction():
            connection.commit()
        return

    if hasattr(connection, "execute") and hasattr(connection, "commit"):
        if commit:
            connection.commit()
        connection.execute(statement)
        if commit:
            connection.commit()
        return

    raise TypeError(
        "checkpoint() requires a SQLAlchemy Connection or DuckDB connection"
    )


class DBAPI:
    paramstyle = "numeric_dollar" if SQLALCHEMY_2 else "qmark"
    apilevel = duckdb.apilevel
    threadsafety = duckdb.threadsafety

    # this is being fixed upstream to add a proper exception hierarchy
    Error = getattr(duckdb, "Error", RuntimeError)
    TransactionException = getattr(duckdb, "TransactionException", Error)
    ParserException = getattr(duckdb, "ParserException", Error)

    @staticmethod
    def Binary(x: Any) -> Any:
        return x


class DuckDBInspector(PGInspector):
    def get_check_constraints(
        self, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> Any:
        try:
            return super().get_check_constraints(table_name, schema, **kw)
        except Exception as e:
            raise NotImplementedError() from e


class ConnectionWrapper:
    __c: duckdb.DuckDBPyConnection
    notices: List[str]
    autocommit = None  # duckdb doesn't support setting autocommit
    closed = False

    def __init__(self, c: duckdb.DuckDBPyConnection) -> None:
        self.__c = c
        self.notices = list()

    def cursor(self) -> "CursorWrapper":
        return CursorWrapper(self.__c, self)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__c, name)

    def close(self) -> None:
        self.__c.close()
        self.closed = True


_REGISTER_NAME_KEYS = ("name", "view_name", "table")
_REGISTER_DATA_KEYS = ("df", "dataframe", "relation", "data")
_IGNORED_POSTGRES_CONFIG_SETTINGS = frozenset(
    {
        "extra_float_digits",
        "application_name",
        "standard_conforming_strings",
        "client_min_messages",
        "datestyle",
        "ssl_renegotiation_limit",
        "statement_timeout",
    }
)
_SET_POSTGRES_CONFIG_RE = re.compile(r"^set\s+(?:session\s+|local\s+)?(\w+)\b")
_SET_TRANSACTION_ISOLATION_RE = re.compile(
    r"^set\s+(?:session\s+characteristics\s+as\s+)?transaction\s+isolation\s+level\s+"
    r"(?:serializable|repeatable\s+read|read\s+committed|read\s+uncommitted)$"
)
_BEGIN_TRANSACTION_ISOLATION_RE = re.compile(
    r"^begin\s+(?:transaction\s+)?isolation\s+level\s+"
    r"(?:serializable|repeatable\s+read|read\s+committed|read\s+uncommitted)$"
)


def _parse_register_params(parameters: Optional[Any]) -> Tuple[str, Any]:
    if parameters is None:
        raise ValueError("register requires a view name and data")
    if isinstance(parameters, dict):
        view_name = None
        for key in _REGISTER_NAME_KEYS:
            if key in parameters:
                view_name = parameters[key]
                break
        df = None
        for key in _REGISTER_DATA_KEYS:
            if key in parameters:
                df = parameters[key]
                break
        if view_name is None or df is None:
            raise ValueError("register requires a view name and data (tuple or dict)")
        return view_name, df
    if isinstance(parameters, (list, tuple)) and len(parameters) == 2:
        return parameters[0], parameters[1]
    raise ValueError("register requires a view name and data (tuple or dict)")


class CursorWrapper:
    __c: duckdb.DuckDBPyConnection
    __connection_wrapper: "ConnectionWrapper"

    def __init__(
        self, c: duckdb.DuckDBPyConnection, connection_wrapper: "ConnectionWrapper"
    ) -> None:
        self.__c = c
        self.__connection_wrapper = connection_wrapper

    def _clear_result(self) -> None:
        self.__c.execute("")

    def executemany(
        self,
        statement: str,
        parameters: Optional[List[Dict]] = None,
        context: Optional[Any] = None,
    ) -> None:
        if not parameters:
            params = []
        elif isinstance(parameters, list):
            params = parameters
        else:
            params = list(parameters)
        self.__c.executemany(statement, params)

    def execute(
        self,
        statement: str,
        parameters: Optional[Tuple] = None,
        context: Optional[Any] = None,
    ) -> None:
        try:
            norm = statement.strip().lower().rstrip(";")
            if norm == "commit":  # this is largely for ipython-sql
                self.__c.commit()
            elif _BEGIN_TRANSACTION_ISOLATION_RE.fullmatch(norm):
                self.__c.begin()
            elif _SET_TRANSACTION_ISOLATION_RE.fullmatch(norm):
                self._clear_result()
            elif _is_ignored_postgres_config_set(norm):
                self._clear_result()
            elif norm.startswith("register"):
                view_name, df = _parse_register_params(parameters)
                self.__c.register(view_name, df)
            elif norm == "show transaction isolation level":
                self.__c.execute("select 'read committed' as transaction_isolation")
            elif norm == "show standard_conforming_strings":
                self.__c.execute("select 'on' as standard_conforming_strings")
            elif parameters is None:
                self.__c.execute(statement)
            else:
                self.__c.execute(statement, parameters)
        except RuntimeError as e:
            message = str(e)
            if message.startswith("Not implemented Error"):
                raise NotImplementedError(*e.args) from e
            elif (
                message
                == "TransactionContext Error: cannot commit - no transaction is active"
            ):
                return
            else:
                raise e

    @property
    def connection(self) -> Any:
        return self.__connection_wrapper

    def close(self) -> None:
        pass  # closing cursors is not supported in duckdb

    @property
    def description(self) -> Any:
        desc = self.__c.description
        if desc is None:
            return None
        fixed = []
        for col in desc:
            if len(col) >= 2:
                type_code = col[1]
                try:
                    hash(type_code)
                    fixed.append(col)
                except TypeError:
                    fixed.append((col[0], str(type_code), *col[2:]))
            else:
                fixed.append(col)
        return fixed

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__c, name)

    def fetchmany(self, size: Optional[int] = None) -> List:
        if size is None:
            return self.__c.fetchmany()
        else:
            return self.__c.fetchmany(size)


def _is_ignored_postgres_config_set(statement: str) -> bool:
    match = _SET_POSTGRES_CONFIG_RE.match(statement)
    if match is None:
        return False
    return match.group(1) in _IGNORED_POSTGRES_CONFIG_SETTINGS


class DuckDBEngineWarning(Warning):
    pass


@lru_cache()
def _get_reserved_words() -> set[str]:
    return {
        keyword_name
        for (keyword_name,) in duckdb.cursor()
        .execute(
            "select keyword_name from duckdb_keywords() where keyword_category == 'reserved'"
        )
        .fetchall()
    }


def _normalize_execution_options(execution_options: Dict[str, Any]) -> Dict[str, Any]:
    if (
        "duckdb_insertmanyvalues_page_size" in execution_options
        and "insertmanyvalues_page_size" not in execution_options
    ):
        execution_options = dict(execution_options)
        warnings.warn(
            "`duckdb_insertmanyvalues_page_size` is deprecated; use "
            "`insertmanyvalues_page_size` instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        execution_options["insertmanyvalues_page_size"] = execution_options[
            "duckdb_insertmanyvalues_page_size"
        ]
    return execution_options


class DuckDBArrowResult:
    def __init__(self, result: Any) -> None:
        self._result = result
        self._arrow = None

    def _fetch_arrow(self) -> Any:
        if self._arrow is not None:
            return self._arrow
        cursor = getattr(self._result, "cursor", None)
        if cursor is None:
            cursor = getattr(self._result, "_cursor", None)
        if cursor is None:
            raise NotImplementedError("Arrow results are not available on this cursor")
        fetch_arrow_table = getattr(cursor, "to_arrow_table", None)
        if fetch_arrow_table is None:
            fetch_arrow_table = getattr(cursor, "fetch_arrow_table", None)
        if fetch_arrow_table is None:
            raise NotImplementedError("Arrow results are not available on this cursor")
        self._arrow = fetch_arrow_table()
        return self._arrow

    @property
    def arrow(self) -> Any:
        return self._fetch_arrow()

    def all(self) -> Any:
        return self._fetch_arrow()

    def fetchall(self) -> Any:
        return self._fetch_arrow()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._result, name)

    def __iter__(self) -> Any:
        return iter(self._result)


class DuckDBExecutionContext(_PGExecutionContext):
    @classmethod
    def _init_compiled(
        cls,
        dialect: "Dialect",
        connection: Any,
        dbapi_connection: Any,
        execution_options: Dict[str, Any],
        compiled: Any,
        parameters: Any,
        invoked_statement: Any,
        extracted_parameters: Any,
        cache_hit: Any = None,
    ) -> Any:
        execution_options = _normalize_execution_options(execution_options)
        return super()._init_compiled(
            dialect,
            connection,
            dbapi_connection,
            execution_options,
            compiled,
            parameters,
            invoked_statement,
            extracted_parameters,
            cache_hit,
        )

    @classmethod
    def _init_statement(
        cls,
        dialect: "Dialect",
        connection: Any,
        dbapi_connection: Any,
        execution_options: Dict[str, Any],
        statement: str,
        parameters: Any,
    ) -> Any:
        execution_options = _normalize_execution_options(execution_options)
        return super()._init_statement(
            dialect,
            connection,
            dbapi_connection,
            execution_options,
            statement,
            parameters,
        )

    def _setup_result_proxy(self) -> Any:
        arraysize = self.execution_options.get("duckdb_arraysize")
        if arraysize is None:
            arraysize = self.execution_options.get("arraysize")
        cursor = getattr(self, "cursor", None)
        if arraysize is not None and hasattr(cursor, "arraysize"):
            cursor.arraysize = arraysize
        result = super()._setup_result_proxy()
        if self.execution_options.get("duckdb_arrow") and getattr(
            result, "returns_rows", False
        ):
            return DuckDBArrowResult(result)
        return result


def _looks_like_motherduck(database: Optional[str], config: Dict[str, Any]) -> bool:
    if database is not None and (
        database.startswith("md:") or database.startswith("motherduck:")
    ):
        return True
    return any(k in config for k in MOTHERDUCK_CONFIG_KEYS)


DISCONNECT_ERROR_PATTERNS = (
    "connection closed",
    "connection reset",
    "connection refused",
    "broken pipe",
    "socket",
    "network is unreachable",
    "timed out",
    "timeout",
    "could not connect",
    "failed to connect",
)

TRANSIENT_ERROR_PATTERNS = (
    "temporarily unavailable",
    "service unavailable",
    "http error: 429",
    "http error: 503",
    "http error: 504",
    "rate limit",
)

IDEMPOTENT_STATEMENT_PREFIXES = (
    "select",
    "show",
    "describe",
    "pragma",
    "explain",
    "values",
)
MUTATING_STATEMENT_PATTERN = re.compile(
    r"\b("
    r"insert|update|delete|merge|copy|create|alter|drop|grant|revoke|truncate|"
    r"call|attach|detach"
    r")\b"
)


def _strip_leading_sql_comments(statement: str) -> str:
    sql = statement.lstrip()
    while sql:
        if sql.startswith("--"):
            newline_index = sql.find("\n")
            if newline_index == -1:
                return ""
            sql = sql[newline_index + 1 :].lstrip()
            continue
        if sql.startswith("/*"):
            comment_end = sql.find("*/", 2)
            if comment_end == -1:
                return ""
            sql = sql[comment_end + 2 :].lstrip()
            continue
        break
    return sql


def _is_idempotent_statement(statement: str) -> bool:
    normalized = _strip_leading_sql_comments(statement).lower()
    if not normalized:
        return False
    if normalized.startswith(IDEMPOTENT_STATEMENT_PREFIXES):
        return True
    if not normalized.startswith("with"):
        return False
    return MUTATING_STATEMENT_PATTERN.search(normalized) is None


def _is_transient_error(error: BaseException) -> bool:
    message = str(error).lower()
    if any(pattern in message for pattern in DISCONNECT_ERROR_PATTERNS):
        return False
    return any(pattern in message for pattern in TRANSIENT_ERROR_PATTERNS)


def _pool_override_from_url(url: SAURL) -> Optional[str]:
    value = None
    if "duckdb_sqlalchemy_pool" in url.query:
        value = url.query.get("duckdb_sqlalchemy_pool")
    elif "pool" in url.query:
        value = url.query.get("pool")
    if value is None:
        value = os.getenv("DUCKDB_SQLALCHEMY_POOL")
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    if value is None:
        return None
    return str(value).lower()


_POOL_CLASS_OVERRIDES: Dict[str, type[pool.Pool]] = {
    "queue": pool.QueuePool,
    "singleton": pool.SingletonThreadPool,
    "singletonthreadpool": pool.SingletonThreadPool,
    "null": pool.NullPool,
    "nullpool": pool.NullPool,
}


def _pool_class_from_override(
    pool_override: Optional[str],
) -> Optional[type[pool.Pool]]:
    if pool_override is None:
        return None
    return _POOL_CLASS_OVERRIDES.get(pool_override)


def _default_pool_class_for_database(
    database: Optional[str], query: Dict[str, Any]
) -> type[pool.Pool]:
    if database == ":memory:":
        return pool.SingletonThreadPool
    if not database or database.startswith(":memory:"):
        return pool.QueuePool
    if _looks_like_motherduck(database, query):
        return pool.NullPool
    return pool.QueuePool


def _apply_motherduck_defaults(config: Dict[str, Any], database: Optional[str]) -> None:
    if "motherduck_token" not in config:
        token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")
        if token and _looks_like_motherduck(database, config):
            config["motherduck_token"] = token

    if "motherduck_token" in config and not isinstance(config["motherduck_token"], str):
        raise TypeError("motherduck_token must be a string")


def _normalize_motherduck_config(config: Dict[str, Any]) -> None:
    _normalize_config_aliases(config)


class DuckDBIdentifierPreparer(PGIdentifierPreparer):
    def __init__(self, dialect: "Dialect", **kwargs: Any) -> None:
        super().__init__(dialect, **kwargs)
        self.reserved_words.update(_get_reserved_words())

    def _separate(self, name: Optional[str]) -> Tuple[Optional[Any], Optional[str]]:
        """
        Get database name and schema name from schema if it contains a database name
            Format:
              <db_name>.<schema_name>
              db_name and schema_name are double quoted if contains spaces or double quotes
        """
        database_name, schema_name = None, name
        if name is not None and "." in name:
            database_name, schema_name = (
                max(s) for s in re.findall(r'"([^.]+)"|([^.]+)', name)
            )
        return database_name, schema_name

    def format_schema(self, name: str) -> str:
        """Prepare a quoted schema name."""
        database_name, schema_name = self._separate(name)
        if database_name is None or schema_name is None:
            return self.quote(name)
        return ".".join(self.quote(str(_n)) for _n in [database_name, schema_name])

    def quote_schema(self, schema: str, force: Any = None) -> str:
        """
        Conditionally quote a schema name.

        :param schema: string schema name
        :param force: unused
        """
        return self.format_schema(schema)


class DuckDBNullType(sqltypes.NullType):
    def result_processor(self, dialect: Any, coltype: object) -> Any:
        if coltype == "JSON":
            return sqltypes.JSON().result_processor(dialect, coltype)
        else:
            return super().result_processor(dialect, coltype)


class Dialect(PGDialect_psycopg2):
    name = "duckdb"
    driver = "duckdb_sqlalchemy"
    _has_events = False
    supports_statement_cache = True
    supports_comments = False
    supports_sane_rowcount = False
    supports_server_side_cursors = False
    execution_ctx_cls = DuckDBExecutionContext
    div_is_floordiv = False  # TODO: tweak this to be based on DuckDB version
    inspector = DuckDBInspector
    insertmanyvalues_page_size = 1000
    use_insertmanyvalues = SQLALCHEMY_2
    use_insertmanyvalues_wo_returning = SQLALCHEMY_2
    duckdb_copy_threshold = 10000
    _capabilities: "DuckDBCapabilities"
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            # the psycopg2 driver registers a _PGNumeric with custom logic for
            # postgres type_codes (such as 701 for float) that duckdb doesn't have
            sqltypes.Numeric: sqltypes.Numeric,
            sqltypes.JSON: sqltypes.JSON,
            UUID: UUID,
        },
    )
    ischema_names = util.update_copy(
        PGDialect.ischema_names,
        ISCHEMA_NAMES,
    )
    preparer = DuckDBIdentifierPreparer
    identifier_preparer: DuckDBIdentifierPreparer

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["use_native_hstore"] = False
        super().__init__(*args, **kwargs)
        self._capabilities = get_capabilities(duckdb.__version__)

    def initialize(self, connection: "Connection") -> None:
        DefaultDialect.initialize(self, connection)
        self._capabilities = get_capabilities(duckdb.__version__)
        self.supports_comments = has_comment_support()

    @property
    def capabilities(self) -> "DuckDBCapabilities":
        return self._capabilities

    def type_descriptor(
        self, typeobj: sqltypes.TypeEngine[Any]
    ) -> sqltypes.TypeEngine[Any]:
        res = super().type_descriptor(typeobj)

        if isinstance(res, sqltypes.NullType):
            return DuckDBNullType()

        return res

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        core_keys = get_core_config()
        preload_extensions = cparams.pop("preload_extensions", [])
        config = dict(cparams.get("config", {}))
        cparams["config"] = config
        config.update(cparams.pop("url_config", {}))
        for key in DIALECT_QUERY_KEYS:
            config.pop(key, None)
        if cparams.get("database") in {None, ""}:
            cparams["database"] = ":memory:"
        _apply_motherduck_defaults(config, cparams.get("database"))
        path_query = extract_path_query_from_config(config)
        if path_query:
            cparams["database"] = append_query_to_database(
                cparams.get("database"), path_query
            )
        validate_motherduck_database_name(cparams.get("database"))
        _normalize_motherduck_config(config)

        ext = {k: config.pop(k) for k in list(config) if k not in core_keys}
        if supports_user_agent:
            user_agent = (
                f"duckdb-sqlalchemy/{__version__}(sqlalchemy/{sqlalchemy_version})"
            )
            if "custom_user_agent" in config:
                user_agent = f"{user_agent} {config['custom_user_agent']}"
            config["custom_user_agent"] = user_agent

        filesystems = cparams.pop("register_filesystems", [])

        conn = duckdb.connect(*cargs, **cparams)

        for extension in preload_extensions:
            conn.execute(f"LOAD {validate_extension_name(extension)}")

        for filesystem in filesystems:
            conn.register_filesystem(filesystem)

        apply_config(self, conn, ext)

        return ConnectionWrapper(conn)

    def on_connect(self) -> None:
        pass

    @classmethod
    def get_pool_class(cls, url: SAURL) -> type[pool.Pool]:
        pool_class = _pool_class_from_override(_pool_override_from_url(url))
        if pool_class is not None:
            return pool_class
        return _default_pool_class_for_database(url.database, dict(url.query))

    @staticmethod
    def dbapi(**kwargs: Any) -> type[DBAPI]:
        return DBAPI

    def _get_server_version_info(self, connection: "Connection") -> Tuple[int, int]:
        return (8, 0)

    def get_default_isolation_level(self, dbapi_conn):
        raise NotImplementedError()

    def do_rollback(self, dbapi_connection: Any) -> None:
        try:
            super().do_rollback(dbapi_connection)
        except DBAPI.TransactionException as e:
            if (
                e.args[0]
                != "TransactionContext Error: cannot rollback - no transaction is active"
            ):
                raise e

    def do_begin(self, dbapi_connection: Any) -> None:
        dbapi_connection.begin()

    def get_view_names(
        self,
        connection: Any,
        schema: Optional[Any] = None,
        include: Optional[Any] = None,
        **kw: Any,
    ) -> Any:
        s = """
            SELECT table_name
            FROM information_schema.tables
            WHERE
                table_type='VIEW'
                AND table_schema = :schema_name
            """
        params = {}
        database_name = None

        if schema is not None:
            database_name, schema = self.identifier_preparer._separate(schema)
        else:
            schema = "main"

        params.update({"schema_name": schema})

        if database_name is not None:
            s += "AND table_catalog = :database_name\n"
            params.update({"database_name": database_name})

        rs = connection.execute(text(s), params)
        return [view for (view,) in rs]

    @cache  # type: ignore[call-arg]
    def get_schema_names(self, connection: "Connection", **kw: "Any"):  # type: ignore[no-untyped-def]
        """
        Return unquoted database_name.schema_name unless either contains spaces or double quotes.
        In that case, escape double quotes and then wrap in double quotes.
        SQLAlchemy definition of a schema includes database name for databases like SQL Server (Ex: databasename.dbo)
        (see https://docs.sqlalchemy.org/en/20/dialects/mssql.html#multipart-schema-names)
        """

        if not supports_attach:
            return super().get_schema_names(connection, **kw)

        s = """
            SELECT database_name, schema_name AS nspname
            FROM duckdb_schemas()
            WHERE schema_name NOT LIKE 'pg\\_%' ESCAPE '\\'
            ORDER BY database_name, nspname
            """
        rs = connection.execute(text(s))

        qs = self.identifier_preparer.quote_schema
        return [qs(".".join(nspname)) for nspname in rs]

    def _build_query_where(
        self,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None,
    ) -> Tuple[str, Dict[str, str]]:
        sql = ""
        params = {}

        # If no database name is provided, try to get it from the schema name
        # specified as "<db name>.<schema name>"
        # If only a schema name is found, database_name will return None
        if database_name is None and schema_name is not None:
            database_name, schema_name = self.identifier_preparer._separate(schema_name)

        if table_name is not None:
            sql += "AND table_name = :table_name\n"
            params.update({"table_name": table_name})

        if schema_name is not None:
            sql += "AND schema_name = :schema_name\n"
            params.update({"schema_name": schema_name})

        if database_name is not None:
            sql += "AND database_name = :database_name\n"
            params.update({"database_name": database_name})

        return sql, params

    @cache  # type: ignore[call-arg]
    def get_table_names(self, connection: "Connection", schema=None, **kw: "Any"):  # type: ignore[no-untyped-def]
        """
        Return unquoted database_name.schema_name unless either contains spaces or double quotes.
        In that case, escape double quotes and then wrap in double quotes.
        SQLAlchemy definition of a schema includes database name for databases like SQL Server (Ex: databasename.dbo)
        (see https://docs.sqlalchemy.org/en/20/dialects/mssql.html#multipart-schema-names)
        """

        if not supports_attach:
            return super().get_table_names(connection, schema, **kw)

        s = """
            SELECT database_name, schema_name, table_name
            FROM duckdb_tables()
            WHERE schema_name NOT LIKE 'pg\\_%' ESCAPE '\\'
            """
        sql, params = self._build_query_where(schema_name=schema)
        s += sql
        rs = connection.execute(text(s), params)

        return [
            table
            for (
                db,
                sc,
                table,
            ) in rs
        ]

    @cache  # type: ignore[call-arg]
    def get_table_oid(  # type: ignore[no-untyped-def]
        self,
        connection: "Connection",
        table_name: str,
        schema: "Optional[str]" = None,
        **kw: "Any",
    ):
        """Fetch the oid for (database.)schema.table_name.
        The schema name can be formatted either as database.schema or just the schema name.
        In the latter scenario the schema associated with the default database is used.
        """
        s = """
            SELECT oid, table_name
            FROM (
                SELECT table_oid AS oid, table_name,              database_name, schema_name FROM duckdb_tables()
                UNION ALL BY NAME
                SELECT view_oid AS oid , view_name AS table_name, database_name, schema_name FROM duckdb_views()
            )
            WHERE schema_name NOT LIKE 'pg\\_%' ESCAPE '\\'
            """
        sql, params = self._build_query_where(table_name=table_name, schema_name=schema)
        s += sql

        rs = connection.execute(text(s), params)
        table_oid = rs.scalar()
        if table_oid is None:
            raise NoSuchTableError(table_name)
        return table_oid

    def _duckdb_table_exists(
        self, connection: "Connection", table_name: str, schema: Optional[str]
    ) -> bool:
        sql = """
            SELECT 1
            FROM duckdb_tables()
            WHERE 1 = 1
            """
        where_sql, params = self._build_query_where(
            table_name=table_name, schema_name=schema
        )
        sql += where_sql
        return connection.execute(text(sql), params).first() is not None

    def _get_reflection_or_empty_for_existing_table(
        self,
        getter: Callable[[], List[Any]],
        connection: "Connection",
        table_name: str,
        schema: Optional[str],
    ) -> List[Any]:
        try:
            return getter()
        except NoSuchTableError:
            if self._duckdb_table_exists(connection, table_name, schema):
                return []
            raise

    def _duckdb_columns(
        self, connection: "Connection", table_name: str, schema: Optional[str]
    ) -> Optional[List[Dict[str, Any]]]:
        rows = self._duckdb_column_rows(
            connection, schema=schema, filter_names=[table_name]
        )
        if not rows:
            return None
        return self._duckdb_columns_from_rows(connection, rows)[table_name]

    def _duckdb_reflection_stmt(
        self,
        relation: str,
        columns: str,
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        include_internal_filter: bool = False,
        suffix: str = "",
    ) -> Tuple[Any, Dict[str, Any]]:
        sql = f"""
            SELECT {columns}
            FROM {relation}()
            WHERE schema_name NOT LIKE 'pg\\_%' ESCAPE '\\'
            """
        params: Dict[str, Any] = {}
        if include_internal_filter:
            sql += "AND internal = false\n"
        if schema is not None:
            where_sql, where_params = self._build_query_where(schema_name=schema)
            sql += where_sql
            params.update(where_params)
        if filter_names is not None:
            names = list(filter_names)
            if not names:
                sql += "AND 1 = 0\n"
            else:
                sql += "AND table_name IN :filter_names\n"
                params["filter_names"] = names
        if suffix:
            sql += suffix
        stmt = text(sql)
        if params.get("filter_names"):
            stmt = stmt.bindparams(bindparam("filter_names", expanding=True))
        return stmt, params

    def _duckdb_column_rows(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
    ) -> List[Dict[str, Any]]:
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_columns",
            (
                "database_name, schema_name, table_name, column_name, column_default, "
                "is_nullable, data_type, data_type_id, comment, column_index"
            ),
            schema=schema,
            filter_names=filter_names,
            include_internal_filter=True,
            suffix="ORDER BY table_name, column_index",
        )
        return [dict(row) for row in connection.execute(stmt, params).mappings()]

    def _duckdb_table_names(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
    ) -> List[str]:
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_tables",
            "table_name",
            schema=schema,
            filter_names=filter_names,
            include_internal_filter=True,
            suffix="ORDER BY table_name",
        )
        return [row[0] for row in connection.execute(stmt, params)]

    def _duckdb_enum_rows(
        self, connection: "Connection", type_ids: Collection[Any]
    ) -> Dict[Any, Dict[str, Any]]:
        ids = sorted({type_id for type_id in type_ids if type_id is not None})
        if not ids:
            return {}
        stmt = text(
            """
            SELECT type_oid, type_name, labels
            FROM duckdb_types()
            WHERE type_oid IN :type_ids
            """
        ).bindparams(bindparam("type_ids", expanding=True))
        return {
            row["type_oid"]: dict(row)
            for row in connection.execute(stmt, {"type_ids": ids}).mappings()
        }

    def _split_duckdb_list(self, value: str) -> List[str]:
        items: List[str] = []
        current: List[str] = []
        depth = 0
        in_single_quote = False
        in_double_quote = False
        index = 0
        while index < len(value):
            char = value[index]
            if in_single_quote:
                current.append(char)
                if char == "'" and index + 1 < len(value) and value[index + 1] == "'":
                    current.append(value[index + 1])
                    index += 1
                elif char == "'":
                    in_single_quote = False
            elif in_double_quote:
                current.append(char)
                if char == '"' and index + 1 < len(value) and value[index + 1] == '"':
                    current.append(value[index + 1])
                    index += 1
                elif char == '"':
                    in_double_quote = False
            else:
                if char == "'":
                    in_single_quote = True
                    current.append(char)
                elif char == '"':
                    in_double_quote = True
                    current.append(char)
                elif char in "([":
                    depth += 1
                    current.append(char)
                elif char in ")]":
                    depth = max(0, depth - 1)
                    current.append(char)
                elif char == "," and depth == 0:
                    item = "".join(current).strip()
                    if item:
                        items.append(item)
                    current = []
                else:
                    current.append(char)
            index += 1
        item = "".join(current).strip()
        if item:
            items.append(item)
        return items

    def _parse_duckdb_enum_labels(
        self, data_type: str, enum_row: Optional[Dict[str, Any]]
    ) -> Tuple[List[str], Optional[str]]:
        if enum_row is not None and enum_row.get("labels"):
            enum_name = enum_row.get("type_name")
            if isinstance(enum_name, str) and enum_name.lower() == "enum":
                enum_name = None
            return list(enum_row["labels"]), cast(Optional[str], enum_name)

        inner = data_type[len("ENUM(") : -1]
        labels = [
            self._unquote_duckdb_string(token)
            for token in self._split_duckdb_list(inner)
        ]
        return labels, None

    def _unquote_duckdb_string(self, value: str) -> str:
        value = value.strip()
        if value.startswith("'") and value.endswith("'"):
            return value[1:-1].replace("''", "'")
        return value

    def _unquote_duckdb_identifier(self, value: str) -> Optional[str]:
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1].replace('""', '"')
        return None

    def _reflect_duckdb_index_expressions(
        self,
        raw_expressions: Any,
    ) -> Tuple[List[str], List[Optional[str]]]:
        raw = str(raw_expressions or "").strip()
        inner = raw[1:-1] if raw.startswith("[") and raw.endswith("]") else raw
        expressions = self._split_duckdb_list(inner)
        column_names: List[Optional[str]] = []
        for expression in expressions:
            candidate = self._unquote_duckdb_string(expression)
            identifier = self._unquote_duckdb_identifier(candidate)
            if identifier is not None:
                column_names.append(identifier)
            elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", candidate):
                column_names.append(candidate)
            else:
                column_names.append(None)
        return expressions, column_names

    def _reflect_duckdb_data_type(
        self,
        data_type: str,
        data_type_id: Optional[Any],
        enum_rows: Dict[Any, Dict[str, Any]],
        type_description: str,
    ) -> Any:
        normalized = data_type.strip()
        dimensions = 0
        while True:
            match = re.search(r"\[[0-9]*\]$", normalized)
            if match is None:
                break
            dimensions += 1
            normalized = normalized[: match.start()].strip()

        upper = normalized.upper()
        if upper == "JSON":
            reflected = sqltypes.JSON()
        elif upper == "BLOB":
            reflected = sqltypes.LargeBinary()
        elif upper.startswith(("DECIMAL(", "NUMERIC(")) and normalized.endswith(")"):
            precision_scale = normalized[normalized.index("(") + 1 : -1]
            parts = [part.strip() for part in precision_scale.split(",", 1)]
            precision = int(parts[0]) if parts[0] else None
            scale = int(parts[1]) if len(parts) > 1 and parts[1] else None
            reflected = sqltypes.Numeric(precision=precision, scale=scale)
        elif upper in {"DECIMAL", "NUMERIC"}:
            reflected = sqltypes.Numeric()
        elif upper.startswith("ENUM(") and normalized.endswith(")"):
            labels, enum_name = self._parse_duckdb_enum_labels(
                normalized, enum_rows.get(data_type_id)
            )
            reflected = sqltypes.Enum(*labels, name=enum_name)
        elif upper.startswith(("STRUCT(", "MAP(", "UNION(")):
            reflected = sqltypes.NULLTYPE
        else:
            reflected = self._reflect_type(  # type: ignore[attr-defined]
                normalized,
                {},
                {},
                type_description=type_description,
                collation=None,
            )

        if dimensions:
            if reflected == sqltypes.NULLTYPE:
                return sqltypes.NULLTYPE
            return sqltypes.ARRAY(reflected, dimensions=dimensions)
        return reflected

    def _duckdb_columns_from_rows(
        self, connection: "Connection", rows: Sequence[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        enum_rows = self._duckdb_enum_rows(
            connection, [row["data_type_id"] for row in rows]
        )
        columns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            columns[row["table_name"]].append(
                {
                    "name": row["column_name"],
                    "type": self._reflect_duckdb_data_type(
                        row["data_type"],
                        row.get("data_type_id"),
                        enum_rows,
                        type_description=f"column '{row['column_name']}'",
                    ),
                    "nullable": bool(row["is_nullable"]),
                    "default": row["column_default"],
                    "autoincrement": False,
                    "comment": row["comment"],
                }
            )
        return dict(columns)

    def _reflection_schema_key(self, schema: Optional[str]) -> Optional[str]:
        return schema

    def _get_single_reflection_result(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str],
        get_multi: Any,
        default_factory: Any,
        **kw: Any,
    ) -> Any:
        scope = kw.pop("scope", None)
        kind = kw.pop("kind", None)
        reflected = dict(
            get_multi(
                connection,
                schema=schema,
                filter_names=[table_name],
                scope=scope,
                kind=kind,
                **kw,
            )
        )
        key = (self._reflection_schema_key(schema), table_name)
        if key in reflected:
            return reflected[key]
        if self._duckdb_table_exists(connection, table_name, schema):
            return default_factory()
        raise NoSuchTableError(table_name)

    def has_table(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> bool:
        try:
            return self.get_table_oid(connection, table_name, schema) is not None
        except NoSuchTableError:
            return False

    @cache  # type: ignore[call-arg]
    def get_columns(  # type: ignore[no-untyped-def]
        self, connection: "Connection", table_name: str, schema=None, **kw: "Any"
    ):
        columns = self._duckdb_columns(connection, table_name, schema)
        if columns is None:
            raise NoSuchTableError(table_name)
        return columns

    @cache  # type: ignore[call-arg]
    def get_pk_constraint(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> "ReflectedPrimaryKeyConstraint":
        return self._get_single_reflection_result(
            connection,
            table_name,
            schema,
            self.get_multi_pk_constraint,
            ReflectionDefaults.pk_constraint,
            **kw,
        )

    def get_multi_pk_constraint(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        **kw: Any,
    ) -> Iterable[Tuple[Any, Any]]:
        table_names = self._duckdb_table_names(
            connection, schema=schema, filter_names=filter_names
        )
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_constraints",
            "table_name, constraint_name, constraint_column_names",
            schema=schema,
            filter_names=filter_names,
            suffix=(
                "AND constraint_type = 'PRIMARY KEY'\n"
                "ORDER BY table_name, constraint_index"
            ),
        )
        constraint_rows = list(connection.execute(stmt, params).mappings())
        constraints = {
            row["table_name"]: {
                "name": row["constraint_name"],
                "constrained_columns": list(row["constraint_column_names"] or []),
            }
            for row in constraint_rows
        }
        schema_key = self._reflection_schema_key(schema)
        return (
            (
                (schema_key, table_name),
                constraints.get(table_name, ReflectionDefaults.pk_constraint()),
            )
            for table_name in table_names
        )

    @cache  # type: ignore[call-arg]
    def get_foreign_keys(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        postgresql_ignore_search_path: bool = False,
        **kw: Any,
    ) -> List["ReflectedForeignKeyConstraint"]:
        super_get_foreign_keys = super().get_foreign_keys
        return self._get_reflection_or_empty_for_existing_table(
            lambda: super_get_foreign_keys(
                connection,
                table_name,
                schema=schema,
                postgresql_ignore_search_path=postgresql_ignore_search_path,
                **kw,
            ),
            connection,
            table_name,
            schema,
        )

    @cache  # type: ignore[call-arg]
    def get_unique_constraints(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List["ReflectedUniqueConstraint"]:
        super_get_unique_constraints = super().get_unique_constraints
        return self._get_reflection_or_empty_for_existing_table(
            lambda: super_get_unique_constraints(
                connection, table_name, schema=schema, **kw
            ),
            connection,
            table_name,
            schema,
        )

    @cache  # type: ignore[call-arg]
    def get_check_constraints(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List["ReflectedCheckConstraint"]:
        super_get_check_constraints = super().get_check_constraints
        return self._get_reflection_or_empty_for_existing_table(
            lambda: super_get_check_constraints(
                connection, table_name, schema=schema, **kw
            ),
            connection,
            table_name,
            schema,
        )

    def get_indexes(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List["ReflectedIndex"]:  # type: ignore[override]
        return self._get_single_reflection_result(
            connection,
            table_name,
            schema,
            self.get_multi_indexes,
            ReflectionDefaults.indexes,
            **kw,
        )

    # the following methods are for SQLA2 compatibility
    def get_multi_indexes(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        **kw: Any,
    ) -> Iterable[Tuple[Any, Any]]:
        table_names = self._duckdb_table_names(
            connection, schema=schema, filter_names=filter_names
        )
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_indexes",
            "table_name, index_name, expressions, is_unique",
            schema=schema,
            filter_names=filter_names,
            suffix="ORDER BY table_name, index_name",
        )
        index_rows = list(connection.execute(stmt, params).mappings())
        indexes: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in index_rows:
            expressions, column_names = self._reflect_duckdb_index_expressions(
                row["expressions"]
            )
            reflected_index: Dict[str, Any] = {
                "name": row["index_name"],
                "column_names": column_names,
                "unique": bool(row["is_unique"]),
            }
            if any(column_name is None for column_name in column_names):
                reflected_index["expressions"] = expressions
            indexes[row["table_name"]].append(reflected_index)
        schema_key = self._reflection_schema_key(schema)
        return (
            ((schema_key, table_name), indexes.get(table_name, []))
            for table_name in table_names
        )

    def create_connect_args(self, url: SAURL) -> Tuple[tuple, dict]:
        opts = url.translate_connect_args(database="database")
        path_query, url_config = split_url_query(dict(url.query))
        opts["url_config"] = url_config
        database = opts.get("database")
        if database in {None, ""}:
            database = ":memory:"
        validate_motherduck_database_name(database)
        opts["database"] = append_query_to_database(database, path_query)
        return (), opts

    @classmethod
    def import_dbapi(cls) -> Any:
        return cls.dbapi()

    def _get_execution_options(self, context: Optional[Any]) -> Dict[str, Any]:
        if context is None:
            return {}
        return getattr(context, "execution_options", {}) or {}

    def _bulk_insert_via_register(
        self,
        cursor: Any,
        context: Any,
        parameters: Sequence[Any],
    ) -> bool:
        if not parameters:
            return False
        compiled = getattr(context, "compiled", None)
        if compiled is None:
            return False
        if getattr(compiled, "effective_returning", None):
            return False
        stmt = getattr(compiled, "statement", None)
        table = getattr(stmt, "table", None)
        if table is None:
            return False
        if getattr(stmt, "_post_values_clause", None) is not None:
            return False

        column_keys = getattr(compiled, "column_keys", None)
        if not column_keys:
            column_keys = getattr(compiled, "positiontup", None)
        if not column_keys and isinstance(parameters[0], dict):
            column_keys = list(parameters[0].keys())
        if not column_keys:
            return False

        column_names = [
            str(getattr(column_key, "key", column_key)) for column_key in column_keys
        ]
        rows = parameters if isinstance(parameters, list) else list(parameters)
        data = _build_bulk_insert_data(rows, column_names)
        if data is None:
            return False

        view_name = f"__duckdb_sa_bulk_{uuid.uuid4().hex}"
        dbapi_conn = cursor.connection
        dbapi_conn.register(view_name, data)
        preparer = getattr(context, "identifier_preparer", self.identifier_preparer)
        target = preparer.format_table(table)
        columns = ", ".join(preparer.quote(col) for col in column_names)
        insert_sql = (
            f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {view_name}"
        )
        try:
            cursor.execute(insert_sql)
        finally:
            try:
                dbapi_conn.unregister(view_name)
            except Exception:
                pass
        return True

    def do_executemany(
        self, cursor: Any, statement: Any, parameters: Any, context: Optional[Any] = ...
    ) -> None:
        if (
            context is not None
            and getattr(context, "isinsert", False)
            and parameters
            and isinstance(parameters, (list, tuple))
        ):
            options = self._get_execution_options(context)
            copy_threshold = options.get(
                "duckdb_copy_threshold", self.duckdb_copy_threshold
            )
            if copy_threshold and len(parameters) >= copy_threshold:
                if self._bulk_insert_via_register(cursor, context, parameters):
                    return None
        return DefaultDialect.do_executemany(
            self, cursor, statement, parameters, context
        )

    def is_disconnect(self, e: Exception, connection: Any, cursor: Any) -> bool:
        message = str(e).lower()
        return any(pattern in message for pattern in DISCONNECT_ERROR_PATTERNS)

    def _execute_with_retry(
        self,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Optional[Any],
        executor: Any,
    ) -> Any:
        options = self._get_execution_options(context)
        retry_count = int(options.get("duckdb_retry_count", 0) or 0)
        if options.get("duckdb_retry_on_transient") and retry_count == 0:
            retry_count = 1
        if retry_count <= 0 or not _is_idempotent_statement(statement):
            return executor()
        backoff = options.get("duckdb_retry_backoff")
        attempt = 0
        while True:
            try:
                return executor()
            except Exception as exc:
                if attempt >= retry_count or not _is_transient_error(exc):
                    raise
                attempt += 1
                if backoff:
                    time.sleep(float(backoff))

    def do_execute(
        self,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Optional[Any] = None,
    ) -> None:
        def executor() -> Any:
            return DefaultDialect.do_execute(
                self, cursor, statement, parameters, context
            )

        self._execute_with_retry(cursor, statement, parameters, context, executor)

    def do_execute_no_params(
        self, cursor: Any, statement: str, context: Optional[Any] = None
    ) -> None:
        def executor() -> Any:
            return DefaultDialect.do_execute_no_params(self, cursor, statement, context)

        self._execute_with_retry(cursor, statement, None, context, executor)

    def _pg_class_filter_scope_schema(
        self,
        query: Select,
        schema: Optional[str],
        scope: Any,
        pg_class_table: Any = None,
    ) -> Any:
        # Scope by schema, but strip any database prefix (DuckDB uses db.schema).
        # This will not work if a schema or table name is not unique!
        if hasattr(super(), "_pg_class_filter_scope_schema"):
            schema_arg = schema
            if schema is not None:
                _, schema_name = self.identifier_preparer._separate(schema)
                schema_arg = schema_name
            return getattr(super(), "_pg_class_filter_scope_schema")(
                query,
                schema=schema_arg,
                scope=scope,
                pg_class_table=pg_class_table,
            )

    @lru_cache()
    def _columns_query(self, schema, has_filter_names, scope, kind):  # type: ignore[no-untyped-def]
        if not SQLALCHEMY_2:
            return super()._columns_query(schema, has_filter_names, scope, kind)  # type: ignore[misc]

        # DuckDB versions before 1.4 don't expose pg_collation; skip collation
        # reflection to avoid Catalog Errors during SQLAlchemy 2.x reflection.
        from sqlalchemy.dialects.postgresql import base as pg_base

        pg_catalog = getattr(pg_base, "pg_catalog")
        REGCLASS = getattr(pg_base, "REGCLASS")
        TEXT = getattr(pg_base, "TEXT")
        OID = getattr(pg_base, "OID")

        server_version_info = self.server_version_info or (0,)

        generated = (
            pg_catalog.pg_attribute.c.attgenerated.label("generated")
            if server_version_info >= (12,)
            else sql.null().label("generated")
        )
        if server_version_info >= (10,):
            identity = (
                select(
                    sql.func.json_build_object(
                        "always",
                        pg_catalog.pg_attribute.c.attidentity == "a",
                        "start",
                        pg_catalog.pg_sequence.c.seqstart,
                        "increment",
                        pg_catalog.pg_sequence.c.seqincrement,
                        "minvalue",
                        pg_catalog.pg_sequence.c.seqmin,
                        "maxvalue",
                        pg_catalog.pg_sequence.c.seqmax,
                        "cache",
                        pg_catalog.pg_sequence.c.seqcache,
                        "cycle",
                        pg_catalog.pg_sequence.c.seqcycle,
                        type_=sqltypes.JSON(),
                    )
                )
                .select_from(pg_catalog.pg_sequence)
                .where(
                    pg_catalog.pg_attribute.c.attidentity != "",
                    pg_catalog.pg_sequence.c.seqrelid
                    == sql.cast(
                        sql.cast(
                            pg_catalog.pg_get_serial_sequence(
                                sql.cast(
                                    sql.cast(
                                        pg_catalog.pg_attribute.c.attrelid,
                                        REGCLASS,
                                    ),
                                    TEXT,
                                ),
                                pg_catalog.pg_attribute.c.attname,
                            ),
                            REGCLASS,
                        ),
                        OID,
                    ),
                )
                .correlate(pg_catalog.pg_attribute)
                .scalar_subquery()
                .label("identity_options")
            )
        else:
            identity = sql.null().label("identity_options")

        default = (
            select(
                pg_catalog.pg_get_expr(
                    pg_catalog.pg_attrdef.c.adbin,
                    pg_catalog.pg_attrdef.c.adrelid,
                )
            )
            .select_from(pg_catalog.pg_attrdef)
            .where(
                pg_catalog.pg_attrdef.c.adrelid == pg_catalog.pg_attribute.c.attrelid,
                pg_catalog.pg_attrdef.c.adnum == pg_catalog.pg_attribute.c.attnum,
                pg_catalog.pg_attribute.c.atthasdef,
            )
            .correlate(pg_catalog.pg_attribute)
            .scalar_subquery()
            .label("default")
        )

        collate = sql.null().label("collation")

        relkinds = getattr(super(), "_kind_to_relkinds")(kind)
        query = (
            select(
                pg_catalog.pg_attribute.c.attname.label("name"),
                pg_catalog.format_type(
                    pg_catalog.pg_attribute.c.atttypid,
                    pg_catalog.pg_attribute.c.atttypmod,
                ).label("format_type"),
                default,
                pg_catalog.pg_attribute.c.attnotnull.label("not_null"),
                pg_catalog.pg_class.c.relname.label("table_name"),
                pg_catalog.pg_description.c.description.label("comment"),
                generated,
                identity,
                collate,
            )
            .select_from(pg_catalog.pg_class)
            .outerjoin(
                pg_catalog.pg_attribute,
                sql.and_(
                    pg_catalog.pg_class.c.oid == pg_catalog.pg_attribute.c.attrelid,
                    pg_catalog.pg_attribute.c.attnum > 0,
                    ~pg_catalog.pg_attribute.c.attisdropped,
                ),
            )
            .outerjoin(
                pg_catalog.pg_description,
                sql.and_(
                    pg_catalog.pg_description.c.objoid
                    == pg_catalog.pg_attribute.c.attrelid,
                    pg_catalog.pg_description.c.objsubid
                    == pg_catalog.pg_attribute.c.attnum,
                ),
            )
            .where(getattr(super(), "_pg_class_relkind_condition")(relkinds))
            .order_by(pg_catalog.pg_class.c.relname, pg_catalog.pg_attribute.c.attnum)
        )
        query = self._pg_class_filter_scope_schema(query, schema, scope=scope)
        if has_filter_names:
            query = query.where(
                pg_catalog.pg_class.c.relname.in_(bindparam("filter_names"))
            )
        return query

    # FIXME: this method is a hack around the fact that we use a single cursor for all queries inside a connection,
    #   and this is required to fix get_multi_columns
    def get_multi_columns(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        **kw: Any,
    ) -> Any:
        rows = self._duckdb_column_rows(
            connection, schema=schema, filter_names=filter_names
        )
        columns = self._duckdb_columns_from_rows(connection, rows)
        schema_key = self._reflection_schema_key(schema)
        return (
            ((schema_key, table_name), table_columns)
            for table_name, table_columns in columns.items()
        )

    # fix for https://github.com/leonardovida/duckdb-sqlalchemy/issues/1128
    # (Overrides sqlalchemy method)
    @lru_cache()
    def _comment_query(  # type: ignore[no-untyped-def]
        self, schema: str, has_filter_names: bool, scope: Any, kind: Any
    ):
        if SQLALCHEMY_VERSION >= Version("2.0.36"):
            from sqlalchemy.dialects.postgresql import base as pg_base

            pg_catalog = getattr(pg_base, "pg_catalog")

            if (
                hasattr(super(), "_kind_to_relkinds")
                and hasattr(super(), "_pg_class_filter_scope_schema")
                and hasattr(super(), "_pg_class_relkind_condition")
            ):
                relkinds = getattr(super(), "_kind_to_relkinds")(kind)
                query = (
                    select(
                        pg_catalog.pg_class.c.relname,
                        pg_catalog.pg_description.c.description,
                    )
                    .select_from(pg_catalog.pg_class)
                    .outerjoin(
                        pg_catalog.pg_description,
                        sql.and_(
                            pg_catalog.pg_class.c.oid
                            == pg_catalog.pg_description.c.objoid,
                            pg_catalog.pg_description.c.objsubid == 0,
                        ),
                    )
                    .where(getattr(super(), "_pg_class_relkind_condition")(relkinds))
                )
                query = self._pg_class_filter_scope_schema(query, schema, scope)
                if has_filter_names:
                    query = query.where(
                        pg_catalog.pg_class.c.relname.in_(bindparam("filter_names"))
                    )
                return query
        else:
            if hasattr(super(), "_comment_query"):
                return getattr(super(), "_comment_query")(
                    schema, has_filter_names, scope, kind
                )


if SQLALCHEMY_VERSION >= Version("2.0.14"):
    from sqlalchemy import TryCast  # type: ignore[attr-defined]

    @compiles(TryCast, "duckdb")  # type: ignore[misc]
    def visit_try_cast(
        instance: TryCast,
        compiler: Any,
        **kw: Any,
    ) -> str:
        return "TRY_CAST({} AS {})".format(
            compiler.process(instance.clause, **kw),
            compiler.process(instance.typeclause, **kw),
        )
