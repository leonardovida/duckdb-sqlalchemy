import os
import re
import time
import uuid
import warnings
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
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
from sqlalchemy.engine.reflection import cache
from sqlalchemy.engine.url import URL as SAURL
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import bindparam
from sqlalchemy.sql.selectable import Select

from ._supports import has_comment_support
from .bulk import copy_from_csv, copy_from_parquet, copy_from_rows
from .capabilities import get_capabilities
from .config import apply_config, get_core_config
from .datatypes import ISCHEMA_NAMES, register_extension_types
from .motherduck import (
    DIALECT_QUERY_KEYS,
    MotherDuckURL,
    append_query_to_database,
    create_engine_from_paths,
    create_motherduck_engine,
    extract_path_query_from_config,
    split_url_query,
    stable_session_hint,
)
from .olap import read_csv, read_csv_auto, read_parquet, table_function
from .url import URL, make_url

try:
    from sqlalchemy.dialects.postgresql.base import PGExecutionContext
except ImportError:  # pragma: no cover - fallback for older SQLAlchemy
    PGExecutionContext = DefaultExecutionContext

__version__ = "0.19.2"
sqlalchemy_version = sqlalchemy.__version__
SQLALCHEMY_VERSION = Version(sqlalchemy_version)
SQLALCHEMY_2 = SQLALCHEMY_VERSION >= Version("2.0.0")
duckdb_version: str = duckdb.__version__
_capabilities = get_capabilities(duckdb_version)
supports_attach: bool = _capabilities.supports_attach
supports_user_agent: bool = _capabilities.supports_user_agent

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.engine.reflection import ReflectedCheckConstraint, ReflectedIndex

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
    "table_function",
    "read_parquet",
    "read_csv",
    "read_csv_auto",
    "copy_from_parquet",
    "copy_from_csv",
    "copy_from_rows",
]


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
    ) -> List["ReflectedCheckConstraint"]:
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
            elif norm.startswith("register"):
                view_name, df = _parse_register_params(parameters)
                self.__c.register(view_name, df)
            elif norm == "show transaction isolation level":
                self.__c.execute("select 'read committed' as transaction_isolation")
            elif parameters is None:
                self.__c.execute(statement)
            else:
                self.__c.execute(statement, parameters)
        except RuntimeError as e:
            if e.args[0].startswith("Not implemented Error"):
                raise NotImplementedError(*e.args) from e
            elif (
                e.args[0]
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


class DuckDBEngineWarning(Warning):
    pass


def index_warning() -> None:
    warnings.warn(
        "duckdb-sqlalchemy doesn't yet support reflection on indices",
        DuckDBEngineWarning,
    )


def _normalize_execution_options(execution_options: Dict[str, Any]) -> Dict[str, Any]:
    if (
        "duckdb_insertmanyvalues_page_size" in execution_options
        and "insertmanyvalues_page_size" not in execution_options
    ):
        execution_options = dict(execution_options)
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
        if cursor is None or not hasattr(cursor, "fetch_arrow_table"):
            raise NotImplementedError("Arrow results are not available on this cursor")
        self._arrow = cursor.fetch_arrow_table()
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


class DuckDBExecutionContext(PGExecutionContext):
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
        if arraysize is not None and hasattr(self.cursor, "arraysize"):
            self.cursor.arraysize = arraysize
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
    motherduck_keys = {
        "motherduck_token",
        "attach_mode",
        "saas_mode",
        "session_hint",
        "access_mode",
        "dbinstance_inactivity_ttl",
        "motherduck_dbinstance_inactivity_ttl",
    }
    return any(k in config for k in motherduck_keys)


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


def _is_idempotent_statement(statement: str) -> bool:
    normalized = statement.strip().lower()
    return normalized.startswith(("select", "show", "describe", "pragma", "explain"))


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


def _apply_motherduck_defaults(config: Dict[str, Any], database: Optional[str]) -> None:
    if "motherduck_token" not in config:
        token = os.getenv("motherduck_token") or os.getenv("MOTHERDUCK_TOKEN")
        if token and _looks_like_motherduck(database, config):
            config["motherduck_token"] = token

    if "motherduck_token" in config and not isinstance(config["motherduck_token"], str):
        raise TypeError("motherduck_token must be a string")


def _normalize_motherduck_config(config: Dict[str, Any]) -> None:
    if (
        "dbinstance_inactivity_ttl" in config
        and "motherduck_dbinstance_inactivity_ttl" not in config
    ):
        config["motherduck_dbinstance_inactivity_ttl"] = config[
            "dbinstance_inactivity_ttl"
        ]


class DuckDBIdentifierPreparer(PGIdentifierPreparer):
    def __init__(self, dialect: "Dialect", **kwargs: Any) -> None:
        super().__init__(dialect, **kwargs)

        self.reserved_words.update(
            {
                keyword_name
                for (keyword_name,) in duckdb.cursor()
                .execute(
                    "select keyword_name from duckdb_keywords() where keyword_category == 'reserved'"
                )
                .fetchall()
            }
        )

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

    def type_descriptor(self, typeobj: Type[sqltypes.TypeEngine]) -> Any:  # type: ignore[override]
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
            conn.execute(f"LOAD {extension}")

        for filesystem in filesystems:
            conn.register_filesystem(filesystem)

        apply_config(self, conn, ext)

        return ConnectionWrapper(conn)

    def on_connect(self) -> None:
        pass

    @classmethod
    def get_pool_class(cls, url: SAURL) -> Type[pool.Pool]:
        pool_override = _pool_override_from_url(url)
        if pool_override == "queue":
            return pool.QueuePool
        if pool_override in {"singleton", "singletonthreadpool"}:
            return pool.SingletonThreadPool
        if pool_override in {"null", "nullpool"}:
            return pool.NullPool
        if not url.database:
            return pool.SingletonThreadPool
        if url.database and url.database.startswith(":memory:"):
            return pool.SingletonThreadPool
        if _looks_like_motherduck(url.database, dict(url.query)):
            return pool.NullPool
        return pool.NullPool

    @staticmethod
    def dbapi(**kwargs: Any) -> Type[DBAPI]:
        return DBAPI

    def _get_server_version_info(self, connection: "Connection") -> Tuple[int, int]:
        return (8, 0)

    def get_default_isolation_level(self, dbapi_conn):
        return self.get_isolation_level(dbapi_conn)

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
            WHERE table_name = :table_name
            """
        params: Dict[str, Any] = {"table_name": table_name}
        if schema is not None:
            database_name, schema_name = self.identifier_preparer._separate(schema)
            sql += "AND schema_name = :schema_name\n"
            params["schema_name"] = schema_name
            if database_name is not None:
                sql += "AND database_name = :database_name\n"
                params["database_name"] = database_name
        return connection.execute(text(sql), params).first() is not None

    def _duckdb_columns(
        self, connection: "Connection", table_name: str, schema: Optional[str]
    ) -> Optional[List[Dict[str, Any]]]:
        sql = """
            SELECT column_name, column_default, is_nullable, data_type, comment, column_index
            FROM duckdb_columns()
            WHERE table_name = :table_name
            """
        params: Dict[str, Any] = {"table_name": table_name}
        if schema is not None:
            database_name, schema_name = self.identifier_preparer._separate(schema)
            sql += "AND schema_name = :schema_name\n"
            params["schema_name"] = schema_name
            if database_name is not None:
                sql += "AND database_name = :database_name\n"
                params["database_name"] = database_name
        sql += "ORDER BY column_index"
        rows = list(connection.execute(text(sql), params).mappings())
        if not rows:
            return None
        columns: List[Dict[str, Any]] = []
        for row in rows:
            coltype = self._reflect_type(  # type: ignore[attr-defined]
                row["data_type"],
                {},
                {},
                type_description=f"column '{row['column_name']}'",
                collation=None,
            )
            columns.append(
                {
                    "name": row["column_name"],
                    "type": coltype,
                    "nullable": bool(row["is_nullable"]),
                    "default": row["column_default"],
                    "autoincrement": False,
                    "comment": row["comment"],
                }
            )
        return columns

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
        self, connection: "Connection", table_name: str, schema=None, **kw: Any
    ):
        try:
            return super().get_columns(connection, table_name, schema=schema, **kw)
        except NoSuchTableError:
            columns = self._duckdb_columns(connection, table_name, schema)
            if columns is None:
                raise
            return columns

    @cache  # type: ignore[call-arg]
    def get_foreign_keys(  # type: ignore[no-untyped-def]
        self, connection: "Connection", table_name: str, schema=None, **kw: Any
    ):
        try:
            return super().get_foreign_keys(connection, table_name, schema=schema, **kw)
        except NoSuchTableError:
            if self._duckdb_table_exists(connection, table_name, schema):
                return []
            raise

    @cache  # type: ignore[call-arg]
    def get_unique_constraints(  # type: ignore[no-untyped-def]
        self, connection: "Connection", table_name: str, schema=None, **kw: Any
    ):
        try:
            return super().get_unique_constraints(
                connection, table_name, schema=schema, **kw
            )
        except NoSuchTableError:
            if self._duckdb_table_exists(connection, table_name, schema):
                return []
            raise

    @cache  # type: ignore[call-arg]
    def get_check_constraints(  # type: ignore[no-untyped-def]
        self, connection: "Connection", table_name: str, schema=None, **kw: Any
    ):
        try:
            return super().get_check_constraints(
                connection, table_name, schema=schema, **kw
            )
        except NoSuchTableError:
            if self._duckdb_table_exists(connection, table_name, schema):
                return []
            raise

    def get_indexes(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List["ReflectedIndex"]:
        index_warning()
        return []

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
        index_warning()
        return []

    def create_connect_args(self, url: SAURL) -> Tuple[tuple, dict]:
        opts = url.translate_connect_args(database="database")
        path_query, url_config = split_url_query(dict(url.query))
        opts["url_config"] = url_config
        opts["database"] = append_query_to_database(opts.get("database"), path_query)
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
            getattr(column_key, "key", column_key) for column_key in column_keys
        ]

        data = None
        if isinstance(parameters[0], dict):
            try:
                import pandas as pd  # type: ignore[import-not-found]

                rows = parameters if isinstance(parameters, list) else list(parameters)
                data = pd.DataFrame.from_records(rows, columns=column_names)
            except Exception:
                data = None
            if data is None:
                try:
                    import pyarrow as pa  # type: ignore[import-not-found]

                    rows = (
                        parameters if isinstance(parameters, list) else list(parameters)
                    )
                    data = pa.Table.from_pylist(rows)
                    if column_names:
                        data = data.select(column_names)
                except Exception:
                    return False
        else:
            try:
                import pandas as pd  # type: ignore[import-not-found]

                rows = parameters if isinstance(parameters, list) else list(parameters)
                data = pd.DataFrame(rows, columns=column_names)
            except Exception:
                data = None
            if data is None:
                try:
                    import pyarrow as pa  # type: ignore[import-not-found]

                    rows = (
                        parameters if isinstance(parameters, list) else list(parameters)
                    )
                    if rows:
                        cols = list(zip(*rows))
                    else:
                        cols = [[] for _ in column_names]
                    data = pa.Table.from_arrays(cols, names=column_names)
                except Exception:
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
        self,
        cursor: Any,
        statement: str,
        context: Optional[Any] = None,
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

        pg_catalog = pg_base.pg_catalog
        REGCLASS = pg_base.REGCLASS
        TEXT = pg_base.TEXT
        OID = pg_base.OID

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

        relkinds = self._kind_to_relkinds(kind)
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
            .where(self._pg_class_relkind_condition(relkinds))
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
        """
        Copyright 2005-2023 SQLAlchemy authors and contributors <see AUTHORS file>.

        Permission is hereby granted, free of charge, to any person obtaining a copy of
        this software and associated documentation files (the "Software"), to deal in
        the Software without restriction, including without limitation the rights to
        use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
        of the Software, and to permit persons to whom the Software is furnished to do
        so, subject to the following conditions:

        The above copyright notice and this permission notice shall be included in all
        copies or substantial portions of the Software.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        SOFTWARE.
        """

        scope = kw.get("scope", scope)
        kind = kw.get("kind", kind)
        has_filter_names, params = self._prepare_filter_names(filter_names)  # type: ignore[attr-defined]
        query = self._columns_query(schema, has_filter_names, scope, kind)  # type: ignore[attr-defined]
        rows = list(connection.execute(query, params).mappings())

        # dictionary with (name, ) if default search path or (schema, name)
        # as keys
        domains: Dict[tuple, dict] = {}
        """
        TODO: fix these pg_collation errors in SQLA2
        domains = {
            ((d["schema"], d["name"]) if not d["visible"] else (d["name"],)): d
            for d in self._load_domains(  # type: ignore[attr-defined]
                connection, schema="*", info_cache=kw.get("info_cache")
            )
        }
        """

        # dictionary with (name, ) if default search path or (schema, name)
        # as keys
        enums = dict(
            (
                ((rec["name"],), rec)
                if rec["visible"]
                else ((rec["schema"], rec["name"]), rec)
            )
            for rec in self._load_enums(  # type: ignore[attr-defined]
                connection, schema="*", info_cache=kw.get("info_cache")
            )
        )

        columns = self._get_columns_info(rows, domains, enums, schema)  # type: ignore[attr-defined]

        return columns.items()

    # fix for https://github.com/leonardovida/duckdb-sqlalchemy/issues/1128
    # (Overrides sqlalchemy method)
    @lru_cache()
    def _comment_query(  # type: ignore[no-untyped-def]
        self, schema: str, has_filter_names: bool, scope: Any, kind: Any
    ):
        if SQLALCHEMY_VERSION >= Version("2.0.36"):
            from sqlalchemy.dialects.postgresql import (  # type: ignore[attr-defined]
                pg_catalog,
            )

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
