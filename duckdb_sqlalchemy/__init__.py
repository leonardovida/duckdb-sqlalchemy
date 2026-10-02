import decimal
import json
import os
import re
import sys
import time
import uuid
import warnings
import weakref
from collections import defaultdict, deque
from functools import lru_cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from threading import Lock
from types import SimpleNamespace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    cast,
)

import duckdb
import sqlalchemy
from packaging.version import Version
from sqlalchemy import event, pool, text, util
from sqlalchemy import schema as sa_schema
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.dialects.postgresql.base import (
    PGCompiler,
    PGDDLCompiler,
    PGDialect,
    PGIdentifierPreparer,
    PGInspector,
    PGTypeCompiler,
)
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import processors
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.interfaces import ExecuteStyle
from sqlalchemy.engine.reflection import (
    ObjectKind,
    ObjectScope,
    ReflectionDefaults,
    cache,
)
from sqlalchemy.engine.url import URL as SAURL
from sqlalchemy.exc import CompileError, InvalidRequestError, NoSuchTableError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import bindparam
from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.selectable import Select

from ._arrow import DuckDBArrowResult
from ._bulk_insert import build_bulk_insert_data as _build_bulk_insert_data
from ._bulk_insert import has_bulk_insert_data_library as _has_bulk_insert_data_library
from ._bulk_insert import (
    infer_bulk_insert_column_keys as _infer_bulk_insert_column_keys,
)
from ._checkpoint import checkpoint
from ._pool import (
    _default_pool_class_for_database,
    _looks_like_motherduck,
    _pool_class_from_override,
    _pool_override_from_url,
)
from ._statements import (
    DISCONNECT_ERROR_PATTERNS,
    _is_aborted_transaction_error,
    _is_idempotent_statement,
    _is_transient_error,
    _strip_leading_sql_comments,
    _top_level_sql_words,
)
from ._supports import has_comment_support
from ._validation import validate_extension_name
from .bulk import (
    copy_from_csv,
    copy_from_parquet,
    copy_from_rows,
    copy_to_parquet,
    insert_from_arrow,
)
from .capabilities import get_capabilities
from .config import apply_config, get_core_config
from .datatypes import ISCHEMA_NAMES, FixedArray, Map, Struct, register_extension_types
from .datatypes import Union as UnionType
from .motherduck import (
    DIALECT_QUERY_KEYS,
    MotherDuckURL,
    _normalize_config_aliases,
    append_query_to_database,
    create_engine_from_paths,
    create_motherduck_engine,
    has_explicit_motherduck_credential,
    move_path_query_to_database,
    split_database_and_url_config,
    stable_session_hint,
    stable_session_name,
    validate_motherduck_database_name,
)
from .olap import (
    md_access_tokens,
    md_cancel_flight_run,
    md_cancel_job_run,
    md_create_dive,
    md_create_flight,
    md_create_guide,
    md_create_job,
    md_delete_dive,
    md_delete_flight,
    md_delete_guide,
    md_delete_job,
    md_flight_logs,
    md_flight_runs,
    md_flight_versions,
    md_flights,
    md_get_dive,
    md_get_dive_version,
    md_get_flight,
    md_get_flight_logs,
    md_get_flight_run,
    md_get_flight_version,
    md_get_guide,
    md_get_job,
    md_get_job_version,
    md_job_run_logs,
    md_job_runs,
    md_job_versions,
    md_jobs,
    md_list_dive_versions,
    md_list_dives,
    md_list_flight_runs,
    md_list_flight_versions,
    md_list_flights,
    md_list_guide_versions,
    md_list_guides,
    md_run_flight,
    md_run_job,
    md_set_guide_access,
    md_update_dive_content,
    md_update_dive_metadata,
    md_update_dive_status,
    md_update_flight,
    md_update_guide,
    md_update_guide_metadata,
    md_update_job,
    md_user_info,
    pragma_storage_info,
    prompt_jev,
    quack_query,
    read_csv,
    read_csv_auto,
    read_parquet,
    table_function,
)
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
    __version__ = "1.5.5.12"
sqlalchemy_version = sqlalchemy.__version__
SQLALCHEMY_VERSION = Version(sqlalchemy_version)
SQLALCHEMY_2 = SQLALCHEMY_VERSION >= Version("2.0.0")
_EMPTY_NAMED_TYPE_LOADER = SimpleNamespace(enums={}, domains={})
duckdb_version: str = duckdb.__version__
_capabilities = get_capabilities(duckdb_version)
supports_attach: bool = _capabilities.supports_attach
supports_user_agent: bool = _capabilities.supports_user_agent

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.engine.interfaces import (  # noqa: F401
        IsolationLevel,
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
    "append_query_to_database",
    "create_engine_from_paths",
    "create_motherduck_engine",
    "make_url",
    "stable_session_hint",
    "stable_session_name",
    "table_function",
    "validate_motherduck_database_name",
    "read_parquet",
    "read_csv",
    "read_csv_auto",
    "pragma_storage_info",
    "quack_query",
    "md_user_info",
    "md_list_dives",
    "md_access_tokens",
    "md_list_guides",
    "md_get_guide",
    "md_create_guide",
    "md_update_guide",
    "md_update_guide_metadata",
    "md_set_guide_access",
    "md_list_guide_versions",
    "md_delete_guide",
    "prompt_jev",
    "md_create_flight",
    "md_list_flights",
    "md_flights",
    "md_get_flight",
    "md_update_flight",
    "md_delete_flight",
    "md_run_flight",
    "md_cancel_flight_run",
    "md_get_flight_run",
    "md_list_flight_runs",
    "md_flight_runs",
    "md_get_flight_logs",
    "md_flight_logs",
    "md_list_flight_versions",
    "md_flight_versions",
    "md_get_flight_version",
    "md_create_job",
    "md_jobs",
    "md_get_job",
    "md_update_job",
    "md_delete_job",
    "md_run_job",
    "md_cancel_job_run",
    "md_job_runs",
    "md_job_run_logs",
    "md_job_versions",
    "md_get_job_version",
    "md_create_dive",
    "md_update_dive_metadata",
    "md_update_dive_status",
    "md_update_dive_content",
    "md_get_dive",
    "md_list_dive_versions",
    "md_get_dive_version",
    "md_delete_dive",
    "copy_from_parquet",
    "copy_from_csv",
    "copy_from_rows",
    "insert_from_arrow",
    "copy_to_parquet",
    "checkpoint",
]


_DUCKDB_CONNECTION_EXCEPTIONS: Tuple[type, ...] = tuple(
    cls
    for cls in (getattr(duckdb, "ConnectionException", None),)
    if isinstance(cls, type)
)
_DUCKDB_IO_EXCEPTIONS: Tuple[type, ...] = tuple(
    cls
    for cls in (
        getattr(duckdb, "IOException", None),
        getattr(duckdb, "HTTPException", None),
    )
    if isinstance(cls, type)
)


class DBAPI:
    paramstyle = "numeric_dollar"
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
    def get_enums(self, schema: Optional[str] = None) -> Any:
        """Return the user-defined ENUM types of ``schema`` (``"*"`` for all).

        Without a schema, the current database and schema are listed.
        """
        with self._operation_context() as conn:
            dialect = cast("Dialect", self.dialect)
            return dialect.get_enums(conn, schema, info_cache=self.info_cache)

    def get_check_constraints(
        self, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> Any:
        try:
            return super().get_check_constraints(table_name, schema, **kw)
        except NoSuchTableError:
            raise
        except Exception as e:
            raise NotImplementedError() from e


# DuckDBPyConnection calls that replace or discard the connection's open result.
_RESULT_DISCARDING_CONNECTION_METHODS = frozenset(
    {
        "append",
        "checkpoint",
        "execute",
        "executemany",
        "install_extension",
        "load_extension",
        "query",
        "register",
        "sql",
        "unregister",
    }
)
# Cursor calls that read the open result in a form other than Python rows.
_NATIVE_RESULT_FETCH_METHODS = frozenset(
    {
        "arrow",
        "df",
        "fetch_arrow_reader",
        "fetch_arrow_table",
        "fetch_df",
        "fetch_df_chunk",
        "fetch_record_batch",
        "fetchdf",
        "fetchnumpy",
        "pl",
        "tf",
        "to_arrow_table",
        "to_arrow_reader",
        "torch",
    }
)


# Tables and views with their comments, as one relation for reflection queries.
_DUCKDB_RELATION_COMMENTS = """(
    SELECT database_name, schema_name, table_name, comment, internal, temporary,
        'table' AS relation_kind
    FROM duckdb_tables()
    UNION ALL BY NAME
    SELECT database_name, schema_name, view_name AS table_name, comment, internal, temporary,
        'view' AS relation_kind
    FROM duckdb_views()
)"""


def _search_path_relation(
    entry: str, current_database: str, preparer: IdentifierPreparer
) -> Tuple[str, str]:
    parts = preparer.unformat_identifiers(entry.strip())
    if len(parts) == 2:
        return parts[0], parts[1]
    return current_database, parts[0]


def _strip_enclosing_parentheses(expression: str) -> str:
    """Drop parentheses that wrap all of ``expression``: ``(a > 0)`` -> ``a > 0``."""
    expression = expression.strip()
    while expression.startswith("(") and expression.endswith(")"):
        depth = 0
        quote: Optional[str] = None
        for index, char in enumerate(expression):
            if quote is not None:
                if char == quote:
                    quote = None
            elif char in "'\"":
                quote = char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and index != len(expression) - 1:
                    return expression
        expression = expression[1:-1].strip()
    return expression


class ConnectionWrapper:
    __c: duckdb.DuckDBPyConnection
    notices: List[str]
    # True (isolation_level="AUTOCOMMIT") makes begin() a no-op, so DuckDB
    # commits every statement on its own.
    autocommit = False
    closed = False

    def __init__(self, c: duckdb.DuckDBPyConnection) -> None:
        self.__c = c
        self.notices = list()
        self._pending_cursor: Optional["weakref.ReferenceType[CursorWrapper]"] = None
        # Statements run since begin(); None outside a transaction begun here.
        self._transaction_statements: Optional[int] = None

    def cursor(self) -> "CursorWrapper":
        return CursorWrapper(self.__c, self)

    def _set_pending_cursor(self, cursor: Optional["CursorWrapper"]) -> None:
        self._pending_cursor = weakref.ref(cursor) if cursor is not None else None

    def _buffer_pending_result(
        self, requester: Optional["CursorWrapper"] = None
    ) -> None:
        """Preserve the unread rows of another cursor before they are discarded.

        Every cursor shares one DuckDBPyConnection, which holds a single open
        result, so running a statement while iterating another result (an ORM
        lazy load inside a loop, for example) would silently truncate it. The
        rows the earlier cursor has not fetched yet are moved into Python memory
        instead. That costs memory proportional to the unread part of the
        result, including results fetched incrementally with ``yield_per``, but
        only when another statement actually runs before they are consumed.
        Results nothing references any more are skipped (weak reference).
        """
        pending = self._pending_cursor() if self._pending_cursor is not None else None
        if pending is not None and pending is not requester:
            pending._buffer_result()
        self._pending_cursor = None

    def begin(self) -> None:
        if not self.autocommit:
            self._begin()

    def _begin(self) -> None:
        self._buffer_pending_result()
        self.__c.begin()
        self._transaction_statements = 0

    def commit(self) -> None:
        self._buffer_pending_result()
        self._transaction_statements = None
        self.__c.commit()

    def rollback(self) -> None:
        # Rolling back ends the unit of work, and it runs on every pool
        # checkin; buffering here would download the rest of any partly read
        # result. Forget the pending cursor instead.
        pending = self._pending_cursor() if self._pending_cursor is not None else None
        try:
            if pending is not None:
                pending.close()
        finally:
            self._pending_cursor = None
            self._transaction_statements = None
            self.__c.rollback()

    def _count_statement(self) -> None:
        if self._transaction_statements is not None:
            self._transaction_statements += 1

    def __getattr__(self, name: str) -> Any:
        if name in _RESULT_DISCARDING_CONNECTION_METHODS:
            self._buffer_pending_result()
            method = getattr(self.__c, name)

            def call_native(*args: Any, **kwargs: Any) -> Any:
                self._buffer_pending_result()
                self._count_statement()
                return method(*args, **kwargs)

            return call_native
        return getattr(self.__c, name)

    def close(self) -> None:
        pending = self._pending_cursor() if self._pending_cursor is not None else None
        try:
            if pending is not None:
                pending.close()
        finally:
            self._pending_cursor = None
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
_DML_ROWCOUNT_PREFIXES = ("insert", "update", "delete", "merge")


def _first_present_register_value(
    parameters: Mapping[str, Any], keys: Sequence[str]
) -> Optional[Any]:
    for key in keys:
        if key in parameters:
            return parameters[key]
    return None


def _parse_register_params(parameters: Optional[Any]) -> Tuple[str, Any]:
    if parameters is None:
        raise ValueError("register requires a view name and data")
    if isinstance(parameters, dict):
        view_name = _first_present_register_value(parameters, _REGISTER_NAME_KEYS)
        df = _first_present_register_value(parameters, _REGISTER_DATA_KEYS)
        if view_name is None or df is None:
            raise ValueError("register requires a view name and data (tuple or dict)")
        return view_name, df
    if isinstance(parameters, (list, tuple)) and len(parameters) == 2:
        return parameters[0], parameters[1]
    raise ValueError("register requires a view name and data (tuple or dict)")


class CursorWrapper:
    __c: duckdb.DuckDBPyConnection
    __connection_wrapper: "ConnectionWrapper"
    # rows fetchmany() returns without a size (DBAPI default: 1); set from the
    # duckdb_arraysize/arraysize execution options
    arraysize = 1

    def __init__(
        self, c: duckdb.DuckDBPyConnection, connection_wrapper: "ConnectionWrapper"
    ) -> None:
        self.__c = c
        self.__connection_wrapper = connection_wrapper
        # Rows moved out of the shared connection by _buffer_result.
        self._buffered_rows: Optional[deque] = None
        self._buffered_description: Any = None
        self._buffered_rowcount: int = -1
        # Rows changed by the last INSERT/UPDATE/DELETE/MERGE, which DuckDB
        # reports as a "Count" result while its own rowcount stays -1.
        self._dml_rowcount: Optional[int] = None
        self._is_dml_count = False
        # True once rows of the current result were read as Python tuples;
        # DuckDB then cannot return the rest of that result as Arrow.
        self._rows_fetched = False
        self._native_reader_active = False
        self._native_reader: Any = None

    def _clear_result(self) -> None:
        self.__c.execute("")

    def _before_execute(self) -> None:
        if self._native_reader_active:
            raise InvalidRequestError(
                "Consume or close the Arrow batch reader before executing another statement"
            )
        self.__connection_wrapper._buffer_pending_result(self)
        self._buffered_rows = None
        self._buffered_description = None
        self._dml_rowcount = None
        self._is_dml_count = False
        self._rows_fetched = False

    def _capture_dml_rowcount(self, statement: str) -> bool:
        """Read the "Count" result of a DML statement into ``rowcount``.

        The count row stays readable, buffered like any other result. A
        RETURNING clause yields its own columns instead, so it is left alone.
        """
        description = getattr(self, "description", None)
        if not description or len(description) != 1 or description[0][0] != "Count":
            return False
        normalized = _strip_leading_sql_comments(statement)
        match = re.match(
            r"\s*(insert|update|delete|merge)\b", normalized, re.IGNORECASE
        )
        # Common generated DML has neither RETURNING nor separators. Avoid
        # scanning every placeholder in a large multi-values INSERT.
        if (
            match is not None
            and ";" not in normalized
            and "returning" not in normalized.lower()
            and statement.count("/*") <= 1
        ):
            operation = match.group(1).lower()
        else:
            words = _top_level_sql_words(statement)
            if not words:
                return False
            operation = words[0]
            if operation == "with":
                operation = next(
                    (
                        word
                        for word in words[1:]
                        if word in (*_DML_ROWCOUNT_PREFIXES, "select")
                    ),
                    "",
                )
            if "returning" in words:
                return False
        if operation not in _DML_ROWCOUNT_PREFIXES:
            return False
        rows = self.__c.fetchall()
        if len(rows) == 1 and isinstance(rows[0][0], int):
            self._dml_rowcount = rows[0][0]
        self._store_buffered_result(rows, description, -1)
        self._is_dml_count = True
        return True

    def _after_execute(self) -> None:
        if getattr(self.__c, "description", None) is not None:
            self.__connection_wrapper._set_pending_cursor(self)

    def _buffer_result(self) -> None:
        """Fetch this cursor's unread rows before another statement replaces them."""
        if self._native_reader_active:
            raise InvalidRequestError(
                "Consume or close the Arrow batch reader before executing another statement"
            )
        description = self.description
        rowcount = self.__c.rowcount
        rows = self.__c.fetchall()
        self._store_buffered_result(rows, description, rowcount)

    def _store_buffered_result(
        self, rows: Iterable[Tuple[Any, ...]], description: Any, rowcount: int
    ) -> None:
        """Keep buffered rows together with their original result metadata."""
        self._buffered_rows = deque(rows)
        self._buffered_description = description
        self._buffered_rowcount = rowcount

    def _result_consumed(self) -> None:
        pending = self.__connection_wrapper._pending_cursor
        if pending is not None and pending() is self:
            self.__connection_wrapper._set_pending_cursor(None)

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
        self._before_execute()
        self.__connection_wrapper._count_statement()
        self.__c.executemany(statement, params)
        if self._capture_dml_rowcount(statement):
            # DuckDB exposes only the final count for executemany, not a total.
            self._dml_rowcount = None
        else:
            self._after_execute()

    def execute(
        self,
        statement: str,
        parameters: Optional[Tuple] = None,
        context: Optional[Any] = None,
    ) -> None:
        self._before_execute()
        norm = statement.strip().lower().rstrip(";")
        if norm == "commit":  # this is largely for ipython-sql
            self.__connection_wrapper.commit()
            return
        elif _BEGIN_TRANSACTION_ISOLATION_RE.fullmatch(norm):
            self.__connection_wrapper._begin()
            return
        self.__connection_wrapper._count_statement()
        if _SET_TRANSACTION_ISOLATION_RE.fullmatch(norm):
            self._clear_result()
        elif _is_ignored_postgres_config_set(norm):
            self._clear_result()
        elif norm.startswith("register"):
            view_name, df = _parse_register_params(parameters)
            self.__c.register(view_name, df)
            return
        elif norm == "show transaction isolation level":
            self.__c.execute("select 'read committed' as transaction_isolation")
        elif norm == "show standard_conforming_strings":
            self.__c.execute("select 'on' as standard_conforming_strings")
        elif parameters is None:
            self.__c.execute(statement)
        else:
            self.__c.execute(statement, parameters)
        if self._capture_dml_rowcount(norm):
            return
        self._after_execute()

    @property
    def connection(self) -> Any:
        return self.__connection_wrapper

    def close(self) -> None:
        if self._native_reader is not None:
            self._native_reader.close()
        # Preserve an exhausted result so an old cursor cannot read a new query.
        self._store_buffered_result([], self.description, self.rowcount)
        self._result_consumed()

    @property
    def description(self) -> Any:
        if self._buffered_rows is not None:
            return self._buffered_description
        desc = self.__c.description
        if desc is None:
            return None
        fixed = []
        for col in desc:
            if len(col) >= 2:
                type_code = col[1]
                try:
                    hash(type_code)
                except TypeError:
                    col = (col[0], str(type_code), *col[2:])
            fixed.append(col)
        return fixed

    @property
    def rowcount(self) -> int:
        if self._dml_rowcount is not None:
            return self._dml_rowcount
        if self._buffered_rows is not None:
            return self._buffered_rowcount
        return self.__c.rowcount

    def __getattr__(self, name: str) -> Any:
        if self._buffered_rows is not None and name in _NATIVE_RESULT_FETCH_METHODS:
            raise NotImplementedError(
                f"{name}() is unavailable: another statement ran on this connection "
                "before the result was consumed, so its remaining rows were "
                "buffered as Python tuples"
            )
        method = getattr(self.__c, name)
        if name not in _NATIVE_RESULT_FETCH_METHODS:
            return method

        def fetch_native(*args: Any, **kwargs: Any) -> Any:
            if self._buffered_rows is not None:
                raise NotImplementedError(
                    "Native results are unavailable after buffering"
                )
            if self._native_reader_active:
                raise InvalidRequestError(
                    "An Arrow batch reader already owns this result"
                )
            value = method(*args, **kwargs)
            self._rows_fetched = True
            if hasattr(value, "read_next_batch"):
                return value
            if name != "fetch_df_chunk":
                self._store_buffered_result([], self.description, self.rowcount)
                self._result_consumed()
            return value

        return fetch_native

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        if self._native_reader_active:
            raise InvalidRequestError("An Arrow batch reader already owns this result")
        if self._buffered_rows is not None:
            return self._buffered_rows.popleft() if self._buffered_rows else None
        self._rows_fetched = True
        row = self.__c.fetchone()
        if row is None:
            self._store_buffered_result([], self.description, self.__c.rowcount)
            self._result_consumed()
        return row

    def fetchall(self) -> List:
        if self._native_reader_active:
            raise InvalidRequestError("An Arrow batch reader already owns this result")
        if self._buffered_rows is not None:
            rows = list(self._buffered_rows)
            self._buffered_rows.clear()
            return rows
        self._rows_fetched = True
        rows = self.__c.fetchall()
        self._store_buffered_result([], self.description, self.__c.rowcount)
        self._result_consumed()
        return rows

    def fetchmany(self, size: Optional[int] = None) -> List:
        if self._native_reader_active:
            raise InvalidRequestError("An Arrow batch reader already owns this result")
        count = self.arraysize if size is None else size
        if self._buffered_rows is not None:
            buffered = self._buffered_rows
            return [buffered.popleft() for _ in range(min(count, len(buffered)))]
        self._rows_fetched = True
        rows = self.__c.fetchmany(count)
        if not rows and count != 0:
            self._store_buffered_result([], self.description, self.__c.rowcount)
            self._result_consumed()
        return rows


def _is_ignored_postgres_config_set(statement: str) -> bool:
    match = _SET_POSTGRES_CONFIG_RE.match(statement)
    if match is None:
        return False
    return match.group(1) in _IGNORED_POSTGRES_CONFIG_SETTINGS


class DuckDBEngineWarning(Warning):
    pass


_RESERVED_WORDS_LOCK = Lock()


@lru_cache()
def _load_reserved_words() -> frozenset[str]:
    return frozenset(
        keyword_name
        for (keyword_name,) in duckdb.cursor()
        .execute(
            "select keyword_name from duckdb_keywords() where keyword_category == 'reserved'"
        )
        .fetchall()
    )


def _get_reserved_words() -> frozenset[str]:
    with _RESERVED_WORDS_LOCK:
        return _load_reserved_words()


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
        **kwargs: Any,
    ) -> Any:
        execution_options = _normalize_execution_options(execution_options)
        context = super()._init_compiled(
            dialect,
            connection,
            dbapi_connection,
            execution_options,
            compiled,
            parameters,
            invoked_statement,
            extracted_parameters,
            cache_hit,
            **kwargs,
        )
        if (
            context.execute_style is ExecuteStyle.INSERTMANYVALUES
            and dialect._routes_insertmanyvalues_to_register(context)
        ):
            context.execute_style = ExecuteStyle.EXECUTEMANY
        return context

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
        if (
            cursor is not None
            and getattr(cursor, "_is_dml_count", False)
            and (self.isinsert or self.isupdate or self.isdelete)
        ):
            # Count is DuckDB's DML acknowledgement, not a RETURNING result.
            # Preserve rowcount while following SQLAlchemy's non-row contract.
            cursor._buffered_description = None
            cursor._buffered_rows.clear()
            cursor._result_consumed()
        if (
            cursor is not None
            and not getattr(cursor, "_is_dml_count", False)
            and (self.isinsert or self.isupdate or self.isdelete)
            and getattr(self.compiled, "returning", None)
        ):
            # Native RETURNING rowcount is -1. Materialize its write result
            # so ORM version checks can verify affected rows on SQLAlchemy 2.0.
            returning_rows = getattr(self, "_insertmanyvalues_rows", None)
            if returning_rows is not None:
                self._rowcount = len(returning_rows)
            else:
                if self.execution_options.get("duckdb_arrow"):
                    fetch_arrow = getattr(cursor, "to_arrow_table", None)
                    if fetch_arrow is None:
                        fetch_arrow = cursor.fetch_arrow_table
                    arrow_table = fetch_arrow()
                    self._duckdb_returning_arrow = arrow_table
                    cursor._store_buffered_result(
                        (),
                        cursor.description,
                        arrow_table.num_rows,
                    )
                    self._rowcount = arrow_table.num_rows
                else:
                    cursor._buffer_result()
                    self._rowcount = len(cursor._buffered_rows)
            cursor._dml_rowcount = self._rowcount
            cursor._result_consumed()
        result = super()._setup_result_proxy()
        if self.execution_options.get("duckdb_arrow") and getattr(
            result, "returns_rows", False
        ):
            arrow_result = DuckDBArrowResult(result)
            returning_arrow = getattr(self, "_duckdb_returning_arrow", None)
            if returning_arrow is not None:
                arrow_result._arrow = returning_arrow
                result.close()
            return arrow_result
        return result


def _apply_motherduck_defaults(config: Dict[str, Any], database: Optional[str]) -> None:
    if _looks_like_motherduck(database) and not has_explicit_motherduck_credential(
        database, config
    ):
        token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")
        if token:
            config["motherduck_token"] = token

    if "motherduck_token" in config and not isinstance(config["motherduck_token"], str):
        raise TypeError("motherduck_token must be a string")


def _normalize_motherduck_config(config: Dict[str, Any]) -> None:
    _normalize_config_aliases(config)


def _pop_application_name(config: Dict[str, Any]) -> Optional[str]:
    application_name = config.pop("application_name", None)
    if application_name is None:
        return None
    return str(application_name)


def _prepare_connection_params(
    cparams: Dict[str, Any], core_keys: Collection[str]
) -> Tuple[Dict[str, Any], Optional[str]]:
    config = dict(cparams.get("config", {}))
    cparams["config"] = config
    config.update(cparams.pop("url_config", {}))
    for key in DIALECT_QUERY_KEYS:
        config.pop(key, None)
    if cparams.get("database") in {None, ""}:
        cparams["database"] = ":memory:"
    _apply_motherduck_defaults(config, cparams.get("database"))
    cparams["database"] = move_path_query_to_database(cparams.get("database"), config)
    _normalize_motherduck_config(config)
    application_name = _pop_application_name(config)
    ext = {k: config.pop(k) for k in list(config) if k not in core_keys}
    return ext, application_name


class DuckDBIdentifierPreparer(PGIdentifierPreparer):
    def __init__(self, dialect: "Dialect", **kwargs: Any) -> None:
        super().__init__(dialect, **kwargs)
        self.reserved_words.update(_get_reserved_words())

    def _separate(self, name: Optional[str]) -> Tuple[Optional[Any], Optional[str]]:
        """
        Get database name and schema name from schema if it contains a database name
            Format:
              <db_name>.<schema_name>
              Components may be double quoted to preserve spaces, dots, or quotes.
        """
        if name is None:
            return None, None
        identifiers = self.unformat_identifiers(name)
        if len(identifiers) == 1:
            return None, identifiers[0]
        if len(identifiers) == 2:
            return identifiers[0], identifiers[1]
        raise ValueError(
            "DuckDB schema names must contain at most a database and schema"
        )

    def format_schema(self, name: str) -> str:
        """Prepare a quoted schema name."""
        database_name, schema_name = self._separate(name)
        if database_name is None or schema_name is None:
            if getattr(name, "quote", None) is not None or schema_name is None:
                # keeps explicit quoting, e.g. schema_translate_map placeholders
                return self.quote(name)
            # quote the unquoted form so '"my.schema"' is not quoted twice
            return self.quote(schema_name)
        return ".".join(self.quote(str(_n)) for _n in [database_name, schema_name])

    def quote_schema(self, schema: str, force: Any = None) -> str:
        """
        Conditionally quote a schema name.

        :param schema: string schema name
        :param force: unused
        """
        return self.format_schema(schema)


def _column_needs_implicit_sequence(column: Any) -> bool:
    """Columns that PostgreSQL would render as SERIAL/BIGSERIAL.

    DuckDB has no SERIAL type, so these columns are backed by an explicitly
    created sequence instead (see :class:`DuckDBDDLCompiler` and the
    ``before_create``/``after_drop`` hooks below).
    """
    table = getattr(column, "table", None)
    if table is None:
        return False
    if not column.primary_key or column is not table._autoincrement_column:
        return False
    if column.default is not None and not (
        isinstance(column.default, sa_schema.Sequence) and column.default.optional
    ):
        return False
    if column.server_default is not None and not isinstance(
        column.server_default, sa_schema.Identity
    ):
        return False
    identity = getattr(column, "identity", None)
    if identity is not None and (
        any(
            getattr(identity, option, None) is not None
            for option in ("start", "increment", "minvalue", "maxvalue", "cache")
        )
        or any(
            getattr(identity, option, None)
            for option in (
                "always",
                "nominvalue",
                "nomaxvalue",
                "cycle",
                "order",
                "on_null",
            )
        )
    ):
        raise CompileError(
            "DuckDB cannot honor Identity options; use an explicit Sequence"
        )
    return True


def _implicit_sequence_ddl_name(preparer: IdentifierPreparer, column: Any) -> str:
    name = preparer.quote(f"{column.table.name}_{column.name}_seq")
    # schema_for_object renders a schema_translate_map placeholder when the
    # preparer translates schemas, like the table name in the same DDL
    schema = preparer.schema_for_object(column.table)
    if schema:
        name = f"{preparer.quote_schema(schema)}.{name}"
    return name


def _execute_implicit_sequence_ddl(
    target: Any, connection: Any, statement_prefix: str
) -> None:
    if connection.dialect.name != "duckdb":
        return
    preparer = connection.dialect.identifier_preparer
    # a MockConnection (create_mock_engine, Alembic's --sql mode) has neither
    # execution options nor exec_driver_sql
    mock = not hasattr(connection, "exec_driver_sql")
    schema_translate_map = (
        None if mock else connection.get_execution_options().get("schema_translate_map")
    )
    if schema_translate_map:
        preparer = preparer._with_schema_translate(schema_translate_map)
    for column in target.columns:
        if _column_needs_implicit_sequence(column):
            statement = (
                f"{statement_prefix} {_implicit_sequence_ddl_name(preparer, column)}"
            )
            if schema_translate_map:
                statement = preparer._render_schema_translates(
                    statement, schema_translate_map
                )
            if mock:
                connection.execute(sa_schema.DDL(statement.replace("%", "%%")))
            else:
                connection.exec_driver_sql(statement)


def _create_implicit_sequences(target: Any, connection: Any, **kw: Any) -> None:
    temporary = any(
        str(prefix).upper() in {"TEMP", "TEMPORARY"} for prefix in target._prefixes
    )
    statement = (
        "CREATE TEMPORARY SEQUENCE IF NOT EXISTS"
        if temporary
        else "CREATE SEQUENCE IF NOT EXISTS"
    )
    _execute_implicit_sequence_ddl(target, connection, statement)


def _drop_implicit_sequences(target: Any, connection: Any, **kw: Any) -> None:
    if target.info.get("_duckdb_preserve_implicit_sequence"):
        return
    _execute_implicit_sequence_ddl(target, connection, "DROP SEQUENCE IF EXISTS")


event.listen(sa_schema.Table, "before_create", _create_implicit_sequences)
event.listen(sa_schema.Table, "after_drop", _drop_implicit_sequences)


class DuckDBDDLCompiler(PGDDLCompiler):
    def get_column_specification(self, column: Any, **kwargs: Any) -> str:
        if not _column_needs_implicit_sequence(column):
            # PostgreSQL renders SERIAL for reflected sequence-backed keys,
            # even when a server default is present. DuckDB has no SERIAL.
            from sqlalchemy.sql.compiler import DDLCompiler

            return DDLCompiler.get_column_specification(self, column, **kwargs)
        colspec = self.preparer.format_column(column)
        colspec += " " + self.dialect.type_compiler_instance.process(
            column.type, type_expression=column
        )
        seq_name = _implicit_sequence_ddl_name(self.preparer, column)
        colspec += " DEFAULT nextval(%s)" % self.sql_compiler.render_literal_value(
            seq_name, sqltypes.String()
        )
        if not column.nullable:
            colspec += " NOT NULL"
        return colspec


class DuckDBTypeCompiler(PGTypeCompiler):
    def visit_FLOAT(self, type_: Any, **kw: Any) -> str:
        # Generic Float(None) must preserve Python's 64-bit float precision.
        return (
            "FLOAT"
            if type_.precision is not None and type_.precision <= 24
            else "DOUBLE"
        )


def _json_pointer(value: Any) -> str:
    parts = value if isinstance(value, (tuple, list)) else (value,)
    if not parts:
        return "$"
    if any(isinstance(part, int) and part < 0 for part in parts):
        return "$" + "".join(
            (f"[#{part}]" if part < 0 else f"[{part}]")
            if isinstance(part, int)
            else "." + json.dumps(str(part), ensure_ascii=False)
            for part in parts
        )
    return "/" + "/".join(
        str(part).replace("~", "~0").replace("/", "~1") for part in parts
    )


class DuckDBJSONIndex(sqltypes.JSON.JSONIndexType):
    def bind_processor(self, dialect: Any) -> Any:
        return _json_pointer

    def literal_processor(self, dialect: Any) -> Any:
        render = sqltypes.String().literal_processor(dialect)
        return lambda value: render(_json_pointer(value))


class DuckDBJSONPath(sqltypes.JSON.JSONPathType):
    def bind_processor(self, dialect: Any) -> Any:
        return _json_pointer

    def literal_processor(self, dialect: Any) -> Any:
        render = sqltypes.String().literal_processor(dialect)
        return lambda value: render(_json_pointer(value))


class DuckDBCompiler(PGCompiler):
    def _json_extract(self, binary: Any, **kw: Any) -> str:
        as_json = isinstance(binary.type, sqltypes.JSON)
        function = "json_extract" if as_json else "json_extract_string"
        expression = f"{function}({self.process(binary.left, **kw)}, {self.process(binary.right, **kw)})"
        if as_json:
            return expression
        target_type = self.dialect.type_compiler_instance.process(binary.type)
        return f"CAST({expression} AS {target_type})"

    def visit_json_getitem_op_binary(
        self, binary: Any, operator: Any, _cast_applied: bool = False, **kw: Any
    ) -> str:
        return self._json_extract(binary, **kw)

    def visit_json_path_getitem_op_binary(
        self, binary: Any, operator: Any, _cast_applied: bool = False, **kw: Any
    ) -> str:
        return self._json_extract(binary, **kw)

    # DuckDB's ``~`` operator is a full-string match (regexp_full_match) and it
    # has no ``~*``; regexp_matches() searches like PostgreSQL's ``~`` and takes
    # the flags as options.
    def _regexp_matches(self, binary: Any, **kw: Any) -> str:
        args = [self.process(binary.left, **kw), self.process(binary.right, **kw)]
        flags = binary.modifiers["flags"]
        if flags is not None:
            # SQLAlchemy before 2.0.18 passes the flags as a bound parameter
            value = flags.value if isinstance(flags, BindParameter) else flags
            args.append(self.render_literal_value(value, sqltypes.STRINGTYPE))
        return "regexp_matches(%s)" % ", ".join(args)

    def visit_regexp_match_op_binary(
        self, binary: Any, operator: Any, **kw: Any
    ) -> str:
        return self._regexp_matches(binary, **kw)

    def visit_not_regexp_match_op_binary(
        self, binary: Any, operator: Any, **kw: Any
    ) -> str:
        return "NOT %s" % self._regexp_matches(binary, **kw)


def _process_decimal_bind(value: Any) -> Any:
    # DuckDB's binder drops a positive exponent (``Decimal("1E+5")`` binds as
    # ``1.00000``), so rewrite such values without an exponent first.
    if isinstance(value, decimal.Decimal) and value.is_finite():
        exponent = value.as_tuple().exponent
        if isinstance(exponent, int) and exponent > 0:
            return decimal.Decimal(format(value, "f"))
    return value


class DuckDBNumeric(sqltypes.Numeric):
    """Numeric for a DBAPI that binds and returns ``Decimal`` natively.

    DECIMAL columns come back as exact ``Decimal`` values, but expressions
    SQLAlchemy types as Numeric (``avg()`` over a DECIMAL column, for example)
    may return DOUBLE or integer values, so only those are converted.
    """

    def bind_processor(self, dialect: Any) -> Any:
        return _process_decimal_bind

    def result_processor(self, dialect: Any, coltype: object) -> Any:
        if not self.asdecimal:
            return processors.to_float
        float_to_decimal = processors.to_decimal_processor_factory(
            decimal.Decimal,
            self._effective_decimal_return_scale,
        )

        def process(value: Any) -> Any:
            if isinstance(value, float):
                return float_to_decimal(value)
            if isinstance(value, int) and not isinstance(value, bool):
                return decimal.Decimal(value)
            return value

        return process


class DuckDBFloat(sqltypes.Float):
    def bind_processor(self, dialect: Any) -> Any:
        return _process_decimal_bind


class DuckDBNullType(sqltypes.NullType):
    def result_processor(self, dialect: Any, coltype: object) -> Any:
        if coltype == "JSON":
            return sqltypes.JSON().result_processor(dialect, coltype)
        else:
            return super().result_processor(dialect, coltype)


class Dialect(PGDialect_psycopg2):
    name = "duckdb"
    driver = "duckdb_sqlalchemy"
    type_compiler_cls = DuckDBTypeCompiler
    supports_identity_columns = False
    _has_events = False
    supports_statement_cache = True
    # COMMENT ON exists in every supported DuckDB; initialize() re-checks, and
    # the class default covers DDL compiled without connecting (--sql mode)
    supports_comments = True
    supports_constraint_comments = False
    # single-statement DML rowcount comes from DuckDB's "Count" result
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_sane_rowcount_returning = True
    supports_server_side_cursors = False
    # duckdb binds and returns decimal.Decimal without going through float
    supports_native_decimal = True
    execution_ctx_cls = DuckDBExecutionContext
    div_is_floordiv = False  # TODO: tweak this to be based on DuckDB version
    inspector = DuckDBInspector
    insertmanyvalues_page_size = 1000
    use_insertmanyvalues = True
    use_insertmanyvalues_wo_returning = True
    duckdb_copy_threshold = 10000
    _capabilities: "DuckDBCapabilities"
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            # the psycopg2 driver registers a _PGNumeric with custom logic for
            # postgres type_codes (such as 701 for float) that duckdb doesn't have
            sqltypes.Numeric: DuckDBNumeric,
            sqltypes.Float: DuckDBFloat,
            sqltypes.JSON: sqltypes.JSON,
            sqltypes.JSON.JSONIndexType: DuckDBJSONIndex,
            sqltypes.JSON.JSONIntIndexType: DuckDBJSONIndex,
            sqltypes.JSON.JSONStrIndexType: DuckDBJSONIndex,
            sqltypes.JSON.JSONPathType: DuckDBJSONPath,
            UUID: UUID,
        },
    )
    ischema_names = util.update_copy(
        PGDialect.ischema_names,
        ISCHEMA_NAMES,
    )
    preparer = DuckDBIdentifierPreparer
    identifier_preparer: DuckDBIdentifierPreparer
    statement_compiler = DuckDBCompiler
    ddl_compiler = DuckDBDDLCompiler

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["use_native_hstore"] = False
        super().__init__(*args, **kwargs)
        self._backslash_escapes = False
        self._capabilities = get_capabilities(duckdb.__version__)

    @classmethod
    def load_provisioning(cls) -> None:
        # The dialect lives in a package __init__, unlike built-in dialects.
        from . import provision  # noqa: F401

    def initialize(self, connection: "Connection") -> None:
        _register_alembic_support()
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
        ext, application_name = _prepare_connection_params(cparams, core_keys)
        config = cparams["config"]
        if supports_user_agent:
            user_agent = (
                f"duckdb-sqlalchemy/{__version__}(sqlalchemy/{sqlalchemy_version})"
            )
            if application_name:
                user_agent = f"{user_agent} application_name({application_name})"
            if "custom_user_agent" in config:
                user_agent = f"{user_agent} {config['custom_user_agent']}"
            config["custom_user_agent"] = user_agent

        filesystems = cparams.pop("register_filesystems", [])

        conn = duckdb.connect(*cargs, **cparams)

        try:
            for extension in preload_extensions:
                conn.execute(f"LOAD {validate_extension_name(extension)}")

            for filesystem in filesystems:
                conn.register_filesystem(filesystem)

            apply_config(self, conn, ext)
        except BaseException:
            # Do not leak the connection (and its file lock) on setup failure.
            try:
                conn.close()
            except Exception:
                pass
            raise

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

    # DuckDB has a single transaction mode (snapshot isolation). It is reported
    # as READ COMMITTED, like the "show transaction isolation level" emulation,
    # so PostgreSQL-oriented tools keep working. AUTOCOMMIT skips BEGIN so that
    # every statement commits on its own.
    _transaction_isolation_level: "IsolationLevel" = "READ COMMITTED"

    def get_isolation_level_values(
        self, dbapi_connection: Any
    ) -> Sequence["IsolationLevel"]:
        return ["AUTOCOMMIT", self._transaction_isolation_level]

    def get_default_isolation_level(self, dbapi_conn: Any) -> "IsolationLevel":
        # a constant, so connecting does not probe the isolation level
        return self._transaction_isolation_level

    def get_isolation_level(self, dbapi_connection: Any) -> "IsolationLevel":
        if getattr(dbapi_connection, "autocommit", False):
            return "AUTOCOMMIT"
        return self._transaction_isolation_level

    def set_isolation_level(
        self, dbapi_connection: Any, level: "IsolationLevel"
    ) -> None:
        dbapi_connection.autocommit = level == "AUTOCOMMIT"

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
            FROM (
                SELECT database_name, schema_name, view_name AS table_name, internal
                FROM duckdb_views()
            )
            WHERE internal = false
            """
        sql, params = self._build_query_where(
            schema_name=schema, default_to_current_schema=True
        )
        s += sql
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

        quote = self.identifier_preparer.quote
        return [".".join(quote(identifier) for identifier in nspname) for nspname in rs]

    @cache  # type: ignore[call-arg]
    def has_sequence(
        self,
        connection: "Connection",
        sequence_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> bool:
        where, params = self._build_query_where(
            schema_name=schema, default_to_current_schema=True
        )
        params["sequence_name"] = sequence_name
        return (
            connection.execute(
                text(
                    "SELECT 1 FROM duckdb_sequences() WHERE sequence_name = :sequence_name "
                    + where
                    + " LIMIT 1"
                ),
                params,
            ).first()
            is not None
        )

    @cache  # type: ignore[call-arg]
    def get_sequence_names(
        self, connection: "Connection", schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        where, params = self._build_query_where(
            schema_name=schema, default_to_current_schema=True
        )
        return list(
            connection.execute(
                text(
                    "SELECT sequence_name FROM duckdb_sequences() WHERE 1 = 1 "
                    + where
                    + " ORDER BY sequence_name"
                ),
                params,
            ).scalars()
        )

    def _build_query_where(
        self,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None,
        default_to_current_schema: bool = False,
    ) -> Tuple[str, Dict[str, str]]:
        """Build the catalog filter for a reflection query.

        ``schema_name`` may be ``<db name>.<schema name>``. A bare schema name
        resolves in the current database, falling back to the first attached
        database that has it, because the same schema name (``main``) usually
        exists in several catalogs. Without a schema, ``default_to_current_schema``
        limits the query to the current database and schema; otherwise the
        caller ranks rows by DuckDB's name resolution order.
        """
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
            if database_name is None:
                sql += (
                    "AND database_name = (\n"
                    "    SELECT database_name FROM duckdb_schemas()\n"
                    "    WHERE schema_name = :schema_name\n"
                    "    ORDER BY database_name <> current_database(), database_name\n"
                    "    LIMIT 1\n"
                    ")\n"
                )
        elif default_to_current_schema:
            sql += (
                "AND database_name = current_database()\n"
                "AND schema_name = current_schema()\n"
            )

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
        sql, params = self._build_query_where(
            schema_name=schema, default_to_current_schema=True
        )
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
            SELECT oid, table_name, database_name, schema_name
            FROM (
                SELECT table_oid AS oid, table_name,              database_name, schema_name FROM duckdb_tables()
                UNION ALL BY NAME
                SELECT view_oid AS oid , view_name AS table_name, database_name, schema_name FROM duckdb_views()
            )
            WHERE schema_name NOT LIKE 'pg\\_%' ESCAPE '\\'
            """
        sql, params = self._build_query_where(table_name=table_name, schema_name=schema)
        s += sql

        visible_rows = self._execute_visible_duckdb_relation_rows(
            connection, text(s), params, schema
        )
        table_oid = visible_rows[0]["oid"] if visible_rows else None
        if table_oid is None:
            raise NoSuchTableError(table_name)
        return table_oid

    def _duckdb_relation_exists(
        self, connection: "Connection", table_name: str, schema: Optional[str]
    ) -> bool:
        sql = """
            SELECT database_name, schema_name, table_name
            FROM (
                SELECT database_name, schema_name, table_name, internal
                FROM duckdb_tables()
                UNION ALL BY NAME
                SELECT database_name, schema_name, view_name AS table_name, internal
                FROM duckdb_views()
            )
            WHERE internal = false
            """
        where_sql, params = self._build_query_where(
            table_name=table_name, schema_name=schema
        )
        rows = self._execute_visible_duckdb_relation_rows(
            connection, text(sql + where_sql), params, schema
        )
        return bool(rows)

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
            FROM {relation}
            WHERE schema_name NOT LIKE 'pg\\_%' ESCAPE '\\'
            """
        params: Dict[str, Any] = {}
        if include_internal_filter:
            sql += "AND internal = false\n"
        # Listing without a schema covers the current schema only; looking up
        # named relations without a schema follows DuckDB's name resolution,
        # which the caller applies with _visible_duckdb_relation_rows.
        where_sql, where_params = self._build_query_where(
            schema_name=schema, default_to_current_schema=filter_names is None
        )
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

    def _visible_duckdb_relation_rows(
        self,
        connection: "Connection",
        rows: Sequence[Dict[str, Any]],
        schema: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Keep, per table name, the relation an unqualified name resolves to.

        With an explicit schema the query already targets a single catalog and
        schema. Without one, only temp objects, the current schema and the
        search path of the current database are visible, matching how DuckDB
        binds an unqualified name; relations in other attached databases are
        not, so SQLAlchemy does not mistake them for the default schema.
        """
        if not rows or schema is not None:
            return list(rows)

        relations_by_table: Dict[str, set[Tuple[str, str]]] = defaultdict(set)
        for row in rows:
            relations_by_table[row["table_name"]].add(
                (row["database_name"], row["schema_name"])
            )
        current_database, current_schema, search_path, search_path_setting = (
            connection.execute(
                text(
                    "SELECT current_database(), current_schema(), "
                    "current_schemas(false), current_setting('search_path')"
                )
            ).one()
        )
        # search_path entries may name another database ("db2.main");
        # unqualified entries refer to the current database.
        search_entries = [
            _search_path_relation(entry, current_database, self.identifier_preparer)
            for entry in self._split_duckdb_list(str(search_path_setting or ""))
            if entry.strip()
        ]
        search_entries.extend(
            (current_database, schema_name) for schema_name in search_path
        )
        search_order: Dict[Tuple[str, str], int] = {}
        for entry in search_entries:
            search_order.setdefault(entry, len(search_order) + 2)

        visible_relations: Dict[str, Tuple[str, str]] = {}
        for table_name, relations in relations_by_table.items():
            ranked: List[Tuple[int, Tuple[str, str]]] = []
            for relation in relations:
                database_name, schema_name = relation
                if database_name == "temp":
                    ranked.append((0, relation))
                elif (
                    database_name == current_database and schema_name == current_schema
                ):
                    ranked.append((1, relation))
                elif relation in search_order:
                    ranked.append((search_order[relation], relation))
            if ranked:
                visible_relations[table_name] = min(ranked)[1]

        return [
            row
            for row in rows
            if visible_relations.get(row["table_name"])
            == (row["database_name"], row["schema_name"])
        ]

    def _execute_visible_duckdb_relation_rows(
        self,
        connection: "Connection",
        statement: Any,
        params: Mapping[str, Any],
        schema: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in connection.execute(statement, params).mappings()]
        return self._visible_duckdb_relation_rows(connection, rows, schema)

    def _duckdb_column_rows(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
    ) -> List[Dict[str, Any]]:
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_columns()",
            (
                "database_name, schema_name, table_name, column_name, column_default, "
                "is_nullable, data_type, data_type_id, comment, column_index"
            ),
            schema=schema,
            filter_names=filter_names,
            include_internal_filter=True,
            suffix="ORDER BY table_name, column_index",
        )
        return self._execute_visible_duckdb_relation_rows(
            connection, stmt, params, schema
        )

    def _duckdb_tables(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
    ) -> Dict[str, Tuple[str, str]]:
        """Map each table name to the (database, schema) it resolves to."""
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_tables()",
            "database_name, schema_name, table_name",
            schema=schema,
            filter_names=filter_names,
            include_internal_filter=True,
            suffix="ORDER BY table_name",
        )
        rows = self._execute_visible_duckdb_relation_rows(
            connection, stmt, params, schema
        )
        return {
            row["table_name"]: (row["database_name"], row["schema_name"])
            for row in rows
        }

    def _duckdb_relation_rows(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
    ) -> List[Dict[str, Any]]:
        """Select kinds and scopes before resolving names that shadow each other."""
        scope = ObjectScope.ANY if scope is None else scope
        kind = ObjectKind.ANY if kind is None else kind
        stmt, params = self._duckdb_reflection_stmt(
            _DUCKDB_RELATION_COMMENTS,
            "database_name, schema_name, table_name, comment, temporary, relation_kind",
            schema=schema,
            # Temporary relations belong to the temp catalog. A listing must
            # include that catalog before applying the requested scope.
            filter_names=[] if filter_names == [] else filter_names,
            include_internal_filter=True,
        )
        sql = stmt.text
        if schema is None and filter_names is None and ObjectScope.TEMPORARY in scope:
            sql = sql.replace(
                "AND database_name = current_database()\nAND schema_name = current_schema()",
                "AND (temporary OR (database_name = current_database() "
                "AND schema_name = current_schema()))",
            )
        if schema is not None and ObjectScope.DEFAULT not in scope:
            database_name, schema_name = self.identifier_preparer._separate(schema)
            if database_name is None:
                sql = sql.replace(
                    "AND database_name = (\n",
                    "AND (temporary OR database_name = (\n",
                ).replace("    LIMIT 1\n)\n", "    LIMIT 1\n))\n")
        if ObjectScope.DEFAULT not in scope:
            sql += "\nAND temporary"
        elif ObjectScope.TEMPORARY not in scope:
            sql += "\nAND NOT temporary"
        kinds = []
        if ObjectKind.TABLE in kind:
            kinds.append("'table'")
        if ObjectKind.VIEW in kind:
            kinds.append("'view'")
        sql += "\nAND relation_kind IN (" + ", ".join(kinds or ["NULL"]) + ")"
        stmt = text(sql)
        if params.get("filter_names"):
            stmt = stmt.bindparams(bindparam("filter_names", expanding=True))
        return self._execute_visible_duckdb_relation_rows(
            connection, stmt, params, schema
        )

    def _duckdb_relations(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
    ) -> Dict[str, Tuple[str, str]]:
        return {
            row["table_name"]: (row["database_name"], row["schema_name"])
            for row in self._duckdb_relation_rows(
                connection, schema, filter_names, scope, kind
            )
        }

    def _duckdb_view_relations(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
    ) -> set[Tuple[str, str, str]]:
        stmt, params = self._duckdb_reflection_stmt(
            "(SELECT database_name, schema_name, view_name AS table_name, internal "
            "FROM duckdb_views())",
            "database_name, schema_name, table_name",
            schema=schema,
            filter_names=filter_names,
            include_internal_filter=True,
        )
        return {
            (row["database_name"], row["schema_name"], row["table_name"])
            for row in connection.execute(stmt, params).mappings()
        }

    def _duckdb_table_names(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
    ) -> List[str]:
        return list(self._duckdb_tables(connection, schema, filter_names))

    def _duckdb_constraint_rows(
        self,
        connection: "Connection",
        constraint_type: str,
        tables: Mapping[str, Tuple[str, str]],
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """duckdb_constraints() rows of ``tables``, grouped by table name.

        Rows are matched on the database and schema of each table, so a table
        that shadows another of the same name (a temp table, or a table in an
        attached database) does not pick up the other table's constraints.
        """
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_constraints()",
            (
                "database_name, schema_name, table_name, constraint_name, "
                "constraint_column_names, expression, referenced_table, "
                "referenced_column_names"
            ),
            schema=None,
            filter_names=filter_names,
            suffix=(
                "AND constraint_type = :constraint_type\n"
                "ORDER BY table_name, constraint_index"
            ),
        )
        params["constraint_type"] = constraint_type
        constraints: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in connection.execute(stmt, params).mappings():
            table_name = row["table_name"]
            if tables.get(table_name) == (row["database_name"], row["schema_name"]):
                constraints[table_name].append(dict(row))
        return constraints

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
        quote: Optional[str] = None
        index = 0
        while index < len(value):
            char = value[index]
            if quote is not None:
                current.append(char)
                if (
                    char == quote
                    and index + 1 < len(value)
                    and value[index + 1] == quote
                ):
                    current.append(value[index + 1])
                    index += 1
                elif char == quote:
                    quote = None
            else:
                if char in {"'", '"'}:
                    quote = char
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

    def _split_duckdb_field(self, field: str) -> Tuple[str, str]:
        """Split a STRUCT/UNION member (``"my field" VARCHAR``) into name and type."""
        field = field.strip()
        if field.startswith('"'):
            index = 1
            while index < len(field):
                if field[index] == '"':
                    if field[index + 1 : index + 2] == '"':
                        index += 2
                        continue
                    break
                index += 1
            name = field[1:index].replace('""', '"')
            return name, field[index + 1 :].strip()
        name, _, field_type = field.partition(" ")
        return name, field_type.strip()

    def _reflect_duckdb_nested_type(
        self, data_type: str, *, type_description: str
    ) -> Any:
        """Reflect STRUCT(...), MAP(...) and UNION(...) into the dialect's types.

        Falls back to NullType when a member type is not recognized or is an
        ENUM, whose type name DuckDB does not report inside nested types, so a
        reflected type always compiles back to valid DDL.
        """
        kind = data_type[: data_type.index("(")].strip().upper()
        members = self._split_duckdb_list(data_type[data_type.index("(") + 1 : -1])

        def reflect(member_type: str) -> Any:
            reflected = self._reflect_duckdb_data_type(
                member_type, None, {}, type_description=type_description
            )
            item_type = getattr(reflected, "item_type", reflected)
            while hasattr(item_type, "item_type"):
                item_type = item_type.item_type
            if isinstance(item_type, sqltypes.Enum):
                return sqltypes.NULLTYPE
            return reflected

        if kind == "MAP":
            if len(members) != 2:
                return sqltypes.NULLTYPE
            key_type, value_type = (reflect(member) for member in members)
            if sqltypes.NULLTYPE in (key_type, value_type):
                return sqltypes.NULLTYPE
            return Map(key_type, value_type)

        fields: Dict[str, Any] = {}
        for member in members:
            name, member_type = self._split_duckdb_field(member)
            reflected = reflect(member_type) if name and member_type else None
            if reflected is None or reflected == sqltypes.NULLTYPE:
                return sqltypes.NULLTYPE
            fields[name] = reflected
        if not fields:
            return sqltypes.NULLTYPE
        return Struct(fields) if kind == "STRUCT" else UnionType(fields)

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

    def _reflect_pg_type_compat(
        self,
        format_type: Optional[str],
        *,
        type_description: str,
    ) -> Any:
        reflect_type = getattr(super(), "_reflect_type", None)
        if reflect_type is not None:
            if SQLALCHEMY_VERSION >= Version("2.1.0b1"):
                return reflect_type(
                    format_type,
                    _EMPTY_NAMED_TYPE_LOADER,
                    type_description=type_description,
                    collation=None,
                )
            return reflect_type(
                format_type,
                {},
                {},
                type_description=type_description,
                collation=None,
            )

        if format_type is None:
            util.warn(
                "PostgreSQL format_type() returned NULL for %s" % type_description
            )
            return sqltypes.NULLTYPE

        args_match = re.search(r"\((.*)\)$", format_type)
        type_args: List[str]
        if args_match and args_match.group(1):
            type_args = [part.strip() for part in args_match.group(1).split(",")]
        else:
            type_args = []

        type_name = re.sub(r"\(.*\)$", "", format_type).strip().lower()
        schema_type = self.ischema_names.get(type_name)
        args: Tuple[Any, ...] = ()
        kwargs: Dict[str, Any] = {}

        if type_name == "numeric" and len(type_args) == 2:
            args = (int(type_args[0]), int(type_args[1]))
        elif type_name == "double precision":
            args = (53,)
        elif type_name in (
            "timestamp with time zone",
            "time with time zone",
        ):
            kwargs["timezone"] = True
            if len(type_args) == 1:
                kwargs["precision"] = int(type_args[0])
        elif type_name in (
            "timestamp without time zone",
            "time without time zone",
            "time",
        ):
            kwargs["timezone"] = False
            if len(type_args) == 1:
                kwargs["precision"] = int(type_args[0])
        elif type_name == "bit varying":
            kwargs["varying"] = True
            if len(type_args) == 1:
                args = (int(type_args[0]),)
        else:
            try:
                charlen = int(type_args[0])
            except (IndexError, ValueError):
                args = tuple(type_args)
            else:
                args = (charlen, *type_args[1:])

        if schema_type is None:
            util.warn(
                "Did not recognize type '%s' of %s" % (type_name, type_description)
            )
            return sqltypes.NULLTYPE

        return schema_type(*args, **kwargs)

    def _reflect_duckdb_data_type(
        self,
        data_type: str,
        data_type_id: Optional[Any],
        enum_rows: Dict[Any, Dict[str, Any]],
        type_description: str,
    ) -> Any:
        normalized = data_type.strip()
        array_sizes: List[Optional[int]] = []
        while True:
            match = re.search(r"\[[0-9]*\]$", normalized)
            if match is None:
                break
            size = match.group()[1:-1]
            array_sizes.append(int(size) if size else None)
            normalized = normalized[: match.start()].strip()

        upper = normalized.upper()
        if upper == "JSON":
            reflected = sqltypes.JSON()
        elif upper == "BLOB":
            reflected = sqltypes.LargeBinary()
        elif upper in {
            "TIMESTAMPTZ",
            "TIMESTAMPTZ_NS",
            "TIMESTAMP WITH TIME ZONE",
        }:
            reflected = sqltypes.TIMESTAMP(timezone=True)
        elif upper in {"TIMETZ", "TIME WITH TIME ZONE"}:
            reflected = sqltypes.TIME(timezone=True)
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
        elif upper.startswith(("STRUCT(", "MAP(", "UNION(")) and normalized.endswith(
            ")"
        ):
            reflected = self._reflect_duckdb_nested_type(
                normalized, type_description=type_description
            )
        elif upper == "STRUCT" or upper.startswith("TUPLE("):
            reflected = sqltypes.NULLTYPE
        else:
            reflected = self._reflect_pg_type_compat(
                normalized,
                type_description=type_description,
            )

        if array_sizes:
            if reflected == sqltypes.NULLTYPE:
                return sqltypes.NULLTYPE
            for size in reversed(array_sizes):
                if size is not None:
                    reflected = FixedArray(reflected, size)
                elif isinstance(reflected, sqltypes.ARRAY):
                    reflected = sqltypes.ARRAY(
                        reflected.item_type, dimensions=(reflected.dimensions or 1) + 1
                    )
                else:
                    reflected = sqltypes.ARRAY(reflected, dimensions=1)
        return reflected

    def _duckdb_columns_from_rows(
        self, connection: "Connection", rows: Sequence[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        enum_rows = self._duckdb_enum_rows(
            connection,
            [
                row["data_type_id"]
                for row in rows
                if row["data_type"].upper().startswith("ENUM")
            ],
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
                    "autoincrement": bool(
                        re.match(
                            r"^nextval\s*\(", row["column_default"] or "", re.IGNORECASE
                        )
                    ),
                    "comment": row["comment"],
                }
            )
        return dict(columns)

    def _reflection_schema_key(self, schema: Optional[str]) -> Optional[str]:
        return schema

    def _iter_reflection_results(
        self,
        schema: Optional[str],
        table_names: Iterable[str],
        reflected: Mapping[str, Any],
        default_factory: Callable[[], Any],
    ) -> Iterable[Tuple[Any, Any]]:
        schema_key = self._reflection_schema_key(schema)
        for table_name in table_names:
            value = reflected.get(table_name)
            if value is None:
                value = default_factory()
            yield (schema_key, table_name), value

    def _get_single_reflection_result(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str],
        get_multi: Any,
        default_factory: Any,
        **kw: Any,
    ) -> Any:
        scope = kw.pop("scope", ObjectScope.ANY)
        kind = kw.pop("kind", ObjectKind.ANY)
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
        if self._duckdb_relation_exists(connection, table_name, schema):
            return default_factory()
        raise NoSuchTableError(table_name)

    @cache  # type: ignore[call-arg]
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

    def has_multi_table(
        self,
        connection: "Connection",
        table_names: Sequence[str],
        schema: Optional[str] = None,
        **kw: Any,
    ) -> Iterable[Tuple[Tuple[Optional[str], str], bool]]:
        relations = (
            self._duckdb_relations(
                connection, schema, table_names, ObjectScope.ANY, ObjectKind.ANY
            )
            if table_names
            else {}
        )
        return [
            ((schema, table_name), table_name in relations)
            for table_name in table_names
        ]

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
        tables = self._duckdb_relations(connection, schema, filter_names, scope, kind)
        rows = self._duckdb_constraint_rows(
            connection, "PRIMARY KEY", tables, schema, list(tables)
        )
        constraints = {
            table_name: {
                "name": row["constraint_name"],
                "constrained_columns": list(row["constraint_column_names"] or []),
            }
            for table_name, (row, *_) in rows.items()
        }
        return self._iter_reflection_results(
            schema, tables, constraints, ReflectionDefaults.pk_constraint
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
        return self._get_single_reflection_result(
            connection,
            table_name,
            schema,
            self.get_multi_foreign_keys,
            ReflectionDefaults.foreign_keys,
            **kw,
        )

    def get_multi_foreign_keys(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        postgresql_ignore_search_path: bool = False,
        **kw: Any,
    ) -> Iterable[Tuple[Any, Any]]:
        tables = self._duckdb_relations(connection, schema, filter_names, scope, kind)
        rows = self._duckdb_constraint_rows(
            connection, "FOREIGN KEY", tables, schema, list(tables)
        )
        # DuckDB foreign keys stay within the resolved child's schema.
        referred_schemas: Dict[str, Optional[str]] = {}
        if rows and schema is None:
            current_database, current_schema = connection.execute(
                text("SELECT current_database(), current_schema()")
            ).one()
            quote = self.identifier_preparer.quote
            for table_name, (database_name, schema_name) in tables.items():
                referred_schemas[table_name] = (
                    None
                    if (database_name, schema_name)
                    == (current_database, current_schema)
                    else schema_name
                    if database_name == current_database
                    else f"{quote(database_name)}.{quote(schema_name)}"
                )
        foreign_keys = {
            table_name: [
                {
                    "name": row["constraint_name"],
                    "constrained_columns": list(row["constraint_column_names"]),
                    "referred_schema": schema
                    if schema is not None
                    else referred_schemas.get(table_name),
                    "referred_table": row["referenced_table"],
                    "referred_columns": list(row["referenced_column_names"]),
                    "options": {},
                    "comment": None,
                }
                for row in table_rows
            ]
            for table_name, table_rows in rows.items()
        }
        return self._iter_reflection_results(
            schema, tables, foreign_keys, ReflectionDefaults.foreign_keys
        )

    @cache  # type: ignore[call-arg]
    def get_unique_constraints(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List["ReflectedUniqueConstraint"]:
        return self._get_single_reflection_result(
            connection,
            table_name,
            schema,
            self.get_multi_unique_constraints,
            ReflectionDefaults.unique_constraints,
            **kw,
        )

    def get_multi_unique_constraints(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        **kw: Any,
    ) -> Iterable[Tuple[Any, Any]]:
        tables = self._duckdb_relations(connection, schema, filter_names, scope, kind)
        rows = self._duckdb_constraint_rows(
            connection, "UNIQUE", tables, schema, list(tables)
        )
        unique_constraints = {
            table_name: [
                {
                    "name": row["constraint_name"],
                    "column_names": list(row["constraint_column_names"]),
                    "comment": None,
                }
                for row in table_rows
            ]
            for table_name, table_rows in rows.items()
        }
        return self._iter_reflection_results(
            schema, tables, unique_constraints, ReflectionDefaults.unique_constraints
        )

    @cache  # type: ignore[call-arg]
    def get_check_constraints(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List["ReflectedCheckConstraint"]:
        return self._get_single_reflection_result(
            connection,
            table_name,
            schema,
            self.get_multi_check_constraints,
            ReflectionDefaults.check_constraints,
            **kw,
        )

    def get_multi_check_constraints(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        **kw: Any,
    ) -> Iterable[Tuple[Any, Any]]:
        tables = self._duckdb_relations(connection, schema, filter_names, scope, kind)
        rows = self._duckdb_constraint_rows(
            connection, "CHECK", tables, schema, list(tables)
        )
        check_constraints = {
            table_name: [
                {
                    "name": row["constraint_name"],
                    "sqltext": _strip_enclosing_parentheses(row["expression"] or ""),
                    "comment": None,
                }
                for row in table_rows
            ]
            for table_name, table_rows in rows.items()
        }
        return self._iter_reflection_results(
            schema, tables, check_constraints, ReflectionDefaults.check_constraints
        )

    @cache  # type: ignore[call-arg]
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
        tables = self._duckdb_relations(connection, schema, filter_names, scope, kind)
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_indexes()",
            (
                "database_name, schema_name, table_name, index_name, "
                "expressions, is_unique"
            ),
            schema=None,
            filter_names=list(tables),
            suffix="ORDER BY table_name, index_name",
        )
        indexes: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in connection.execute(stmt, params).mappings():
            if tables.get(row["table_name"]) != (
                row["database_name"],
                row["schema_name"],
            ):
                continue
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
        return self._iter_reflection_results(
            schema, tables, indexes, ReflectionDefaults.indexes
        )

    def get_multi_table_comment(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        **kw: Any,
    ) -> Iterable[Tuple[Any, Any]]:
        rows = self._duckdb_relation_rows(connection, schema, filter_names, scope, kind)
        return [
            (
                (self._reflection_schema_key(schema), row["table_name"]),
                {"text": row["comment"]},
            )
            for row in rows
        ]

    @cache  # type: ignore[call-arg]
    def get_temp_table_names(  # type: ignore[no-untyped-def]
        self, connection: "Connection", **kw: "Any"
    ):
        rows = connection.execute(
            text(
                "SELECT table_name FROM duckdb_tables() "
                "WHERE temporary AND NOT internal ORDER BY table_name"
            )
        )
        return [table_name for (table_name,) in rows]

    @cache  # type: ignore[call-arg]
    def get_temp_view_names(  # type: ignore[no-untyped-def]
        self, connection: "Connection", schema: "Optional[str]" = None, **kw: "Any"
    ):
        rows = connection.execute(
            text(
                "SELECT view_name FROM duckdb_views() "
                "WHERE temporary AND NOT internal ORDER BY view_name"
            )
        )
        return [view_name for (view_name,) in rows]

    @cache  # type: ignore[call-arg]
    def get_enums(  # type: ignore[no-untyped-def]
        self, connection: "Connection", schema: "Optional[str]" = None, **kw: "Any"
    ):
        """User-defined ENUM types; ``schema="*"`` lists every schema.

        PostgreSQL's pg_type query also returned DuckDB's built-in ``enum``
        type, without labels.
        """
        sql = (
            "SELECT database_name, schema_name, type_name, labels, "
            "database_name = current_database() AS in_current_database, "
            "database_name = current_database() "
            "AND schema_name = current_schema() AS visible "
            "FROM duckdb_types() WHERE logical_type = 'ENUM' AND NOT internal\n"
        )
        params: Dict[str, Any] = {}
        if schema != "*":
            where_sql, params = self._build_query_where(
                schema_name=schema, default_to_current_schema=True
            )
            sql += where_sql
        sql += "ORDER BY database_name, schema_name, type_name"
        quote = self.identifier_preparer.quote
        return [
            {
                "name": row["type_name"],
                "schema": row["schema_name"]
                if row["in_current_database"]
                else f"{quote(row['database_name'])}.{quote(row['schema_name'])}",
                "visible": bool(row["visible"]),
                "labels": list(row["labels"] or []),
            }
            for row in connection.execute(text(sql), params).mappings()
        ]

    @cache  # type: ignore[call-arg]
    def has_schema(self, connection: "Connection", schema: str, **kw: Any) -> bool:
        """Whether ``schema`` (``schema`` or ``database.schema``) exists."""
        database_name, schema_name = self.identifier_preparer._separate(schema)
        sql = "SELECT 1 FROM duckdb_schemas() WHERE schema_name = :schema_name"
        params = {"schema_name": schema_name}
        if database_name is not None:
            sql += " AND database_name = :database_name"
            params["database_name"] = database_name
        return connection.execute(text(sql + " LIMIT 1"), params).first() is not None

    @cache  # type: ignore[call-arg]
    def get_table_options(
        self,
        connection: "Connection",
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> Dict[str, Any]:
        return self._get_single_reflection_result(
            connection, table_name, schema, self.get_multi_table_options, dict, **kw
        )

    def get_multi_table_options(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        **kw: Any,
    ) -> Iterable[Tuple[Any, Any]]:
        table_names = self._duckdb_relations(
            connection, schema, filter_names, scope, kind
        )
        return self._iter_reflection_results(schema, table_names, {}, dict)

    def create_connect_args(self, url: SAURL) -> Tuple[tuple, dict]:
        opts = url.translate_connect_args(database="database")
        database = opts.get("database")
        if database in {None, ""}:
            database = ":memory:"
        opts["database"], opts["url_config"] = split_database_and_url_config(
            database, dict(url.query)
        )
        return (), opts

    @classmethod
    def import_dbapi(cls) -> Any:
        return cls.dbapi()

    def _get_execution_options(self, context: Optional[Any]) -> Dict[str, Any]:
        if context is None:
            return {}
        return getattr(context, "execution_options", {}) or {}

    def _bulk_insert_register_columns(
        self, context: Any, parameters: Sequence[Any]
    ) -> Optional[List[str]]:
        """Column names for the register fast path, or None when it can't apply."""
        if not parameters:
            return None
        compiled = getattr(context, "compiled", None)
        if compiled is None:
            return None
        if getattr(compiled, "effective_returning", None):
            return None
        stmt = getattr(compiled, "statement", None)
        table = getattr(stmt, "table", None)
        if table is None:
            return None
        if getattr(stmt, "_post_values_clause", None) is not None:
            return None
        # The register path replaces VALUES with a column projection. Explicit
        # expressions and SQL defaults must retain their original compiled SQL.
        if getattr(stmt, "_values", None) or getattr(stmt, "_ordered_values", None):
            return None
        # The rewritten INSERT ... SELECT would drop prefixes (OR REPLACE,
        # OR IGNORE), hints and CTEs.
        if (
            getattr(stmt, "_prefixes", None)
            or getattr(stmt, "_hints", None)
            or getattr(stmt, "_statement_hints", None)
            or getattr(stmt, "_independent_ctes", None)
        ):
            return None
        if any(
            column.default is not None
            and (column.default.is_clause_element or column.default.is_sequence)
            for column in table.columns
        ):
            return None

        column_keys = getattr(compiled, "positiontup", None)
        if not column_keys:
            column_keys = getattr(compiled, "column_keys", None)
        if not column_keys:
            column_keys = _infer_bulk_insert_column_keys(parameters)
        if not column_keys:
            return None

        column_names = [
            str(getattr(column_key, "key", column_key)) for column_key in column_keys
        ]
        # Bind keys need not be physical column names. Decline the optimization
        # unless the mapping is unambiguous; normal executemany handles these.
        if any(key not in table.c or table.c[key].name != key for key in column_names):
            return None
        for name in column_names:
            type_impl = table.c[name].type.dialect_impl(self)
            if type_impl._has_bind_expression:
                return None
            while isinstance(
                type_impl, (sqltypes.TypeDecorator, sqltypes.ARRAY, FixedArray)
            ):
                type_impl = (
                    type_impl.impl
                    if isinstance(type_impl, sqltypes.TypeDecorator)
                    else type_impl.item_type.dialect_impl(self)
                )
            # The DBAPI representation of a MAP is a key/value dictionary.
            # Inferred Arrow data treats it as STRUCT, not MAP.
            if isinstance(type_impl, Map):
                return None
        return column_names

    def _reaches_copy_threshold(self, context: Any, parameters: Sequence[Any]) -> bool:
        options = self._get_execution_options(context)
        copy_threshold = options.get(
            "duckdb_copy_threshold", self.duckdb_copy_threshold
        )
        return bool(copy_threshold) and len(parameters) >= copy_threshold

    def _routes_insertmanyvalues_to_register(self, context: Any) -> bool:
        """Whether a multi-row INSERT should use the register fast path.

        ``use_insertmanyvalues_wo_returning`` sends every multi-row INSERT
        through batched multi-VALUES statements, which bypass
        ``do_executemany``; large plain INSERTs are switched back so they load
        through a registered Arrow table or DataFrame instead.
        """
        parameters = context.parameters
        if not _has_bulk_insert_data_library() or not self._reaches_copy_threshold(
            context, parameters
        ):
            return False
        columns = self._bulk_insert_register_columns(context, parameters)
        if columns is None:
            return False
        data = _build_bulk_insert_data(parameters, columns)
        if data is None:
            return False
        # Reuse the conversion in do_executemany; if conversion is unsafe,
        # retain the normal batched VALUES path.
        context._duckdb_bulk_data = data
        return True

    def _bulk_insert_via_register(
        self,
        cursor: Any,
        context: Any,
        parameters: Sequence[Any],
    ) -> bool:
        column_names = self._bulk_insert_register_columns(context, parameters)
        if column_names is None:
            return False
        table = context.compiled.statement.table
        data = getattr(context, "_duckdb_bulk_data", None)
        if data is None:
            data = _build_bulk_insert_data(parameters, column_names)
        else:
            del context._duckdb_bulk_data
        if data is None:
            return False

        view_name = f"__duckdb_sa_bulk_{uuid.uuid4().hex}"
        dbapi_conn = cursor.connection
        preparer: Any = getattr(
            context, "identifier_preparer", self.identifier_preparer
        )
        target = preparer.format_table(table)
        schema_translate_map = self._get_execution_options(context).get(
            "schema_translate_map"
        )
        if schema_translate_map:
            render_schema_translates = getattr(
                preparer, "_render_schema_translates", None
            )
            if render_schema_translates is None:
                return False
            target = render_schema_translates(target, schema_translate_map)
        columns = ", ".join(preparer.quote(col) for col in column_names)
        insert_sql = (
            f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {view_name}"
        )
        dbapi_conn.register(view_name, data)
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
            if self._reaches_copy_threshold(context, parameters):
                if self._bulk_insert_via_register(cursor, context, parameters):
                    return None
        return DefaultDialect.do_executemany(
            self, cursor, statement, parameters, context
        )

    def is_disconnect(self, e: Exception, connection: Any, cursor: Any) -> bool:
        if isinstance(e, duckdb.Error):
            if isinstance(e, _DUCKDB_CONNECTION_EXCEPTIONS):
                return True
            # IOException and HTTPException subclass OperationalError but report
            # file or remote I/O failures (a missing CSV, an S3 timeout) on a
            # connection that is still usable. Invalidating it would discard
            # the pooled connection and, for :memory:, the whole database.
            if not isinstance(e, duckdb.OperationalError) or isinstance(
                e, _DUCKDB_IO_EXCEPTIONS
            ):
                return False
        message = str(e).lower()
        return any(pattern in message for pattern in DISCONNECT_ERROR_PATTERNS)

    def _prepare_retry(self, cursor: Any) -> bool:
        """Return whether the failed statement can run again on this connection.

        A failed statement aborts DuckDB's open transaction, so a retry inside
        it can only fail with "Current transaction is aborted". Retrying is safe
        outside an explicit transaction, or when the failed statement was the
        first one in its transaction: rolling back and beginning again then
        loses no earlier work.
        """
        connection = cast(Any, getattr(cursor, "connection", None))
        statements = getattr(connection, "_transaction_statements", None)
        if statements is None:
            return True
        if statements > 1:
            return False
        connection.rollback()
        connection.begin()
        return True

    def _execute_with_retry(
        self,
        statement: str,
        context: Optional[Any],
        executor: Callable[[], Any],
        cursor: Any = None,
    ) -> Any:
        options = self._get_execution_options(context)
        retry_count = int(options.get("duckdb_retry_count", 0) or 0)
        if options.get("duckdb_retry_on_transient") and retry_count == 0:
            retry_count = 1
        if retry_count <= 0 or not _is_idempotent_statement(statement):
            return executor()
        backoff = options.get("duckdb_retry_backoff")
        attempt = 0
        first_error: Optional[Exception] = None
        while True:
            try:
                return executor()
            except Exception as exc:
                if first_error is not None and _is_aborted_transaction_error(exc):
                    # a transaction the dialect does not track (a raw BEGIN)
                    # was aborted by the first failure; report that failure
                    raise first_error from None
                if attempt >= retry_count or not _is_transient_error(exc):
                    raise
                if not self._prepare_retry(cursor):
                    raise
                first_error = first_error or exc
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

        self._execute_with_retry(statement, context, executor, cursor)

    def do_execute_no_params(
        self, cursor: Any, statement: str, context: Optional[Any] = None
    ) -> None:
        def executor() -> Any:
            return DefaultDialect.do_execute_no_params(self, cursor, statement, context)

        self._execute_with_retry(statement, context, executor, cursor)

    def _pg_class_filter_scope_schema(
        self,
        query: Select,
        schema: Optional[str],
        scope: Any,
        pg_class_table: Any = None,
    ) -> Any:
        # Scope by schema, but strip any database prefix (DuckDB uses db.schema).
        # This will not work if a schema or table name is not unique!
        schema_arg = schema
        if schema is not None:
            _, schema_name = self.identifier_preparer._separate(schema)
            schema_arg = schema_name
        return super()._pg_class_filter_scope_schema(
            query,
            schema=schema_arg,
            scope=scope,
            pg_class_table=pg_class_table,
        )

    # Reflect columns from duckdb_columns() rather than PostgreSQL's pg_catalog
    # queries, which do not map DuckDB types or attached databases.
    def get_multi_columns(
        self,
        connection: "Connection",
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        scope: Any = None,
        kind: Any = None,
        **kw: Any,
    ) -> Any:
        relations = self._duckdb_relations(
            connection, schema, filter_names, scope, kind
        )
        if not relations:
            return iter(())
        stmt, params = self._duckdb_reflection_stmt(
            "duckdb_columns()",
            "database_name, schema_name, table_name, column_name, column_default, "
            "is_nullable, data_type, data_type_id, comment, column_index",
            schema=None,
            filter_names=list(relations),
            include_internal_filter=True,
            suffix="ORDER BY table_name, column_index",
        )
        rows = [
            dict(row)
            for row in connection.execute(stmt, params).mappings()
            if relations.get(row["table_name"])
            == (row["database_name"], row["schema_name"])
        ]
        columns = self._duckdb_columns_from_rows(connection, rows)
        schema_key = self._reflection_schema_key(schema)
        return (
            ((schema_key, table_name), table_columns)
            for table_name, table_columns in columns.items()
        )


def _register_alembic_support() -> None:
    """Register the Alembic implementation once Alembic has been imported.

    Alembic is optional and heavy to import, so it is only loaded when the
    application (for example the ``alembic`` command running ``env.py``) has
    imported it already.
    """
    if "alembic" not in sys.modules or "duckdb_sqlalchemy.alembic_impl" in sys.modules:
        return
    try:
        from . import alembic_impl  # noqa: F401
    except Exception as exc:  # pragma: no cover - depends on the Alembic version
        warnings.warn(
            f"duckdb_sqlalchemy could not register its Alembic support: {exc}",
            DuckDBEngineWarning,
            stacklevel=2,
        )


_register_alembic_support()


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
