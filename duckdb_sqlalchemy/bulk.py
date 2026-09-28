import csv
import math
import tempfile
import uuid
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.expression import Executable
from sqlalchemy.sql.expression import table as table_clause
from sqlalchemy.sql.selectable import CompoundSelect, Select
from sqlalchemy.sql.visitors import InternalTraversal

from ._row_shape import rows_as_sequences
from ._validation import (
    validate_dotted_identifier,
    validate_identifier,
    validate_identifier_list,
)

TableLike = Union[str, Any]


def _quote_literal(value: Any) -> str:
    if isinstance(value, Path):
        value = str(value)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"COPY option values must be finite numbers, got {value!r}")
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _render_option_value(value: Any) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_quote_literal(str(key))}: {_render_option_value(item)}"
            for key, item in value.items()
        )
        return "{" + items + "}"
    return _quote_literal(value)


def _format_copy_options(options: Mapping[str, Any]) -> str:
    if not options:
        return ""
    parts = []
    for key, value in options.items():
        if value is None:
            continue
        opt_key = validate_identifier(str(key), kind="COPY option key").upper()
        if isinstance(value, (list, tuple)):
            inner = ", ".join(_quote_literal(v) for v in value)
            parts.append(f"{opt_key} ({inner})")
        else:
            parts.append(f"{opt_key} {_render_option_value(value)}")
    if not parts:
        return ""
    return " (" + ", ".join(parts) + ")"


def _get_identifier_preparer(connection: Any) -> Any:
    return getattr(getattr(connection, "dialect", None), "identifier_preparer", None)


def _effective_schema(connection: Any, table: Any) -> Optional[str]:
    # Apply the connection's schema_translate_map, as compiled statements do.
    schema_for_object = getattr(connection, "schema_for_object", None)
    if callable(schema_for_object):
        return schema_for_object(table)
    return getattr(table, "schema", None)


def _format_table(connection: Any, table: TableLike) -> str:
    if hasattr(table, "name"):
        schema = _effective_schema(connection, table)
        preparer = _get_identifier_preparer(connection)
        if preparer is not None:
            if schema != getattr(table, "schema", None):
                table = table_clause(cast(str, table.name), schema=schema)
            return preparer.format_table(table)
        name = getattr(table, "name", None)
        if schema:
            schema_name = validate_dotted_identifier(
                str(schema), kind="table schema identifier"
            )
            table_name = validate_identifier(str(name), kind="table identifier")
            return f"{schema_name}.{table_name}"
        return validate_identifier(str(name), kind="table identifier")
    table_name = str(table)
    validate_dotted_identifier(table_name, kind="table identifier")
    return table_name


def _format_columns(connection: Any, columns: Optional[Sequence[str]]) -> str:
    if not columns:
        return ""
    validated_columns = validate_identifier_list(columns, kind="column identifier")
    preparer = _get_identifier_preparer(connection)
    if preparer is None:
        cols = ", ".join(validated_columns)
    else:
        cols = ", ".join(preparer.quote_identifier(col) for col in validated_columns)
    return f" ({cols})"


def _execute_sql(connection: Any, statement: str) -> Any:
    if hasattr(connection, "exec_driver_sql"):
        return connection.exec_driver_sql(statement)
    return connection.execute(statement)


class _CopyToParquet(Executable, ClauseElement):
    inherit_cache = False
    _traverse_internals = [("selectable", InternalTraversal.dp_clauseelement)]

    def _generate_cache_key(self) -> None:
        # Keep bind traversal without caching literal destinations or COPY options.
        return None

    def __init__(
        self,
        selectable: Union[Select, CompoundSelect],
        path: Union[str, Path],
        copy_options: Mapping[str, Any],
    ) -> None:
        self.selectable = selectable
        self.path = path
        self._copy_options = copy_options


@compiles(_CopyToParquet, "duckdb")
def _compile_copy_to_parquet(
    statement: _CopyToParquet, compiler: Any, **kw: Any
) -> str:
    # COPY returns its own metadata, not the SELECT column types/processors.
    with compiler._nested_result():
        query = compiler.process(statement.selectable, **kw)
    options = _format_copy_options({"format": "parquet", **statement._copy_options})
    return f"COPY ({query}) TO {_quote_literal(statement.path)}{options}"


def copy_to_parquet(
    connection: Any,
    selectable: Union[Select, CompoundSelect],
    path: Union[str, Path],
    *,
    parameters: Optional[Mapping[str, Any]] = None,
    **options: Any,
) -> Any:
    """Export a SELECT to Parquet and return DuckDB's COPY result.

    By default the result contains a single ``Count`` column. COPY options
    may change the result schema.
    The output path is a literal for compatibility with older DuckDB releases.
    COPY overwrites an existing single-file destination, and its file write is
    not rolled back with the surrounding SQL transaction.
    """

    if not isinstance(selectable, (Select, CompoundSelect)):
        raise TypeError("copy_to_parquet requires a Select or CompoundSelect")
    if any(str(key).lower() == "format" for key in options):
        raise ValueError("copy_to_parquet always uses FORMAT parquet")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("copy_to_parquet parameters must be a mapping")
    statement = _CopyToParquet(selectable, path, options)
    return connection.execute(statement, parameters or {})


def _close_and_unlink_tempfile(tmp: Any) -> None:
    path = tmp.name
    try:
        tmp.flush()
    finally:
        tmp.close()
    Path(path).unlink(missing_ok=True)


_BINARY_TYPES = (bytes, bytearray, memoryview)
_NULL_STRING_OPTIONS = ("nullstr", "null")


def _resolve_null_marker(
    copy_options: Mapping[str, Any],
) -> Tuple[str, Dict[str, Any]]:
    """Pick the CSV marker written for ``None`` and the matching NULLSTR option.

    Without a user ``nullstr``/``null`` option, a unique marker keeps empty
    strings distinct from NULL. A user-supplied value is honored as given and
    its (first) string is used to write ``None``.
    """

    options = dict(copy_options)
    user_value = None
    for key in [k for k in options if str(k).lower() in _NULL_STRING_OPTIONS]:
        value = options.pop(key)
        if value is not None:
            user_value = value
    if user_value is None:
        marker = f"__duckdb_sqlalchemy_null_{uuid.uuid4().hex}__"
        options["nullstr"] = marker
        return marker, options
    if isinstance(user_value, str):
        marker = user_value
    elif (
        isinstance(user_value, (list, tuple))
        and user_value
        and all(isinstance(item, str) for item in user_value)
    ):
        marker = user_value[0]
    else:
        raise ValueError(
            "copy_from_rows nullstr must be a string or a non-empty list of strings"
        )
    options["nullstr"] = user_value
    return marker, options


def _csv_value(value: Any, null_marker: str) -> Any:
    if value is None:
        return null_marker
    if isinstance(value, _BINARY_TYPES):
        raise TypeError(
            "copy_from_rows cannot write binary values "
            f"({type(value).__name__}) to CSV; use copy_from_parquet or a "
            "regular INSERT for binary data"
        )
    return value


def _copy_rows_as_csv_chunks(
    connection: Any,
    table: TableLike,
    rows: Iterable[Sequence[Any]],
    *,
    columns: Optional[Sequence[str]],
    chunk_size: int,
    include_header: bool,
    copy_options: Mapping[str, Any],
    null_marker: str,
) -> None:
    def open_writer() -> Tuple[Any, Any, int]:
        tmp = tempfile.NamedTemporaryFile(
            "w", newline="", suffix=".csv", delete=False, encoding="utf-8"
        )
        writer = csv.writer(tmp)
        if include_header and columns:
            writer.writerow(columns)
        return tmp, writer, 0

    def flush_chunk(tmp: Any) -> None:
        tmp.flush()
        path = tmp.name
        tmp.close()
        try:
            copy_from_csv(
                connection,
                table,
                path,
                columns=columns if columns else None,
                **copy_options,
            )
        finally:
            Path(path).unlink(missing_ok=True)

    tmp = None
    writer = None
    count = 0

    try:
        for row in rows:
            if tmp is None or writer is None:
                tmp, writer, count = open_writer()
            elif chunk_size and count >= chunk_size:
                flush_chunk(tmp)
                tmp, writer, count = open_writer()

            writer.writerow([_csv_value(value, null_marker) for value in row])
            count += 1

        if tmp is not None and count:
            flush_chunk(tmp)
            tmp = None
    finally:
        if tmp is not None and not tmp.closed:
            _close_and_unlink_tempfile(tmp)


def _copy_rows_as_sequences(
    first: Union[Mapping[str, Any], Sequence[Any]],
    rows: Iterable[Union[Mapping[str, Any], Sequence[Any]]],
    columns: Optional[Sequence[str]],
) -> Tuple[Iterable[Sequence[Any]], Optional[Sequence[str]]]:
    return rows_as_sequences(first, rows, columns)


def copy_from_parquet(
    connection: Any,
    table: TableLike,
    path: Union[str, Path],
    *,
    columns: Optional[Sequence[str]] = None,
    **options: Any,
) -> Any:
    return _copy_from_file(
        connection,
        table,
        path,
        format_name="parquet",
        columns=columns,
        **options,
    )


def copy_from_csv(
    connection: Any,
    table: TableLike,
    path: Union[str, Path],
    *,
    columns: Optional[Sequence[str]] = None,
    **options: Any,
) -> Any:
    return _copy_from_file(
        connection,
        table,
        path,
        format_name="csv",
        columns=columns,
        **options,
    )


def _copy_from_file(
    connection: Any,
    table: TableLike,
    path: Union[str, Path],
    *,
    format_name: str,
    columns: Optional[Sequence[str]] = None,
    **options: Any,
) -> Any:
    validate_identifier(format_name, kind="COPY format")
    table_name = _format_table(connection, table)
    column_clause = _format_columns(connection, columns)
    path_literal = _quote_literal(path)
    copy_options = {"format": format_name, **options}
    options_clause = _format_copy_options(copy_options)
    statement = f"COPY {table_name}{column_clause} FROM {path_literal}{options_clause}"
    return _execute_sql(connection, statement)


def copy_from_rows(
    connection: Any,
    table: TableLike,
    rows: Iterable[Union[Mapping[str, Any], Sequence[Any]]],
    *,
    columns: Optional[Sequence[str]] = None,
    chunk_size: int = 100000,
    include_header: bool = False,
    **copy_options: Any,
) -> Any:
    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return None

    header = copy_options.pop("header", include_header)
    null_marker, copy_options = _resolve_null_marker(copy_options)
    copy_options = {"header": header, **copy_options}

    chunked_rows, columns = _copy_rows_as_sequences(first, iterator, columns)
    if header and columns is None:
        raise ValueError(
            "copy_from_rows header mode requires columns for sequence rows"
        )
    _copy_rows_as_csv_chunks(
        connection,
        table,
        chunked_rows,
        columns=columns,
        chunk_size=chunk_size,
        include_header=header,
        copy_options=copy_options,
        null_marker=null_marker,
    )
    return None
