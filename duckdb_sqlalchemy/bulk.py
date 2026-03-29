import csv
import tempfile
from itertools import chain
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple, Union, cast

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
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


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
            parts.append(f"{opt_key} {_quote_literal(value)}")
    if not parts:
        return ""
    return " (" + ", ".join(parts) + ")"


def _get_identifier_preparer(connection: Any) -> Any:
    return getattr(getattr(connection, "dialect", None), "identifier_preparer", None)


def _format_table(connection: Any, table: TableLike) -> str:
    if hasattr(table, "name"):
        preparer = _get_identifier_preparer(connection)
        if preparer is not None:
            return preparer.format_table(table)
        schema = getattr(table, "schema", None)
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


def _unlink_if_exists(path: Union[str, Path]) -> None:
    try:
        Path(path).unlink()
    except FileNotFoundError:
        pass


def _close_and_unlink_tempfile(tmp: Any) -> None:
    path = tmp.name
    try:
        tmp.flush()
    finally:
        tmp.close()
    _unlink_if_exists(path)


def _copy_rows_as_csv_chunks(
    connection: Any,
    table: TableLike,
    rows: Iterable[Sequence[Any]],
    *,
    columns: Optional[Sequence[str]],
    chunk_size: int,
    include_header: bool,
    copy_options: Mapping[str, Any],
) -> None:
    def open_writer() -> Tuple[Any, Any, int]:
        tmp = tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False)
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
            _unlink_if_exists(path)

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

            writer.writerow(row)
            count += 1

        if tmp is not None and count:
            flush_chunk(tmp)
            tmp = None
    finally:
        if tmp is not None and not tmp.closed:
            _close_and_unlink_tempfile(tmp)


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

    copy_options = {"header": include_header, **copy_options}

    if isinstance(first, Mapping):
        if columns is None:
            columns = [str(col) for col in cast(Mapping[str, Any], first).keys()]

        def row_to_seq(row: Mapping[str, Any]) -> Sequence[Any]:
            return [row.get(col) for col in columns or []]

        first_row = row_to_seq(cast(Mapping[str, Any], first))
        remaining_rows = (row_to_seq(cast(Mapping[str, Any], row)) for row in iterator)
    else:
        first_row = cast(Sequence[Any], first)
        remaining_rows = (cast(Sequence[Any], row) for row in iterator)

    chunked_rows = chain((first_row,), remaining_rows)
    _copy_rows_as_csv_chunks(
        connection,
        table,
        chunked_rows,
        columns=columns,
        chunk_size=chunk_size,
        include_header=include_header,
        copy_options=copy_options,
    )
    return None
