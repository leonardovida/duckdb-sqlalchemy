import csv
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple, Union

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
        opt_key = str(key).upper()
        if isinstance(value, (list, tuple)):
            inner = ", ".join(_quote_literal(v) for v in value)
            parts.append(f"{opt_key} ({inner})")
        else:
            parts.append(f"{opt_key} {_quote_literal(value)}")
    if not parts:
        return ""
    return " (" + ", ".join(parts) + ")"


def _format_table(connection: Any, table: TableLike) -> str:
    if hasattr(table, "name"):
        preparer = getattr(
            getattr(connection, "dialect", None), "identifier_preparer", None
        )
        if preparer is not None:
            return preparer.format_table(table)
        schema = getattr(table, "schema", None)
        name = getattr(table, "name", None)
        if schema:
            return f"{schema}.{name}"
        return str(name)
    return str(table)


def _format_columns(connection: Any, columns: Optional[Sequence[str]]) -> str:
    if not columns:
        return ""
    preparer = getattr(
        getattr(connection, "dialect", None), "identifier_preparer", None
    )
    if preparer is None:
        cols = ", ".join(columns)
    else:
        cols = ", ".join(preparer.quote_identifier(col) for col in columns)
    return f" ({cols})"


def _execute_sql(connection: Any, statement: str) -> Any:
    if hasattr(connection, "exec_driver_sql"):
        return connection.exec_driver_sql(statement)
    return connection.execute(statement)


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

    if isinstance(first, Mapping):
        if columns is None:
            columns = list(first.keys())

        def row_to_seq(row: Mapping[str, Any]) -> Sequence[Any]:
            return [row.get(col) for col in columns or []]
    else:

        def row_to_seq(row: Sequence[Any]) -> Sequence[Any]:
            return row

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
            try:
                Path(path).unlink()
            except FileNotFoundError:
                pass

    copy_options = {"header": include_header, **copy_options}

    tmp, writer, count = open_writer()
    writer.writerow(row_to_seq(first))
    count += 1

    for row in iterator:
        writer.writerow(row_to_seq(row))
        count += 1
        if chunk_size and count >= chunk_size:
            flush_chunk(tmp)
            tmp, writer, count = open_writer()

    if count:
        flush_chunk(tmp)
