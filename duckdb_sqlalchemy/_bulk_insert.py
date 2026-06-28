from __future__ import annotations

from typing import Any, Optional, Sequence, cast

from ._row_shape import infer_mapping_column_keys, rows_use_mapping_shape


def infer_bulk_insert_column_keys(rows: Sequence[Any]) -> Optional[list[str]]:
    return infer_mapping_column_keys(rows)


def build_bulk_insert_dataframe(
    rows: Sequence[Any], column_names: Sequence[str]
) -> Any:
    try:
        import pandas as pd  # type: ignore[import-not-found]
    except Exception:
        return None

    try:
        if rows_use_mapping_shape(rows):
            return pd.DataFrame.from_records(rows, columns=column_names)
        return pd.DataFrame(rows, columns=cast(Any, column_names))
    except Exception:
        return None


def build_bulk_insert_arrow_table(
    rows: Sequence[Any], column_names: Sequence[str]
) -> Any:
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
    except Exception:
        return None

    try:
        if rows_use_mapping_shape(rows):
            table = pa.Table.from_pylist(rows)
            if column_names:
                return table.select(column_names)
            return table
        columns = list(zip(*rows)) if rows else [[] for _ in column_names]
        return pa.Table.from_arrays(columns, names=column_names)
    except Exception:
        return None


def build_bulk_insert_data(rows: Sequence[Any], column_names: Sequence[str]) -> Any:
    data = build_bulk_insert_dataframe(rows, column_names)
    if data is not None:
        return data
    return build_bulk_insert_arrow_table(rows, column_names)
