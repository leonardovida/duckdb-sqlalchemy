from __future__ import annotations

import importlib.util
import math
from functools import lru_cache
from numbers import Integral, Real
from typing import Any, Optional, Sequence, cast

from ._row_shape import infer_mapping_column_keys, rows_use_mapping_shape


def infer_bulk_insert_column_keys(rows: Sequence[Any]) -> Optional[list[str]]:
    return infer_mapping_column_keys(rows)


@lru_cache()
def has_bulk_insert_data_library() -> bool:
    return any(
        importlib.util.find_spec(module) is not None for module in ("pyarrow", "pandas")
    )


def _is_integer(value: Any) -> bool:
    return isinstance(value, Integral) and not isinstance(value, bool)


def _has_unsafe_numeric_coercion(
    rows: Sequence[Any], column_names: Sequence[str]
) -> bool:
    mapping_rows = rows_use_mapping_shape(rows)
    for index, name in enumerate(column_names):
        has_float = False
        has_large_integer = False
        for row in rows:
            value = row.get(name) if mapping_rows else row[index]
            if value is None or isinstance(value, (str, bytes, bool)):
                continue
            if isinstance(value, int):
                has_large_integer = has_large_integer or abs(value) > 2**53
            elif isinstance(value, float):
                has_float = True
            elif isinstance(value, Integral):
                has_large_integer = has_large_integer or abs(value) > 2**53
            elif isinstance(value, Real):
                has_float = True
            if has_float and has_large_integer:
                return True
    return False


def _restore_nullable_integers(
    pd: Any, frame: Any, rows: Sequence[Any], column_names: Sequence[str]
) -> None:
    # pandas stores an integer column that contains None as float64, which
    # silently rounds integers above 2**53; use the nullable Int64 dtype.
    mapping_rows = rows_use_mapping_shape(rows)
    for index, name in enumerate(column_names):
        if frame[name].dtype.kind != "f":
            continue
        values = [row.get(name) if mapping_rows else row[index] for row in rows]
        if any(value is not None for value in values) and all(
            value is None or _is_integer(value) for value in values
        ):
            frame[name] = pd.array(values, dtype="Int64")


def build_bulk_insert_dataframe(
    rows: Sequence[Any], column_names: Sequence[str]
) -> Any:
    try:
        import pandas as pd  # type: ignore[import-not-found]
    except Exception:
        return None

    try:
        if _has_unsafe_numeric_coercion(rows, column_names):
            return None
        mapping_rows = rows_use_mapping_shape(rows)
        for index, name in enumerate(column_names):
            values = [row.get(name) if mapping_rows else row[index] for row in rows]
            # DuckDB's pandas scan treats NaN as NULL. Ordinary bindings
            # preserve NaN, so this batch must keep the ordinary insert path.
            if any(isinstance(value, Real) and not isinstance(value, Integral) and math.isnan(value) for value in values):
                return None
            kinds = {type(value) for value in values if value is not None}
            if len(kinds) > 1 and not all(
                value is None
                or (isinstance(value, Real) and not isinstance(value, bool))
                for value in values
            ):
                return None
        if mapping_rows:
            frame = pd.DataFrame.from_records(rows, columns=column_names)
        else:
            frame = pd.DataFrame(rows, columns=cast(Any, column_names))
        _restore_nullable_integers(pd, frame, rows, column_names)
        return frame
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
        if _has_unsafe_numeric_coercion(rows, column_names):
            return None
        if rows_use_mapping_shape(rows):
            table = pa.Table.from_pylist(rows)
            if column_names:
                return table.select(column_names)
            return table
        # Build each column separately instead of retaining a transposed copy
        # of every cell alongside the Arrow arrays.
        arrays = [
            pa.array([row[index] for row in rows]) for index in range(len(column_names))
        ]
        return pa.Table.from_arrays(arrays, names=column_names)
    except Exception:
        return None


def build_bulk_insert_data(rows: Sequence[Any], column_names: Sequence[str]) -> Any:
    # Arrow keeps nullable integer columns as int64; pandas is the fallback for
    # values Arrow cannot infer (UUID objects, integers beyond int64, ...).
    data = build_bulk_insert_arrow_table(rows, column_names)
    if data is not None:
        return data
    return build_bulk_insert_dataframe(rows, column_names)
