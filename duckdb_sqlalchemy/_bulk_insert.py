from __future__ import annotations

import datetime
import importlib.util
import math
from collections.abc import Mapping
from decimal import Decimal
from functools import lru_cache
from numbers import Integral, Real
from typing import Any, Collection, Iterable, Optional, Sequence, cast

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
    rows: Sequence[Any],
    column_names: Sequence[str],
    floating_columns: Optional[Mapping[int, int]] = None,
) -> bool:
    mapping_rows = rows_use_mapping_shape(rows)
    for index, name in enumerate(column_names):
        if floating_columns is not None and index not in floating_columns:
            continue
        limit = 2**53 if floating_columns is None else floating_columns[index]
        has_float = floating_columns is not None
        has_large_integer = False
        for row in rows:
            value = row.get(name) if mapping_rows else row[index]
            if value is None or isinstance(value, (str, bytes, bool)):
                continue
            if isinstance(value, int):
                has_large_integer = has_large_integer or abs(value) > limit
            elif isinstance(value, float):
                has_float = True
            elif isinstance(value, Integral):
                has_large_integer = has_large_integer or abs(int(value)) > limit
            elif isinstance(value, Real):
                has_float = True
            if has_float and has_large_integer:
                return True
    return False


def _has_unsafe_nested_numeric_coercion(values: Iterable[Any]) -> bool:
    """Track precision per nested field (list elements share one field)."""
    integers: dict[tuple[Any, ...], int] = {}
    limits: dict[tuple[Any, ...], int] = {}

    def visit(value: Any, path: tuple[Any, ...]) -> bool:
        if isinstance(value, Mapping):
            return any(visit(item, (*path, key)) for key, item in value.items())
        if isinstance(value, (list, tuple)) or (
            not isinstance(value, (str, bytes, bytearray, memoryview))
            and getattr(value, "ndim", 0) > 0
        ):
            return any(visit(item, (*path, None)) for item in value)
        if isinstance(value, bool):
            return False
        if isinstance(value, Integral):
            integers[path] = max(integers.get(path, 0), abs(int(value)))
        elif isinstance(value, Real):
            width = getattr(getattr(value, "dtype", None), "itemsize", 8)
            limit = 2 ** (11 if width == 2 else 24 if width == 4 else 53)
            limits[path] = min(limits.get(path, limit), limit)
        return path in limits and integers.get(path, 0) > limits[path]

    return any(visit(value, ()) for value in values)


# Arrow and pandas cannot carry these exactly: DECIMAL is at most 38 digits
# and integers beyond HUGEINT become BIGINT.
_MAX_DECIMAL_DIGITS = 38
_MAX_INTEGER = 2**127


def _decimal_shape(value: Decimal) -> Optional[tuple[int, int]]:
    """(integer digits, scale) of ``value``, or None for NaN and infinity."""
    _, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        return None
    return max(len(digits) + exponent, 0), max(-exponent, 0)


def _column_changes_values_on_register(
    index: int,
    name: str,
    values: Sequence[Any],
    numeric_columns: Optional[Collection[str]],
) -> bool:
    """Whether registering one column's ``values`` would store different values."""
    aware: dict[tuple[Any, ...], bool] = {}
    # Whether a field holds datetimes (True) or plain dates (False). Arrow
    # infers a field with both as date32 and drops the time of day.
    has_time: dict[tuple[Any, ...], bool] = {}
    struct_keys: dict[tuple[Any, ...], dict[str, str]] = {}
    # One inferred DECIMAL type holds the widest integer part and scale.
    decimals: dict[tuple[Any, ...], tuple[int, int]] = {}
    # Number kinds per nested field of a non-numeric column. Arrow infers one
    # type per field, so an int or bool next to a float in a text field is
    # stored as "1.0", as at the top level.
    number_kinds: dict[tuple[Any, ...], set[str]] = {}

    def mixes_numbers(path: tuple[Any, ...], kind: str) -> bool:
        kinds = number_kinds.setdefault(path, set())
        kinds.add(kind)
        return len(kinds) > 1

    def visit(value: Any, path: tuple[Any, ...], preserve_decimal_text: bool) -> bool:
        if value is None or isinstance(value, (str, bytes)):
            return False
        nested_text = preserve_decimal_text and len(path) > 1
        if isinstance(value, bool) or _is_integer(value):
            return nested_text and mixes_numbers(path, "int")
        if isinstance(value, Real):
            return nested_text and mixes_numbers(path, "float")
        if isinstance(value, datetime.datetime):
            if has_time.setdefault(path, True) is not True:
                return True
            has_tz = value.tzinfo is not None
            return aware.setdefault(path, has_tz) is not has_tz
        if isinstance(value, datetime.date):
            return has_time.setdefault(path, False) is not False
        if isinstance(value, datetime.time):
            return value.tzinfo is not None
        if isinstance(value, datetime.timedelta):
            return True
        if isinstance(value, Decimal):
            # Registration infers one scale per field, including nested
            # fields. A subsequent cast to text pads individually bound
            # values (1.2 becomes "1.200", or an integer 1 becomes "1.00").
            if preserve_decimal_text:
                return True
            shape = _decimal_shape(value)
            if shape is None:
                return True
            integer_digits, scale = decimals.get(path, (0, 0))
            integer_digits = max(integer_digits, shape[0])
            scale = max(scale, shape[1])
            decimals[path] = (integer_digits, scale)
            return integer_digits + scale > _MAX_DECIMAL_DIGITS
        if isinstance(value, Mapping):
            keys = struct_keys.setdefault(path, {})
            for key, item in value.items():
                if isinstance(key, str):
                    if keys.setdefault(key.lower(), key) != key:
                        return True
                if visit(item, (*path, key), preserve_decimal_text):
                    return True
            return False
        dtype = getattr(value, "dtype", None)
        if dtype is not None and getattr(value, "ndim", 0) > 0:
            if dtype.kind == "m":
                return True
            if dtype.kind != "O":
                if preserve_decimal_text and dtype.kind in "biuf" and value.size:
                    leaf = (*path, *(None,) * value.ndim)
                    return mixes_numbers(leaf, "float" if dtype.kind == "f" else "int")
                return False
        elif dtype is not None:
            return dtype.kind == "m"
        if isinstance(value, (list, tuple)) or getattr(value, "ndim", 0) > 0:
            return any(
                visit(item, (*path, None), preserve_decimal_text) for item in value
            )
        return False

    kinds = set(map(type, values))
    # A column of ints and floats infers as DOUBLE, so a text column
    # would store "1.0" where the ordinary path stores "1". Arrow also
    # turns a bool next to a float into 1.0, where the ordinary path
    # stores "true".
    if (
        numeric_columns is not None
        and name not in numeric_columns
        and any(_is_float_kind(kind) for kind in kinds)
        and any(_is_integer_kind(kind) or kind is bool for kind in kinds)
    ):
        return True
    if kinds <= _PLAIN_KINDS:
        # Arrow rejects integers beyond int64, and the DataFrame builder
        # checks the HUGEINT range itself.
        return False
    if kinds <= _NAIVE_OR_AWARE_KINDS:
        zones = {value.tzinfo is None for value in values if value is not None}
        return len(zones) > 1
    path = (index,)
    preserve_decimal_text = numeric_columns is not None and name not in numeric_columns
    return any(visit(value, path, preserve_decimal_text) for value in values)


def _changes_values_on_register(
    rows: Sequence[Any],
    column_names: Sequence[str],
    numeric_columns: Optional[Collection[str]],
) -> bool:
    """Whether registering ``rows`` would store different values.

    The ordinary insert path binds each Python value on its own. A registered
    Arrow table or DataFrame infers one type per column (or nested field),
    which drops TIME offsets, turns intervals into microseconds, shifts naive
    datetimes mixed with aware ones, merges STRUCT keys that differ only in
    case, renders integers mixed with floats as floats and truncates
    datetimes mixed with dates to midnight.
    """
    mapping_rows = rows_use_mapping_shape(rows)
    for index, name in enumerate(column_names):
        if mapping_rows:
            values = [row.get(name) for row in rows]
        else:
            values = [row[index] for row in rows]
        if _column_changes_values_on_register(index, name, values, numeric_columns):
            return True
    return False


_PLAIN_KINDS = frozenset({type(None), str, bool, bytes, float, int, datetime.date})
_NAIVE_OR_AWARE_KINDS = frozenset({type(None), datetime.datetime})


def _is_integer_kind(kind: type) -> bool:
    return issubclass(kind, Integral) and not issubclass(kind, bool)


def _is_float_kind(kind: type) -> bool:
    return issubclass(kind, Real) and not issubclass(kind, Integral)


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
            if any(
                isinstance(value, (Mapping, list, tuple))
                or getattr(value, "ndim", 0) > 0
                for value in values
            ) and _has_unsafe_nested_numeric_coercion(values):
                return None
            # DuckDB's pandas scan treats NaN as NULL. Ordinary bindings
            # preserve NaN, so this batch must keep the ordinary insert path.
            if any(
                isinstance(value, Real)
                and not isinstance(value, Integral)
                and math.isnan(value)
                for value in values
            ):
                return None
            if any(
                _is_integer(value) and abs(value) >= _MAX_INTEGER for value in values
            ):
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
        if rows_use_mapping_shape(rows):
            table = pa.Table.from_pylist(rows)
            if column_names:
                table = table.select(column_names)
        else:
            # Do not retain a transposed copy of all cells.
            arrays = [
                pa.array([row[index] for row in rows])
                for index in range(len(column_names))
            ]
            table = pa.Table.from_arrays(arrays, names=column_names)
        floating_columns = {
            index: 2
            ** (
                11
                if pa.types.is_float16(field.type)
                else 24
                if pa.types.is_float32(field.type)
                else 53
            )
            for index, field in enumerate(table.schema)
            if pa.types.is_floating(field.type)
        }
        mapping_rows = rows_use_mapping_shape(rows)
        for index, field in enumerate(table.schema):
            if (
                pa.types.is_nested(field.type)
                and _has_floating_field(field.type)
                and _has_unsafe_nested_numeric_coercion(
                    row.get(column_names[index]) if mapping_rows else row[index]
                    for row in rows
                )
            ):
                return None
        # Only floating inference can round an integer. Inspect its original
        # values before accepting it, and leave safe int/string columns alone.
        if floating_columns and _has_unsafe_numeric_coercion(
            rows, column_names, floating_columns
        ):
            return None
        return table
    except Exception:
        return None


def _has_floating_field(arrow_type: Any) -> bool:
    """Whether a nested Arrow type has a floating field at any depth."""
    import pyarrow as pa  # type: ignore[import-not-found]

    if pa.types.is_floating(arrow_type):
        return True
    return any(
        _has_floating_field(arrow_type.field(index).type)
        for index in range(arrow_type.num_fields)
    )


def _has_unsafe_floating_integer(values: Sequence[Any], limit: int) -> bool:
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            if abs(value) > limit:
                return True
        elif isinstance(value, Integral):
            if abs(int(value)) > limit:
                return True
    return False


def build_bulk_insert_data(
    rows: Sequence[Any],
    column_names: Sequence[str],
    numeric_columns: Optional[Collection[str]] = None,
) -> Any:
    """Arrow table or DataFrame for ``rows``, or None to use the ordinary path.

    ``numeric_columns`` names the target columns with a numeric type. When it
    is given, other columns decline values that mix integers and floats.
    """
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
    except Exception:
        pa = None

    if pa is None or not column_names:
        if _changes_values_on_register(rows, column_names, numeric_columns):
            return None
        if pa is not None:
            data = build_bulk_insert_arrow_table(rows, column_names)
            if data is not None:
                return data
        return build_bulk_insert_dataframe(rows, column_names)

    mapping_rows = rows_use_mapping_shape(rows)
    arrays: list[Any] = []
    arrow_ok = True
    # Extract each column once to validate register safety and build the Arrow
    # array together, avoiding duplicate passes over ``rows``.
    for index, name in enumerate(column_names):
        if mapping_rows:
            values = [row.get(name) for row in rows]
        else:
            values = [row[index] for row in rows]
        if _column_changes_values_on_register(index, name, values, numeric_columns):
            return None
        if not arrow_ok:
            continue
        try:
            array = pa.array(values)
        except Exception:
            arrow_ok = False
            arrays.clear()
            continue
        if pa.types.is_floating(array.type):
            limit = 2 ** (
                11
                if pa.types.is_float16(array.type)
                else 24
                if pa.types.is_float32(array.type)
                else 53
            )
            if _has_unsafe_floating_integer(values, limit):
                arrow_ok = False
                arrays.clear()
                continue
        elif pa.types.is_nested(array.type) and _has_floating_field(array.type):
            # Only floating inference can round an integer.
            if _has_unsafe_nested_numeric_coercion(values):
                arrow_ok = False
                arrays.clear()
                continue
        arrays.append(array)

    if arrow_ok:
        try:
            return pa.Table.from_arrays(arrays, names=list(column_names))
        except Exception:
            pass
    # Arrow keeps nullable integer columns as int64; pandas is the fallback for
    # values Arrow cannot infer (UUID objects, integers beyond int64, ...).
    return build_bulk_insert_dataframe(rows, column_names)


def pandas_rows_to_analyze(data: Any) -> Optional[int]:
    """Rows DuckDB must analyze to type ``data``'s object columns exactly.

    DuckDB types a pandas object column from a sample of
    ``pandas_analyze_sample`` rows and casts every other row to that type,
    which rounds a Decimal with a wider scale and rejects an integer or STRUCT
    key the sample did not see. None for Arrow data and for DataFrames
    without object columns.
    """
    dtypes = getattr(data, "dtypes", None)
    if dtypes is None or not any(dtype.kind == "O" for dtype in dtypes):
        return None
    return len(data)
