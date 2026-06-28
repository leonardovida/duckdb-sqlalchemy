from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from itertools import chain
from typing import Any, Optional, Tuple, Union, cast


def rows_use_mapping_shape(rows: Sequence[Any]) -> bool:
    return bool(rows) and isinstance(rows[0], Mapping)


def infer_mapping_column_keys(rows: Sequence[Any]) -> Optional[list[str]]:
    if not rows_use_mapping_shape(rows):
        return None
    return [str(key) for key in cast(Mapping[str, Any], rows[0]).keys()]


def mapping_row_as_sequence(
    row: Mapping[str, Any], columns: Sequence[str]
) -> Sequence[Any]:
    return [row.get(col) for col in columns]


def rows_as_sequences(
    first: Union[Mapping[str, Any], Sequence[Any]],
    rows: Iterable[Union[Mapping[str, Any], Sequence[Any]]],
    columns: Optional[Sequence[str]],
) -> Tuple[Iterable[Sequence[Any]], Optional[Sequence[str]]]:
    if isinstance(first, Mapping):
        if columns is None:
            columns = [str(col) for col in cast(Mapping[str, Any], first).keys()]

        first_row = mapping_row_as_sequence(cast(Mapping[str, Any], first), columns)
        remaining_rows = (
            mapping_row_as_sequence(cast(Mapping[str, Any], row), columns)
            for row in rows
        )
        return chain((first_row,), remaining_rows), columns

    first_row = cast(Sequence[Any], first)
    remaining_rows = (cast(Sequence[Any], row) for row in rows)
    return chain((first_row,), remaining_rows), columns
