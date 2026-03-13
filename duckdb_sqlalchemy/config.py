import os
from decimal import Decimal
from functools import lru_cache
from typing import Any, Dict, Set, Type, Union

import duckdb
from sqlalchemy import Boolean, Float, Integer, String
from sqlalchemy.engine import Dialect
from sqlalchemy.sql.type_api import TypeEngine

from ._validation import validate_identifier
from .motherduck import MOTHERDUCK_CONFIG_KEYS

TYPES: Dict[Type, TypeEngine] = {
    bool: Boolean(),
    int: Integer(),
    float: Float(),
    str: String(),
}

ConfigValue = Union[str, int, bool, float, os.PathLike[Any], Decimal, None]


@lru_cache()
def get_core_config() -> Set[str]:
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute("SELECT name FROM duckdb_settings()").fetchall()
    return {name for (name,) in rows} | MOTHERDUCK_CONFIG_KEYS


def apply_config(
    dialect: Dialect,
    conn: Any,
    ext: Dict[str, ConfigValue],
) -> None:
    processors = _build_literal_processors(dialect)
    string_processor = String().literal_processor(dialect=dialect)
    for k, v in ext.items():
        key = validate_identifier(k, kind="config key")
        value_sql = _render_config_value(v, processors, string_processor)
        conn.execute(f"SET {key} = {value_sql}")


def _build_literal_processors(dialect: Dialect) -> Dict[type, Any]:
    # TODO: does sqlalchemy have something that could do this for us?
    return {
        typ: type_engine.literal_processor(dialect=dialect)
        for typ, type_engine in TYPES.items()
    }


def _render_config_value(
    value: ConfigValue,
    processors: Dict[type, Any],
    string_processor: Any,
) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, os.PathLike):
        return string_processor(os.fspath(value))
    if isinstance(value, Decimal):
        return string_processor(str(value))

    process = None
    for typ, processor in processors.items():
        if isinstance(value, typ):
            process = processor
            break
    if process is None:
        return string_processor(str(value))
    return process(value)
