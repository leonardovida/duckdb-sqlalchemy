import os
from decimal import Decimal
from functools import lru_cache
from typing import Dict, Set, Type, Union

import duckdb
from sqlalchemy import Boolean, Float, Integer, String
from sqlalchemy.engine import Dialect
from sqlalchemy.sql.type_api import TypeEngine

TYPES: Dict[Type, TypeEngine] = {
    bool: Boolean(),
    int: Integer(),
    float: Float(),
    str: String(),
}


@lru_cache()
def get_core_config() -> Set[str]:
    # List of connection string parameters that are supported by MotherDuck
    # See: https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/
    motherduck_config_keys = {
        "motherduck_token",
        "attach_mode",
        "saas_mode",
        "session_hint",
        "access_mode",
        "dbinstance_inactivity_ttl",
        "motherduck_dbinstance_inactivity_ttl",
    }

    with duckdb.connect(":memory:") as conn:
        rows = conn.execute("SELECT name FROM duckdb_settings()").fetchall()
    return {name for (name,) in rows} | motherduck_config_keys


def apply_config(
    dialect: Dialect,
    conn: duckdb.DuckDBPyConnection,
    ext: Dict[str, Union[str, int, bool, float, None]],
) -> None:
    # TODO: does sqlalchemy have something that could do this for us?
    processors = [
        (typ, type_engine.literal_processor(dialect=dialect))
        for typ, type_engine in TYPES.items()
    ]
    string_processor = String().literal_processor(dialect=dialect)

    for k, v in ext.items():
        if v is None:
            conn.execute(f"SET {k} = NULL")
            continue
        if isinstance(v, os.PathLike):
            v = os.fspath(v)
            process = string_processor
        elif isinstance(v, Decimal):
            v = str(v)
            process = string_processor
        else:
            process = None
            for typ, processor in processors:
                if isinstance(v, typ):
                    process = processor
                    break
            if process is None:
                v = str(v)
                process = string_processor
        assert process, f"Not able to configure {k} with {v}"
        conn.execute(f"SET {k} = {process(v)}")
