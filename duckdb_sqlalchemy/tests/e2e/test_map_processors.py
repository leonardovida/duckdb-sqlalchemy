"""MAP member types convert both stored values and typed query results."""

from decimal import Decimal
from enum import Enum as PythonEnum
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import (
    ARRAY,
    JSON,
    Column,
    Enum,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    select,
)
from sqlalchemy.types import TypeDecorator

from ...datatypes import Map
from .conftest import EngineFactory


class PrefixedString(TypeDecorator):
    impl = String
    cache_ok = True

    def __init__(self, prefix: str):
        super().__init__()
        self.prefix = prefix

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        return None if value is None else self.prefix + value

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        return None if value is None else value.removeprefix(self.prefix)


class Label(PythonEnum):
    FIRST = "first"
    SECOND = "second"


@pytest.mark.parametrize("threshold", [0, 1])
def test_map_member_decorators_store_and_read_converted_values(
    db_path: Path, make_engine: EngineFactory, threshold: int
) -> None:
    engine = make_engine(db_path)
    table = Table(
        "maps",
        MetaData(),
        Column("id", Integer),
        Column("payload", Map(PrefixedString("key:"), PrefixedString("value:"))),
    )
    values = [{"a": "one", "b": None}, {}, None]
    with engine.begin() as conn:
        table.create(conn)
        conn.execution_options(duckdb_copy_threshold=threshold).execute(
            table.insert(), [{"id": i, "payload": v} for i, v in enumerate(values)]
        )
        assert conn.exec_driver_sql(
            "SELECT payload FROM maps ORDER BY id"
        ).scalars().all() == [
            {"key:a": "value:one", "key:b": None},
            {},
            None,
        ]
        conn.exec_driver_sql(
            "INSERT INTO maps VALUES (3, map(['key:c'], ['value:two']))"
        )
    engine.dispose()
    with make_engine(db_path).connect() as conn:
        assert conn.execute(
            select(table.c.payload).order_by(table.c.id)
        ).scalars().all() == [
            *values,
            {"c": "two"},
        ]


@pytest.mark.parametrize("threshold", [0, 1])
def test_map_enum_members_and_nested_maps_round_trip(
    db_path: Path, make_engine: EngineFactory, threshold: int
) -> None:
    engine = make_engine(db_path)
    enum_type = Enum(Label, native_enum=False)
    table = Table(
        "nested_maps",
        MetaData(),
        Column("id", Integer),
        Column("payload", Map(enum_type, Map(String, ARRAY(enum_type)))),
    )
    values = [{Label.FIRST: {"labels": [Label.SECOND, None]}}, {}, None]
    with engine.begin() as conn:
        table.create(conn)
        conn.execution_options(duckdb_copy_threshold=threshold).execute(
            table.insert(), [{"id": i, "payload": v} for i, v in enumerate(values)]
        )
        assert conn.exec_driver_sql(
            "SELECT payload FROM nested_maps ORDER BY id"
        ).scalars().all() == [
            {"FIRST": {"labels": ["SECOND", None]}},
            {},
            None,
        ]
    engine.dispose()
    with make_engine(db_path).connect() as conn:
        assert (
            conn.execute(select(table.c.payload).order_by(table.c.id)).scalars().all()
            == values
        )


def test_map_result_processors_read_existing_sql_values(
    db_path: Path,
    make_engine: EngineFactory,
) -> None:
    engine = make_engine(db_path)
    table = Table(
        "existing_maps",
        MetaData(),
        Column("labels", Map(Enum(Label, native_enum=False), PrefixedString("value:"))),
        Column("documents", Map(String, JSON)),
        Column("numbers", Map(String, Numeric(38, 2, asdecimal=False))),
    )
    with engine.begin() as conn:
        table.create(conn)
        conn.exec_driver_sql(
            "INSERT INTO existing_maps VALUES "
            "(map(['FIRST'], ['value:one']), "
            "map(['doc'], ['{\"nested\": [1, null]}']), map(['n'], [1.25]))"
        )
        row = conn.execute(select(table)).one()
        assert row.labels == {Label.FIRST: "one"}
        assert row.documents == {"doc": {"nested": [1, None]}}
        assert row.numbers == {"n": 1.25}
        assert isinstance(row.numbers["n"], float)


def test_map_uses_dialect_numeric_bind_processor(
    db_path: Path,
    make_engine: EngineFactory,
) -> None:
    engine = make_engine(db_path)
    table = Table("decimal_maps", MetaData(), Column("v", Map(String, Numeric(38, 2))))
    value = {
        "positive_exponent": Decimal("1E+5"),
        "precise": Decimal("9007199254740993.01"),
    }
    with engine.begin() as conn:
        table.create(conn)
        conn.execute(table.insert(), {"v": value})
        assert conn.exec_driver_sql("SELECT v FROM decimal_maps").scalar_one() == value
        assert conn.execute(select(table.c.v)).scalar_one() == value
