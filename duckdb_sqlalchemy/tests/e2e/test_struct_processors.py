"""STRUCT field types convert stored values and typed query results."""

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

from ...datatypes import FixedArray, Map, Struct
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


@pytest.fixture(params=["ordinary", "arrow", "pandas"])
def threshold(request: Any, monkeypatch: pytest.MonkeyPatch) -> int:
    if request.param == "ordinary":
        return 0
    pytest.importorskip("pyarrow" if request.param == "arrow" else "pandas")
    if request.param == "pandas":
        monkeypatch.setattr(
            "duckdb_sqlalchemy._bulk_insert.build_bulk_insert_arrow_table",
            lambda *args: None,
        )
    return 1


def test_struct_decorators_store_and_read_converted_fields(
    db_path: Path, make_engine: EngineFactory, threshold: int
) -> None:
    engine = make_engine(db_path)
    table = Table(
        "records",
        MetaData(),
        Column("id", Integer),
        Column(
            "v",
            Struct(
                {
                    "Label": PrefixedString("label:"),
                    "nullable": PrefixedString("null:"),
                    "Ä": PrefixedString("upper:"),
                    "ä": PrefixedString("lower:"),
                }
            ),
        ),
    )
    value = {"label": "one", "nullable": None, "Ä": "a", "ä": "b"}
    expected = {**value, "Label": value["label"]}
    del expected["label"]
    rows = [{"id": 0, "v": value}, {"id": 1, "v": None}]
    with engine.begin() as conn:
        table.create(conn)
        conn.execution_options(duckdb_copy_threshold=threshold).execute(
            table.insert(), rows
        )
        assert value == {"label": "one", "nullable": None, "Ä": "a", "ä": "b"}
        assert conn.exec_driver_sql("SELECT v FROM records ORDER BY id").all() == [
            ({"Label": "label:one", "nullable": None, "Ä": "upper:a", "ä": "lower:b"},),
            (None,),
        ]
        conn.exec_driver_sql(
            "INSERT INTO records VALUES (2, "
            "{'Label': 'label:two', 'nullable': NULL, 'Ä': 'upper:c', 'ä': 'lower:d'})"
        )
    engine.dispose()
    with make_engine(db_path).connect() as conn:
        assert conn.execute(select(table.c.v).order_by(table.c.id)).scalars().all() == [
            expected,
            None,
            {"Label": "two", "nullable": None, "Ä": "c", "ä": "d"},
        ]


@pytest.mark.parametrize("threshold", [0, 1])
def test_struct_processors_recurse_through_nested_members(
    db_path: Path, make_engine: EngineFactory, threshold: int
) -> None:
    record = Struct({"label": Enum(Label, native_enum=False), "doc": JSON})
    table = Table(
        "nested_records",
        MetaData(),
        Column("id", Integer),
        Column("v", Struct({"record": record})),
        Column("many", ARRAY(record)),
        Column("fixed", FixedArray(record, 2)),
        Column("mapped", Map(String, record)),
    )
    value = {"label": Label.SECOND, "doc": {"nested": [1, None]}}
    rows = [
        {
            "id": 0,
            "v": {"record": value},
            "many": [value, None],
            "fixed": [value, None],
            "mapped": {"a": value, "b": None},
        },
        {"id": 1, "v": None, "many": [], "fixed": None, "mapped": {}},
    ]
    engine = make_engine(db_path)
    with engine.begin() as conn:
        table.create(conn)
        conn.execution_options(duckdb_copy_threshold=threshold).execute(
            table.insert(), rows
        )
        assert (
            conn.exec_driver_sql(
                "SELECT v.record.label, many[1].label, fixed[1].label, mapped['a'].label "
                "FROM nested_records WHERE id = 0"
            ).one()
            == ("SECOND",) * 4
        )
    engine.dispose()
    with make_engine(db_path).connect() as conn:
        assert [
            dict(row)
            for row in conn.execute(select(table).order_by(table.c.id)).mappings()
        ] == rows


def test_struct_result_processors_read_existing_sql_values(
    db_path: Path, make_engine: EngineFactory
) -> None:
    table = Table(
        "existing_records",
        MetaData(),
        Column(
            "v",
            Struct(
                {
                    "label": Enum(Label, native_enum=False),
                    "doc": JSON,
                    "n": Numeric(38, 2, asdecimal=False),
                }
            ),
        ),
    )
    with make_engine(db_path).begin() as conn:
        table.create(conn)
        conn.exec_driver_sql(
            "INSERT INTO existing_records VALUES "
            "({'label': 'FIRST', 'doc': '{\"nested\": [1, null]}', 'n': 1.25})"
        )
        value = conn.execute(select(table.c.v)).scalar_one()
        assert value == {"label": Label.FIRST, "doc": {"nested": [1, None]}, "n": 1.25}
        assert isinstance(value["n"], float)


def test_struct_uses_dialect_numeric_bind_processor(
    db_path: Path, make_engine: EngineFactory, threshold: int
) -> None:
    table = Table(
        "decimal_records",
        MetaData(),
        Column("v", Struct({"exponent": Numeric(38, 2), "precise": Numeric(38, 2)})),
    )
    value = {"exponent": Decimal("1E+5"), "precise": Decimal("9007199254740993.01")}
    with make_engine(db_path).begin() as conn:
        table.create(conn)
        conn.execution_options(duckdb_copy_threshold=threshold).execute(
            table.insert(), [{"v": value}, {"v": None}]
        )
        assert conn.exec_driver_sql("SELECT v FROM decimal_records").all() == [
            (value,),
            (None,),
        ]
        assert conn.execute(select(table.c.v)).scalars().all() == [value, None]
