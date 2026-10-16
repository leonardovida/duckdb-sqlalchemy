import datetime
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, List

import duckdb
import pytest
from sqlalchemy import Column as SAColumn
from sqlalchemy import (
    DateTime,
    Float,
    Integer,
    Interval,
    MetaData,
    String,
    Table,
    Time,
    create_engine,
    insert,
    inspect,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from duckdb_sqlalchemy import checkpoint
from duckdb_sqlalchemy._bulk_insert import build_bulk_insert_data
from duckdb_sqlalchemy.bulk import copy_from_csv, copy_from_rows
from duckdb_sqlalchemy.datatypes import Map, Struct, TimeNs, TimestampNs, VarInt
from duckdb_sqlalchemy.datatypes import Union as DuckUnion

pytest.importorskip("pyarrow")

PLUS_FIVE = datetime.timezone(datetime.timedelta(hours=5))


@pytest.fixture
def engine() -> Engine:
    engine = create_engine("duckdb:///:memory:")
    yield engine
    engine.dispose()


def _insert_both_ways(engine: Engine, column: SAColumn, values: List[Any]) -> Any:
    """Rows stored by the ordinary path and by a forced register path."""
    stored = []
    for threshold in (0, 1):
        metadata = MetaData()
        table = Table(
            f"t{threshold}",
            metadata,
            SAColumn("id", Integer),
            SAColumn(column.name, column.type),
        )
        with engine.begin() as conn:
            metadata.create_all(conn)
            conn.execution_options(duckdb_copy_threshold=threshold).execute(
                insert(table),
                [{"id": i, column.name: value} for i, value in enumerate(values)],
            )
            stored.append(
                conn.execute(
                    text(
                        f"SELECT CAST({column.name} AS VARCHAR) FROM t{threshold} ORDER BY id"
                    )
                ).fetchall()
            )
    return stored


@pytest.mark.parametrize(
    ("column", "values"),
    [
        (SAColumn("v", Time(timezone=True)), [datetime.time(10, tzinfo=PLUS_FIVE)] * 2),
        (
            SAColumn("v", Interval()),
            [datetime.timedelta(days=1), datetime.timedelta(hours=1)],
        ),
        (
            SAColumn("v", DateTime(timezone=True)),
            [
                datetime.datetime(2024, 1, 1, 10, tzinfo=PLUS_FIVE),
                datetime.datetime(2024, 1, 1, 10),
            ],
        ),
        (SAColumn("v", String), [1, 2.5]),
        (SAColumn("v", Float), [Decimal("1" * 40), Decimal(1)]),
        (SAColumn("v", Struct({"a": Integer})), [{"A": 1}, {"a": 2}]),
        (SAColumn("v", DuckUnion({"n": Integer, "s": String})), [1, 2]),
        (
            SAColumn("v", ARRAY(Interval())),
            [[datetime.timedelta(days=1)], [datetime.timedelta(days=2)]],
        ),
    ],
)
def test_register_path_stores_the_same_values(
    engine: Engine, column: SAColumn, values: List[Any]
) -> None:
    ordinary, registered = _insert_both_ways(engine, column, values)
    assert registered == ordinary


def test_register_path_declines_values_arrow_cannot_carry() -> None:
    columns = ["v"]
    assert (
        build_bulk_insert_data([(datetime.time(1, tzinfo=PLUS_FIVE),)], columns) is None
    )
    assert build_bulk_insert_data([(datetime.timedelta(days=1),)], columns) is None
    assert build_bulk_insert_data([(2**127, uuid.uuid4())], ["v", "u"]) is None
    assert build_bulk_insert_data([(Decimal("NaN"),)], columns) is None
    wide = [(Decimal("123456789.5"),), (Decimal("0." + "1" * 30),)]
    assert build_bulk_insert_data(wide, columns) is None
    assert build_bulk_insert_data([({"a": 1},), ({"A": 2},)], columns) is None
    assert build_bulk_insert_data([(1,), (2.5,)], columns, numeric_columns=()) is None


def test_register_path_keeps_mixed_numbers_for_numeric_columns() -> None:
    rows = [(1,), (2.5,), (None,)]
    assert build_bulk_insert_data(rows, ["v"], numeric_columns={"v"}) is not None
    assert build_bulk_insert_data([(datetime.time(1),), (None,)], ["v"]) is not None
    assert build_bulk_insert_data([(Decimal("1.5"),)], ["v"]) is not None


@pytest.mark.parametrize(
    "value",
    [["a", None], {"a": 1, "b": None}, (1, 2), datetime.timedelta(days=1)],
)
def test_copy_from_rows_rejects_values_csv_cannot_represent(value: Any) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t(v VARCHAR)")

    with pytest.raises(TypeError, match="nested or interval"):
        copy_from_rows(conn, "t", [(value,)], columns=["v"])


def test_copy_from_csv_quotes_column_names(engine: Engine, tmp_path: Path) -> None:
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("1\n2\n")
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE t("b c" INTEGER, other INTEGER)'))
        copy_from_csv(conn, "t", csv_path, columns=["b c"])
        assert conn.execute(text('SELECT "b c" FROM t ORDER BY 1')).fetchall() == [
            (1,),
            (2,),
        ]


def test_map_with_list_keys_keeps_duckdb_key_value_shape(engine: Engine) -> None:
    metadata = MetaData()
    table = Table("m", metadata, SAColumn("v", Map(ARRAY(Integer), Integer)))
    with engine.begin() as conn:
        metadata.create_all(conn)
        conn.execute(text("INSERT INTO m VALUES (MAP([[1], [2]], [10, 20]))"))
        assert conn.execute(select(table.c.v)).scalar_one() == {
            "key": [[1], [2]],
            "value": [10, 20],
        }


def test_reflected_bignum_and_nanosecond_types_recreate_exactly(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE src(b VARINT, ts TIMESTAMP_NS, t TIME_NS)"))
        columns = {c["name"]: c["type"] for c in inspect(conn).get_columns("src")}
        assert isinstance(columns["b"], VarInt)
        assert isinstance(columns["ts"], TimestampNs)
        assert isinstance(columns["t"], TimeNs)

        reflected = Table("src", MetaData(), autoload_with=conn)
        copy = reflected.to_metadata(MetaData(), name="copy")
        copy.create(conn)
        types = conn.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'copy' ORDER BY ordinal_position"
            )
        ).fetchall()
        source = conn.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'src' ORDER BY ordinal_position"
            )
        ).fetchall()
        assert types == source


def test_checkpoint_accepts_a_session(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t(i INTEGER)"))
    with Session(engine) as session:
        session.execute(text("INSERT INTO t VALUES (1)"))
        checkpoint(session)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT i FROM t")).fetchall() == [(1,)]
