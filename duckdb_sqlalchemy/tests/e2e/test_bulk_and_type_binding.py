"""Values written through bulk helpers and typed binds are stored as given."""

import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import (
    ARRAY,
    REAL,
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    select,
    text,
)

from duckdb_sqlalchemy import copy_from_rows, insert_from_arrow
from duckdb_sqlalchemy.datatypes import FixedArray, HugeInteger, Map, Struct, Union

from .conftest import EngineFactory

NOON = datetime.datetime(2024, 1, 1, 12, 30)


def test_bulk_fast_path_keeps_time_of_datetimes_mixed_with_dates(
    db_path: Path, make_engine: EngineFactory
) -> None:
    pytest.importorskip("pyarrow")
    metadata = MetaData()
    events = Table(
        "events",
        metadata,
        Column("at", DateTime),
        Column("ats", ARRAY(DateTime)),
        Column("info", Struct({"at": DateTime})),
    )
    engine = make_engine(db_path)
    # A date first makes Arrow infer date32 for the whole column.
    rows = [
        {"at": value, "ats": [value], "info": {"at": value}}
        for value in (datetime.date(2024, 1, 1), NOON, NOON)
    ]
    with engine.begin() as conn:
        metadata.create_all(conn)
        conn.execution_options(duckdb_copy_threshold=2).execute(events.insert(), rows)
        stored = conn.execute(select(events)).all()

    for at, ats, info in stored[1:]:
        assert at == NOON
        assert ats == [NOON]
        assert info == {"at": NOON}


@pytest.mark.parametrize(
    "escape, quote",
    [("\\", '"'), ("'", '"'), ("\\", "'")],
    ids=["backslash", "single-quote", "custom-quote"],
)
def test_copy_from_rows_round_trips_escape_and_quote_characters(
    escape: str, quote: str, db_path: Path, make_engine: EngineFactory
) -> None:
    values = ["C:\\temp", 'say "hi"', "it's", "trailing\\", "", None]
    engine = make_engine(db_path)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (id INTEGER, v VARCHAR)")
        copy_from_rows(
            conn,
            "t",
            list(enumerate(values)),
            columns=["id", "v"],
            escape=escape,
            quote=quote,
        )
        stored = [v for (v,) in conn.exec_driver_sql("SELECT v FROM t ORDER BY id")]

    assert stored == values


def test_copy_from_rows_stores_aware_datetimes_like_insert(
    db_path: Path, make_engine: EngineFactory
) -> None:
    aware = datetime.datetime(
        2024, 1, 1, 12, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=5))
    )
    engine = make_engine(db_path)
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TimeZone = 'America/New_York'")
        conn.exec_driver_sql("CREATE TABLE t (id INTEGER, a TIMESTAMP, b TIMESTAMPTZ)")
        conn.exec_driver_sql("INSERT INTO t VALUES (1, $1, $1)", (aware,))
        copy_from_rows(conn, "t", [(2, aware, aware)], columns=["id", "a", "b"])
        inserted, copied = conn.exec_driver_sql("SELECT a, b FROM t ORDER BY id").all()

    assert copied == inserted
    assert inserted[0] == datetime.datetime(2024, 1, 1, 2, 0)


def test_copy_from_rows_writes_pandas_missing_values_as_null(
    db_path: Path, make_engine: EngineFactory
) -> None:
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {
            "id": [1, 2],
            "name": pd.array(["a", pd.NA], dtype="string"),
            "seen": [pd.Timestamp("2024-01-01"), pd.NaT],
        }
    )
    engine = make_engine(db_path)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE t (id INTEGER, name VARCHAR, seen TIMESTAMP)"
        )
        copy_from_rows(conn, "t", frame.to_dict("records"))
        stored = conn.exec_driver_sql("SELECT * FROM t ORDER BY id").all()

    assert stored == [(1, "a", datetime.datetime(2024, 1, 1)), (2, None, None)]


def test_insert_from_arrow_maps_case_colliding_names_by_position(
    db_path: Path, make_engine: EngineFactory
) -> None:
    pa = pytest.importorskip("pyarrow")
    data = pa.table({"id": [1, 10], "ID": [2, 20]})
    engine = make_engine(db_path)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (a INTEGER, b INTEGER)")
        insert_from_arrow(conn, "t", data, columns=["a", "b"])
        stored = conn.exec_driver_sql("SELECT a, b FROM t ORDER BY a").all()

    assert stored == [(1, 2), (10, 20)]


def test_decimal_with_positive_exponent_binds_its_full_value(
    db_path: Path, make_engine: EngineFactory
) -> None:
    hundred = Decimal("100.00").normalize()
    assert hundred.as_tuple().exponent == 2
    metadata = MetaData()
    t = Table(
        "t",
        metadata,
        Column("i", Integer),
        Column("b", BigInteger),
        Column("h", HugeInteger),
        Column("u", Union({"n": Numeric(10, 2), "label": String})),
        Column("ids", ARRAY(Integer)),
    )
    engine = make_engine(db_path)
    with engine.begin() as conn:
        metadata.create_all(conn)
        conn.execute(
            t.insert(),
            {"i": hundred, "b": hundred, "h": hundred, "u": hundred, "ids": [hundred]},
        )
        row = conn.execute(select(t)).one()

    assert (row.i, row.b, row.h, row.u, row.ids) == (100, 100, 100, 100, [100])


def test_numpy_arrays_bind_to_fixed_and_variable_arrays(
    db_path: Path, make_engine: EngineFactory
) -> None:
    np = pytest.importorskip("numpy")
    metadata = MetaData()
    t = Table(
        "t",
        metadata,
        Column("embedding", FixedArray(REAL, 3)),
        Column("scores", ARRAY(REAL)),
        Column("ids", ARRAY(BigInteger)),
    )
    engine = make_engine(db_path)
    with engine.begin() as conn:
        metadata.create_all(conn)
        conn.execute(
            t.insert(),
            {
                "embedding": np.array([0.25, 0.5, 0.75], dtype=np.float32),
                "scores": np.array([1.5, 2.5], dtype=np.float32),
                "ids": np.array([1, 2], dtype=np.int64),
            },
        )
        row = conn.execute(select(t)).one()

    assert list(row.embedding) == [0.25, 0.5, 0.75]
    assert row.scores == [1.5, 2.5]
    assert row.ids == [1, 2]


def test_map_with_list_keys_round_trips_its_fetched_shape(
    db_path: Path, make_engine: EngineFactory
) -> None:
    metadata = MetaData()
    t = Table("t", metadata, Column("m", Map(ARRAY(Integer), String)))
    engine = make_engine(db_path)
    with engine.begin() as conn:
        metadata.create_all(conn)
        conn.execute(text("INSERT INTO t VALUES (MAP {[1, 2]: 'a', [3]: 'b'})"))
        fetched = conn.execute(select(t.c.m)).scalar_one()
        conn.execute(t.insert(), {"m": fetched})
        stored = conn.execute(select(t.c.m)).scalars().all()

    assert fetched == {"key": [[1, 2], [3]], "value": ["a", "b"]}
    assert stored == [fetched, fetched]
