"""Results of statements executed with ``duckdb_arrow=True``.

DuckDB 2.0 copies a DB-API result to export it as Arrow and warns about it, so
the dialect runs these statements as relations there. The tests describe what
a caller sees on every supported DuckDB version.
"""

import warnings
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Sequence,
    String,
    Table,
    exc,
    insert,
    inspect,
    select,
    text,
    update,
)
from sqlalchemy.engine import Connection
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from duckdb_sqlalchemy.datatypes import HugeInteger, UInt64

from .conftest import EngineFactory

pa = pytest.importorskip("pyarrow")

# Converting the value fails only for rows far beyond the first batch.
FAILS_LATE = (
    "SELECT CASE WHEN i > 900000 THEN CAST('x' AS INTEGER) ELSE i END AS i "
    "FROM range(1000000) r(i)"
)


def _arrow(conn: Connection) -> Connection:
    return conn.execution_options(duckdb_arrow=True)


def _numbers(conn: Connection) -> None:
    conn.execute(text("CREATE TABLE numbers (i INTEGER, s VARCHAR)"))
    conn.execute(text("INSERT INTO numbers SELECT i, 'v' || i FROM range(10) r(i)"))


def test_arrow_table_and_batches_raise_no_warning(make_engine: EngineFactory) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn, warnings.catch_warnings():
        # DuckDB 2.0 warns when a DB-API result is exported as Arrow.
        warnings.filterwarnings("error", message="Arrow output on a DB-API result")
        _numbers(conn)
        table = (
            _arrow(conn)
            .execute(text("SELECT i, s FROM numbers WHERE i < :n ORDER BY i"), {"n": 3})
            .arrow
        )
        assert table.to_pylist() == [
            {"i": 0, "s": "v0"},
            {"i": 1, "s": "v1"},
            {"i": 2, "s": "v2"},
        ]
        with _arrow(conn).execute(text("SELECT i FROM numbers")).batches(4) as reader:
            assert sum(batch.num_rows for batch in reader) == 10


def test_batches_stream_instead_of_reading_the_whole_result(
    make_engine: EngineFactory,
) -> None:
    # With several threads DuckDB can scan the failing rows before it hands
    # out the first batch, so one thread keeps the test deterministic.
    engine = make_engine(
        "duckdb:///:memory:",
        isolation_level="AUTOCOMMIT",
        connect_args={"config": {"threads": 1}},
    )
    with engine.connect() as conn:
        reader = _arrow(conn).execute(text(FAILS_LATE)).batches(1000)
        # A result read in full before the first batch would fail here.
        assert reader.read_next_batch().num_rows == 1000
        reader.close()
        assert conn.execute(text("SELECT 1")).scalar() == 1


def test_failed_arrow_read_raises_a_sqlalchemy_error_and_frees_the_connection(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:", isolation_level="AUTOCOMMIT")
    statement = text("SELECT CAST('x' || i AS INTEGER) FROM range(3) r(i)")
    with engine.connect() as conn:
        # DuckDB 1.x raises from execute(), DuckDB 2.0 from the read.
        with pytest.raises(exc.DataError) as raised:
            _arrow(conn).execute(statement).arrow
        assert "CAST('x' || i AS INTEGER)" in str(raised.value.statement)
        with pytest.raises(exc.DataError):
            _arrow(conn).execute(statement).batches(2).read_next_batch()
        assert conn.execute(text("SELECT 1")).scalar() == 1


def test_statement_runs_once_however_often_arrow_is_read(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE SEQUENCE s"))
        result = _arrow(conn).execute(text("SELECT nextval('s') AS n FROM range(3)"))
        assert result.arrow.column("n").to_pylist() == [1, 2, 3]
        assert result.arrow is result.arrow
        assert conn.execute(text("SELECT currval('s')")).scalar() == 3


def test_unread_result_keeps_its_rows_when_another_statement_runs(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        _numbers(conn)
        result = _arrow(conn).execute(text("SELECT count(*) FROM numbers"))
        conn.execute(text("INSERT INTO numbers VALUES (99, 'late')"))
        # The rows are those of the moment the next statement ran, as tuples.
        assert result.fetchone() == (10,)
        with pytest.raises(NotImplementedError, match="buffered"):
            result.cursor.to_arrow_table()


def test_rows_can_be_read_from_an_arrow_result(make_engine: EngineFactory) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        _numbers(conn)
        result = _arrow(conn).execute(text("SELECT i FROM numbers ORDER BY i"))
        assert result.fetchone() == (0,)
        with pytest.raises(exc.InvalidRequestError, match="rows were fetched"):
            result.arrow
        assert result.fetchmany(2) == [(1,), (2,)]
        assert [row.i for row in result] == [3, 4, 5, 6, 7, 8, 9]
        assert _arrow(conn).execute(text("SELECT 7")).scalar() == 7
        assert _arrow(conn).execute(text("SELECT i FROM numbers")).first() == (0,)
        assert conn.execute(text("SELECT count(*) FROM numbers")).scalar() == 10


def test_returning_and_dml_counts_with_the_arrow_option(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    numbers = Table("numbers", MetaData(), Column("i", Integer), Column("s", String))
    with engine.begin() as conn:
        _numbers(conn)
        one = _arrow(conn).execute(
            insert(numbers).returning(numbers.c.i), {"i": 50, "s": "a"}
        )
        assert one.arrow.to_pylist() == [{"i": 50}]
        changed = _arrow(conn).execute(
            update(numbers)
            .where(numbers.c.i == 50)
            .values(s="z")
            .returning(numbers.c.s)
        )
        assert changed.arrow.to_pylist() == [{"s": "z"}]
        assert _arrow(conn).execute(update(numbers).values(s="q")).rowcount == 11
        assert conn.execute(text("SELECT count(*) FROM numbers")).scalar() == 11


def test_multi_row_returning_arrow_has_duckdb_column_types(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    big = Table(
        "big",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("h", HugeInteger),
        Column("u", UInt64),
    )
    stmt = insert(big).returning(big.c.id, big.c.h, big.c.u)
    # Powers of two, which DuckDB 1.3 binds exactly although it binds integers
    # beyond BIGINT as DOUBLE.
    rows = [{"id": i, "h": 2 ** (100 + i), "u": 2**64 - 1 - i} for i in range(5)]
    with engine.begin() as conn:
        big.metadata.create_all(conn)
        one = _arrow(conn).execute(stmt, rows[:1]).arrow
        # Two rows per page, so the four rows take two INSERT statements.
        conn.execution_options(insertmanyvalues_page_size=2)
        many = conn.execute(stmt, rows[1:]).arrow
    assert many.schema.equals(one.schema)
    assert many.column("id").to_pylist() == [1, 2, 3, 4]
    assert many.column("h").to_pylist() == [
        Decimal(2 ** (100 + i)) for i in range(1, 5)
    ]
    assert many.column("u").to_pylist() == [2**64 - 1 - i for i in range(1, 5)]
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM big")).scalar() == 5


def test_multi_row_returning_arrow_has_only_the_returned_columns(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    named = Table(
        "named",
        MetaData(),
        Column("id", Integer, Sequence("named_id_seq"), primary_key=True),
        Column("name", String),
    )
    try:
        stmt = insert(named).returning(named.c.name, sort_by_parameter_order=True)
    except exc.ArgumentError:
        pytest.skip("sort_by_parameter_order needs SQLAlchemy 2.0.10")
    with engine.begin() as conn:
        named.metadata.create_all(conn)
        # SQLAlchemy adds the id to RETURNING to sort the rows; it is not a
        # column of the result.
        result = _arrow(conn).execute(stmt, [{"name": n} for n in "abc"])
        assert list(result.keys()) == ["name"]
        assert result.arrow.to_pylist() == [{"name": n} for n in "abc"]


def test_textual_dml_count_row_reads_as_arrow(make_engine: EngineFactory) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (i INTEGER)"))
        inserted = _arrow(conn).execute(text("INSERT INTO t VALUES (1), (2)"))
        assert inserted.rowcount == 2
        table = inserted.arrow
        assert table.to_pylist() == [{"Count": 2}]
        assert table.schema.field("Count").type == pa.int64()
        deleted = conn.execute(text("DELETE FROM t WHERE i = 1"))
        with deleted.batches() as reader:
            assert reader.read_all().to_pylist() == [{"Count": 1}]
        assert conn.execute(text("SELECT list(i) FROM t")).scalar() == [2]


def test_unread_arrow_result_does_not_block_the_pooled_connection(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        _arrow(conn).execute(text("SELECT * FROM range(50000)"))
    with engine.connect() as conn:
        reader = _arrow(conn).execute(text("SELECT * FROM range(50000)")).batches(100)
        assert reader.read_next_batch().num_rows == 100
    with engine.connect() as conn:
        assert conn.execute(text("SELECT 4")).scalar() == 4


def test_pandas_fetch_from_an_arrow_result(make_engine: EngineFactory) -> None:
    pytest.importorskip("pandas")
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        for fetch in ("df", "fetch_df", "fetchdf"):
            cursor: Any = _arrow(conn).execute(text("SELECT * FROM range(3)")).cursor
            assert getattr(cursor, fetch)().shape == (3, 1)


def test_engine_wide_arrow_option_keeps_orm_and_reflection_working(
    make_engine: EngineFactory,
) -> None:
    class Base(DeclarativeBase):
        pass

    class Widget(Base):
        __tablename__ = "widget"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]

    engine = make_engine("duckdb:///:memory:", execution_options={"duckdb_arrow": True})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all([Widget(name="a"), Widget(name="b")])
        session.commit()
        widget = session.get(Widget, 1)
        assert widget is not None
        widget.name = "c"
        session.commit()
        names = session.scalars(select(Widget.name).order_by(Widget.id)).all()
        assert names == ["c", "b"]
    inspector = inspect(engine)
    assert inspector.get_table_names() == ["widget"]
    assert [column["name"] for column in inspector.get_columns("widget")] == [
        "id",
        "name",
    ]
    Base.metadata.drop_all(engine)
    assert inspect(engine).get_table_names() == []


def test_arrow_result_supports_context_manager(make_engine: EngineFactory) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        with _arrow(conn).execute(text("SELECT i FROM range(3) r(i)")) as result:
            assert result.arrow.column("i").to_pylist() == [0, 1, 2]

        with _arrow(conn).execute(text("SELECT i FROM range(50000) r(i)")) as unread:
            assert unread.returns_rows

        with _arrow(conn).execute(text("SELECT i FROM range(50000) r(i)")) as streaming:
            reader = streaming.batches(100)
            assert reader.read_next_batch().num_rows == 100
        assert reader.closed
        assert conn.execute(text("SELECT 9")).scalar() == 9
