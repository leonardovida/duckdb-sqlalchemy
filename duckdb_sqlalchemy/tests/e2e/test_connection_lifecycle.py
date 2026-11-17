"""Connection and result lifecycle edge cases against real databases."""

import gc
from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, event, exc, insert, text
from sqlalchemy.engine import Connection

from duckdb_sqlalchemy import insert_from_arrow

from .conftest import EngineFactory

pa = pytest.importorskip("pyarrow")


def test_raw_arrow_reader_does_not_block_later_statements(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    with engine.connect() as conn:
        result = conn.exec_driver_sql("SELECT * FROM range(10)")
        cursor = result.cursor
        # to_arrow_reader replaces fetch_record_batch, which DuckDB 2.0 removes.
        fetch_reader = getattr(cursor, "to_arrow_reader", None)
        reader = fetch_reader() if fetch_reader else cursor.fetch_record_batch()
        assert reader.read_all().num_rows == 10
        # result stays referenced; the next statement and commit still work
        assert conn.execute(text("SELECT 1")).scalar() == 1
        conn.commit()
        assert result is not None


def _partly_read_reader(conn: Connection) -> object:
    cursor = conn.exec_driver_sql("SELECT * FROM range(10)").cursor
    fetch_reader = getattr(cursor, "to_arrow_reader", None)
    if fetch_reader is not None:
        reader = fetch_reader(batch_size=2)
    else:
        reader = cursor.fetch_record_batch(rows_per_batch=2)
    assert reader.read_next_batch().num_rows == 2
    return reader


def test_partly_read_raw_arrow_reader_is_closed_by_commit_and_checkin(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path, pool_size=1)
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (i INTEGER)"))
        conn.commit()

        # The reader outlives its result and cursor. DuckDB 2.0 refused
        # COMMIT, ROLLBACK and every later statement while it was open.
        reader = _partly_read_reader(conn)
        conn.execute(text("INSERT INTO t VALUES (1)"))
        conn.commit()
        with pytest.raises(pa.ArrowInvalid, match="closed"):
            reader.read_next_batch()

        reader = _partly_read_reader(conn)
    # Pool checkin rolls back, which must not fail or leave the reader open.

    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM t")).scalar() == 1
    with pytest.raises(pa.ArrowInvalid, match="closed"):
        reader.read_next_batch()


def test_failed_commit_does_not_leak_a_transaction_to_the_pool(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path, pool_size=1)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (i INTEGER)"))

    with pytest.raises(exc.InvalidRequestError, match="batch reader"):
        with engine.connect() as conn:
            conn.execute(text("INSERT INTO t VALUES (1)"))
            result = conn.execution_options(duckdb_arrow=True).execute(
                text("SELECT * FROM range(100000)")
            )
            reader = result.batches(1000)
            reader.read_next_batch()
            conn.commit()

    with engine.connect() as conn:
        # the failed commit rolled back instead of leaving BEGIN open
        assert conn.execute(text("SELECT count(*) FROM t")).scalar() == 0


def test_retry_reports_a_fetch_error_of_an_earlier_result(
    make_engine: EngineFactory,
) -> None:
    def flaky(i: int) -> int:
        if i == 250_000:
            raise RuntimeError("HTTP Error: 503 Service Unavailable")
        return i

    engine = make_engine(
        "duckdb:///:memory:",
        execution_options={"duckdb_retry_on_transient": True},
        # With more threads, another thread may report "Interrupted!"
        # instead of the UDF's error.
        connect_args={"config": {"threads": 1}},
    )
    event.listen(
        engine,
        "connect",
        lambda dbapi_conn, _: dbapi_conn.create_function(
            "flaky", flaky, side_effects=True
        ),
    )
    with engine.connect() as conn:
        big = conn.execute(
            text("SELECT flaky(i::INTEGER) AS v FROM range(300000) t(i)")
        )
        assert big.fetchone() == (0,)
        # Buffering the first result fails. That error must surface instead of
        # being retried as this statement's failure.
        with pytest.raises(exc.DBAPIError, match="503"):
            conn.execute(text("SELECT 42"))
        with pytest.raises(exc.InvalidRequestError, match="could not be read"):
            big.fetchall()


def test_write_conflict_does_not_invalidate_the_connection(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE job_timeouts (i INTEGER)"))

    with engine.connect() as first, engine.connect() as second:
        first.execute(text("ALTER TABLE job_timeouts ADD COLUMN a INTEGER"))
        with pytest.raises(exc.DBAPIError, match="job_timeouts") as raised:
            second.execute(text("ALTER TABLE job_timeouts ADD COLUMN b INTEGER"))
            second.commit()
        # "timeouts" in the conflict message is not a lost connection
        assert raised.value.connection_invalidated is False
        second.rollback()
        first.commit()
        assert second.execute(text("SELECT count(*) FROM job_timeouts")).scalar() == 0


def test_register_returns_no_rows(make_engine: EngineFactory) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        previous = conn.exec_driver_sql("SELECT * FROM range(5)")
        previous.fetchone()
        previous.close()
        result = conn.exec_driver_sql(
            "register", ("people", pa.table({"name": ["Ada", "Grace"]}))
        )
        assert result.returns_rows is False
        assert conn.execute(text("SELECT count(*) FROM people")).scalar() == 2


def test_arrow_returning_for_executemany_insert(make_engine: EngineFactory) -> None:
    engine = make_engine("duckdb:///:memory:")
    table = Table("t", MetaData(), Column("i", Integer))
    with engine.begin() as conn:
        table.create(conn)
        result = conn.execution_options(duckdb_arrow=True).execute(
            insert(table).returning(table.c.i), [{"i": n} for n in range(5)]
        )
        assert result.arrow.column("i").to_pylist() == [0, 1, 2, 3, 4]


def test_raw_commit_keeps_later_writes_in_a_transaction(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (i INTEGER)"))
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO t VALUES (1)"))
        conn.exec_driver_sql("commit")
        conn.execute(text("INSERT INTO t VALUES (2)"))
        conn.rollback()
        assert conn.execute(text("SELECT list(i) FROM t")).scalar() == [1]


def _abandon_partly_read_result(conn: Connection, how: str) -> None:
    """Leave an unfinished DuckDB result behind without a cursor to buffer it."""
    result = conn.exec_driver_sql("SELECT * FROM range(5000)")
    assert result.fetchone() == (0,)
    if how == "closed":
        result.close()
    else:
        del result
        gc.collect()


# DuckDB 2.0 refuses every call except execute() while a result is unfinished,
# where DuckDB 1.x discards the result.
@pytest.mark.parametrize("how", ["closed", "dropped"])
def test_raw_driver_calls_work_after_an_abandoned_result(
    make_engine: EngineFactory, how: str
) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        _abandon_partly_read_result(conn, how)
        driver = conn.connection.driver_connection
        assert driver is not None
        assert driver.sql("SELECT 42").fetchall() == [(42,)]

        _abandon_partly_read_result(conn, how)
        driver.register("people", pa.table({"name": ["Ada", "Grace"]}))
        assert conn.execute(text("SELECT count(*) FROM people")).scalar() == 2


@pytest.mark.parametrize("how", ["closed", "dropped"])
def test_bulk_paths_work_after_an_abandoned_result(
    make_engine: EngineFactory, how: str
) -> None:
    engine = make_engine("duckdb:///:memory:")
    table = Table("t", MetaData(), Column("i", Integer))
    with engine.begin() as conn:
        table.create(conn)

        _abandon_partly_read_result(conn, how)
        conn.execution_options(duckdb_copy_threshold=10).execute(
            insert(table), [{"i": n} for n in range(20)]
        )

        _abandon_partly_read_result(conn, how)
        insert_from_arrow(conn, table, pa.table({"i": [100, 101]}))

        assert conn.execute(text("SELECT count(*), max(i) FROM t")).one() == (22, 101)


def test_raw_driver_call_keeps_the_rows_of_an_open_result(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        result = conn.exec_driver_sql("SELECT * FROM range(5)")
        assert result.fetchone() == (0,)
        driver = conn.connection.driver_connection
        assert driver is not None
        driver.register("people", pa.table({"name": ["Ada"]}))
        assert result.fetchall() == [(1,), (2,), (3,), (4,)]
        assert conn.execute(text("SELECT name FROM people")).scalar() == "Ada"
