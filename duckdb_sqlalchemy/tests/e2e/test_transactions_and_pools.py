"""Transaction boundaries and connection pooling against real databases."""

import threading
from pathlib import Path
from typing import List

import pytest
from sqlalchemy import exc, inspect, pool, text

from .conftest import EngineFactory


def test_failed_transaction_rolls_back_ddl_and_dml(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (i INTEGER PRIMARY KEY)"))
        conn.execute(text("INSERT INTO t VALUES (1)"))

    with pytest.raises(RuntimeError):
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE scratch (i INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (2)"))
            raise RuntimeError("abort")

    with engine.connect() as conn:
        with pytest.raises(exc.IntegrityError):
            conn.execute(text("INSERT INTO t VALUES (1)"))
        conn.rollback()
        # The connection stays usable after a failed statement.
        conn.execute(text("INSERT INTO t VALUES (3)"))
        conn.commit()
        assert conn.execute(text("SELECT list(i ORDER BY i) FROM t")).scalar() == [
            1,
            3,
        ]
    assert not inspect(engine).has_table("scratch")


def test_savepoints_fail_loudly_and_keep_the_outer_transaction(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (i INTEGER)"))
        conn.commit()
        with conn.begin():
            conn.execute(text("INSERT INTO t VALUES (1)"))
            # DuckDB has no SAVEPOINT. The error must surface, not silently
            # degrade into a flat transaction.
            with pytest.raises(exc.ProgrammingError, match="SAVEPOINT"):
                conn.begin_nested()
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM t")).scalar() == 1


def test_autocommit_connection_is_reset_when_returned_to_the_pool(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path, pool_size=1, max_overflow=0)
    with engine.connect() as conn:
        auto = conn.execution_options(isolation_level="AUTOCOMMIT")
        auto.execute(text("CREATE TABLE t AS SELECT 1 AS i"))
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO t VALUES (2)"))
        conn.rollback()
        assert conn.execute(text("SELECT count(*) FROM t")).scalar() == 1


@pytest.mark.parametrize(
    "url, pool_class, shared",
    [
        ("duckdb:///:memory:", pool.SingletonThreadPool, False),
        ("duckdb:///:memory:shared_e2e", pool.QueuePool, True),
        ("FILE", pool.QueuePool, True),
    ],
    ids=["memory", "named-memory", "file"],
)
def test_threads_see_the_database_their_pool_promises(
    url: str,
    pool_class: type,
    shared: bool,
    db_path: Path,
    make_engine: EngineFactory,
) -> None:
    engine = make_engine(db_path if url == "FILE" else url, pool_pre_ping=True)
    assert isinstance(engine.pool, pool_class)
    errors: List[BaseException] = []
    counts: List[int] = []
    # Stay within SingletonThreadPool's default size so no thread's private
    # in-memory database is recycled mid-test.
    workers = 4
    barrier = threading.Barrier(workers)
    if shared:
        # Concurrent CREATE of one table is a DuckDB write-write conflict.
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (n INTEGER)"))

    def worker(n: int) -> None:
        try:
            if not shared:
                with engine.begin() as conn:
                    conn.execute(text("CREATE TABLE t (n INTEGER)"))
            barrier.wait(timeout=30)
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO t VALUES (:n)"), {"n": n})
            barrier.wait(timeout=30)
            with engine.connect() as conn:
                counts.append(conn.execute(text("SELECT count(*) FROM t")).scalar())
        except BaseException as error:  # pragma: no cover - surfaced below
            errors.append(error)
            barrier.abort()

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, errors
    # One database for every thread, or one private database per thread.
    assert counts == ([workers] * workers if shared else [1] * workers)
    if isinstance(engine.pool, pool.QueuePool):
        assert engine.pool.checkedout() == 0


@pytest.mark.parametrize("override", ["null", "queue", "singleton"])
def test_pool_override_changes_connection_reuse(
    override: str, make_engine: EngineFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DUCKDB_SQLALCHEMY_POOL", override)
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t AS SELECT 1 AS i"))
        conn.commit()
    with engine.connect() as conn:
        reused = inspect(conn).has_table("t")
    # NullPool opens a fresh in-memory database each checkout.
    assert reused is (override != "null")
