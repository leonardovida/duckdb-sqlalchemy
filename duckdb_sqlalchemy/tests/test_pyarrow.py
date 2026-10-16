from typing import Any, Optional, cast

import pytest
from pyarrow import RecordBatch, RecordBatchReader
from pyarrow import Table as ArrowTable
from sqlalchemy import MetaData, Table, create_engine, text

import duckdb_sqlalchemy

# DuckDB 2.0 results export Arrow only when the statement runs with duckdb_arrow.
_ARROW_CURSOR_OPTIONS = (
    {} if duckdb_sqlalchemy._DBAPI_EXPORTS_ARROW else {"duckdb_arrow": True}
)


def _to_arrow_table(cursor: Any) -> Any:
    if hasattr(cursor, "to_arrow_table"):
        return cursor.to_arrow_table()
    return cursor.fetch_arrow_table()


def _to_arrow_reader(cursor: Any, batch_size: Optional[int] = None) -> Any:
    if hasattr(cursor, "to_arrow_reader"):
        if batch_size is None:
            return cursor.to_arrow_reader()
        return cursor.to_arrow_reader(batch_size=batch_size)
    if batch_size is None:
        return cursor.fetch_record_batch()
    return cursor.fetch_record_batch(rows_per_batch=batch_size)


def test_fetch_arrow() -> None:
    engine = create_engine("duckdb:///:memory:")
    with engine.begin() as con:
        con.execute(text("CREATE TABLE tbl (label VARCHAR, value DOUBLE)"))
        con.execute(
            text(
                "INSERT INTO tbl VALUES ('xx',-1.0), ('ww',-4.5), ('zz',6.0), ('yy',2.5)"
            )
        )

    md = MetaData()
    t = Table("tbl", md, autoload_with=engine)
    stmt = t.select().where(t.c.value > -4.0).order_by(t.c.label)

    # rows
    with engine.begin() as con:
        cursor = con.execute(stmt).cursor
        assert cursor is not None
        res = cursor.fetchall()
        assert res == [("xx", -1.0), ("yy", 2.5), ("zz", 6.0)]

    # arrow table
    with engine.begin() as con:
        res = _to_arrow_table(
            con.execution_options(**_ARROW_CURSOR_OPTIONS).execute(stmt).cursor
        )
        assert isinstance(res, ArrowTable)
        assert res == ArrowTable.from_pydict(
            {"label": ["xx", "yy", "zz"], "value": [-1.0, 2.5, 6.0]}
        )

    # arrow batches
    with engine.begin() as con:
        res = _to_arrow_reader(
            con.execution_options(**_ARROW_CURSOR_OPTIONS).execute(stmt).cursor
        )
        assert isinstance(res, RecordBatchReader)
        assert res.read_all() == ArrowTable.from_pydict(
            {"label": ["xx", "yy", "zz"], "value": [-1.0, 2.5, 6.0]}
        )
        res = _to_arrow_reader(
            con.execution_options(**_ARROW_CURSOR_OPTIONS).execute(stmt).cursor,
            batch_size=2,
        )
        assert res.read_next_batch() == RecordBatch.from_pydict(
            {"label": ["xx", "yy"], "value": [-1.0, 2.5]}
        )
        assert res.read_next_batch() == RecordBatch.from_pydict(
            {"label": ["zz"], "value": [6.0]}
        )


@pytest.mark.skipif(
    duckdb_sqlalchemy._DBAPI_EXPORTS_ARROW, reason="DB-API results export Arrow"
)
def test_plain_cursor_arrow_points_to_the_execution_option() -> None:
    engine = create_engine("duckdb:///:memory:")
    with engine.connect() as con:
        cursor = con.exec_driver_sql("SELECT 1 AS a").cursor
        assert not hasattr(cursor, "to_arrow_table")
        with pytest.raises(AttributeError, match="duckdb_arrow=True"):
            cursor.to_arrow_table()


def test_arrow_execution_option() -> None:
    engine = create_engine("duckdb:///:memory:")
    with engine.begin() as con:
        con.execute(text("CREATE TABLE tbl (label VARCHAR, value DOUBLE)"))
        con.execute(text("INSERT INTO tbl VALUES ('aa', 1.0), ('bb', 2.0)"))

    with engine.connect().execution_options(duckdb_arrow=True) as con:
        result = con.execute(text("SELECT * FROM tbl ORDER BY label"))
        table = cast(Any, result).arrow
        assert isinstance(table, ArrowTable)
        assert table == ArrowTable.from_pydict(
            {"label": ["aa", "bb"], "value": [1.0, 2.0]}
        )
