"""DuckDB keywords work as table and column names end to end."""

import duckdb
from sqlalchemy import Column, Integer, MetaData, Table, inspect, select

from .conftest import EngineFactory


def _keywords(*categories: str) -> list:
    rows = duckdb.sql(
        "SELECT keyword_name, keyword_category FROM duckdb_keywords() ORDER BY 1"
    ).fetchall()
    return [name for name, category in rows if not categories or category in categories]


def test_every_keyword_as_a_column_name(make_engine: EngineFactory) -> None:
    names = _keywords()
    engine = make_engine("duckdb:///:memory:")
    metadata = MetaData()
    wide = Table("wide", metadata, *(Column(name, Integer) for name in names))
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(wide.insert(), {name: i for i, name in enumerate(names)})
        assert list(conn.execute(select(wide)).one()) == list(range(len(names)))
        by_name = select(wide.c[names[-1]]).where(wide.c[names[0]] == 0)
        assert conn.execute(by_name).scalar_one() == len(names) - 1

    assert [c["name"] for c in inspect(engine).get_columns("wide")] == names


def test_reserved_keywords_as_table_names(make_engine: EngineFactory) -> None:
    # Only these categories need quoting in table position.
    names = _keywords("reserved", "type_function")
    engine = make_engine("duckdb:///:memory:")
    metadata = MetaData()
    tables = [Table(name, metadata, Column(name, Integer)) for name in names]
    metadata.create_all(engine)

    with engine.begin() as conn:
        for i, table in enumerate(tables):
            conn.execute(table.insert(), {table.name: i})
            assert conn.execute(select(table)).scalar_one() == i
    assert set(names) <= set(inspect(engine).get_table_names())
