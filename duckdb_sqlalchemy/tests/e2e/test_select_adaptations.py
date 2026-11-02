"""SELECT features that PostgreSQL spells differently from DuckDB."""

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Table,
    exc,
    func,
    insert,
    literal,
    select,
    tablesample,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql.operators import ColumnOperators

from .conftest import EngineFactory


def _numbers(make_engine: EngineFactory, count: int) -> Table:
    engine = make_engine("duckdb:///:memory:")
    metadata = MetaData()
    numbers = Table("numbers", metadata, Column("n", Integer, primary_key=True))
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(insert(numbers), [{"n": n} for n in range(count)])
    numbers.info["engine"] = engine
    return numbers


@pytest.mark.skipif(
    not hasattr(ColumnOperators, "bitwise_xor"),
    reason="bitwise operators need SQLAlchemy 2.0.2",
)
def test_bitwise_xor_runs(make_engine: EngineFactory) -> None:
    numbers = _numbers(make_engine, 4)
    with numbers.info["engine"].connect() as conn:
        rows = conn.scalars(select(numbers.c.n.bitwise_xor(1)).order_by(numbers.c.n))
        assert list(rows) == [1, 0, 3, 2]


def test_with_for_update_is_ignored(make_engine: EngineFactory) -> None:
    numbers = _numbers(make_engine, 3)
    with Session(numbers.info["engine"]) as session:
        for options in [{}, {"read": True}, {"nowait": True, "of": numbers}]:
            stmt = select(numbers.c.n).where(numbers.c.n > 0).with_for_update(**options)
            assert sorted(session.scalars(stmt)) == [1, 2]
        session.execute(numbers.update().values(n=numbers.c.n + 10))
        session.commit()


def test_fetch_percent_limits_by_percentage(make_engine: EngineFactory) -> None:
    numbers = _numbers(make_engine, 10)
    stmt = select(numbers.c.n).order_by(numbers.c.n).offset(2).fetch(30, percent=True)
    with numbers.info["engine"].connect() as conn:
        assert list(conn.scalars(stmt)) == [2, 3, 4]
        with pytest.raises(exc.CompileError, match="WITH TIES"):
            conn.execute(
                select(numbers.c.n).order_by(numbers.c.n).fetch(2, with_ties=True)
            )


def test_tablesample_samples_a_percentage(make_engine: EngineFactory) -> None:
    numbers = _numbers(make_engine, 5000)
    count = select(func.count())
    with numbers.info["engine"].connect() as conn:
        everything = tablesample(numbers, 100)
        assert conn.scalar(count.select_from(everything)) == 5000
        seeded = tablesample(numbers, func.bernoulli(10), seed=literal(7))
        sampled = conn.scalar(count.select_from(seeded))
        assert 300 < sampled < 700  # 10 percent, not 10 rows
        assert conn.scalar(count.select_from(seeded)) == sampled
