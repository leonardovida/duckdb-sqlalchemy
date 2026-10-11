"""Large executemany INSERTs load identically through the register fast path."""

import datetime
import decimal
from collections import UserDict
from pathlib import Path
from typing import Any, Dict, List

import pytest
import sqlalchemy
from packaging.version import Version
from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    func,
    insert,
    select,
    text,
)
from sqlalchemy.orm import Session

from ... import Dialect
from ...datatypes import UBigInteger
from .conftest import EngineFactory

ROWS = Dialect.duckdb_copy_threshold + 1
# The preserve_rowcount execution option arrived in SQLAlchemy 2.0.28.
PRESERVES_ROWCOUNT = Version(sqlalchemy.__version__) >= Version("2.0.28")
UTC = datetime.timezone.utc


def _table(metadata: MetaData, name: str) -> Table:
    return Table(
        name,
        metadata,
        Column("id", Integer),
        Column("big", BigInteger),
        Column("ubig", UBigInteger),
        Column("amount", Numeric(38, 12)),
        Column("at", DateTime(timezone=True)),
        Column("label", String),
        Column("doc", JSON),
    )


def _row(i: int) -> Dict[str, Any]:
    nulls = i % 7 == 0
    return {
        "id": i,
        "big": None if nulls else (-(2**63) + i % 3 if i % 2 else 2**63 - 1 - i % 3),
        # Unsigned values beyond int64 mixed with NULLs have no safe Arrow or
        # pandas form and correctly fall back to ordinary inserts.
        "ubig": 2**64 - 1 - i,
        "amount": decimal.Decimal(f"{i}.{i:012d}"[:30]) * (-1 if i % 5 else 1),
        "at": datetime.datetime(2026, 1, 1, tzinfo=UTC)
        + datetime.timedelta(seconds=i, microseconds=i % 1000),
        "label": None if nulls else f'row {i} ✓ "quoted", comma\nnewline',
        "doc": None if nulls else {"i": i, "tags": ["x", i]},
    }


@pytest.fixture
def register_calls(monkeypatch: pytest.MonkeyPatch) -> List[bool]:
    """Record whether each executemany took the register path (pass-through)."""
    calls: List[bool] = []
    original = Dialect._bulk_insert_via_register

    def spy(self: Dialect, *args: Any, **kwargs: Any) -> bool:
        used = original(self, *args, **kwargs)
        calls.append(used)
        return used

    monkeypatch.setattr(Dialect, "_bulk_insert_via_register", spy)
    return calls


@pytest.mark.parametrize("row_type", [dict, UserDict], ids=["dict", "mapping"])
def test_fast_path_and_ordinary_inserts_store_identical_rows(
    row_type: type,
    db_path: Path,
    make_engine: EngineFactory,
    register_calls: List[bool],
) -> None:
    engine = make_engine(db_path)
    metadata = MetaData()
    fast, slow = _table(metadata, "fast"), _table(metadata, "slow")
    metadata.create_all(engine)
    rows = [row_type(_row(i)) for i in range(ROWS)]

    for table, threshold in ((fast, ROWS), (slow, 0)):
        with engine.begin() as conn:
            options = {"duckdb_copy_threshold": threshold, "preserve_rowcount": True}
            result = conn.execution_options(**options).execute(table.insert(), rows)
            if PRESERVES_ROWCOUNT:
                assert result.rowcount == ROWS
            assert (
                conn.execute(select(func.count()).select_from(table)).scalar() == ROWS
            )
    assert register_calls == [True]

    with engine.connect() as conn:
        for left, right in (("fast", "slow"), ("slow", "fast")):
            diff = conn.execute(
                text(f"SELECT count(*) FROM (FROM {left} EXCEPT ALL FROM {right})")
            ).scalar_one()
            assert diff == 0, f"{left} has rows missing from {right}"
        stored = conn.execute(select(fast).order_by(fast.c.id)).mappings().all()
    assert [dict(row) for row in stored[:50]] == [_row(i) for i in range(50)]


def test_fast_path_rolls_back_with_the_transaction(
    make_engine: EngineFactory, register_calls: List[bool]
) -> None:
    engine = make_engine("duckdb:///:memory:")
    metadata = MetaData()
    table = _table(metadata, "t")
    metadata.create_all(engine)
    with pytest.raises(RuntimeError):
        with engine.begin() as conn:
            conn.execute(table.insert(), [_row(i) for i in range(ROWS)])
            raise RuntimeError("abort")
    assert register_calls == [True]
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM t")).scalar_one() == 0


def test_orm_bulk_insert_above_threshold(
    db_path: Path, make_engine: EngineFactory, register_calls: List[bool]
) -> None:
    engine = make_engine(db_path)
    metadata = MetaData()
    table = _table(metadata, "events")
    metadata.create_all(engine)
    with Session(engine) as session:
        session.execute(insert(table), [_row(i) for i in range(ROWS)])
        session.commit()
    assert register_calls == [True]
    with engine.connect() as conn:
        total = conn.execute(text("SELECT count(*), sum(id) FROM events")).one()
    assert tuple(total) == (ROWS, ROWS * (ROWS - 1) // 2)


def test_pandas_to_sql_above_threshold_round_trips(
    db_path: Path, make_engine: EngineFactory
) -> None:
    pd = pytest.importorskip("pandas")
    engine = make_engine(db_path)
    frame = pd.DataFrame(
        {
            "id": range(ROWS),
            "score": [i / 4 for i in range(ROWS)],
            "name": [None if i % 9 == 0 else f"n{i}" for i in range(ROWS)],
        }
    )
    with engine.begin() as conn:
        frame.to_sql("scores", conn, index=False)
    with engine.connect() as conn:
        back = pd.read_sql("SELECT * FROM scores ORDER BY id", conn)
    pd.testing.assert_frame_equal(back, frame, check_dtype=False)
