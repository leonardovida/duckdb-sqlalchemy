import importlib.util

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select


def test_bulk_insert_register_path() -> None:
    if (
        importlib.util.find_spec("pandas") is None
        and importlib.util.find_spec("pyarrow") is None
    ):
        pytest.skip("pandas or pyarrow is required for bulk insert fast path")

    engine = create_engine("duckdb:///:memory:")
    md = MetaData()
    t = Table(
        "bulk_insert",
        md,
        Column("id", Integer),
        Column("name", String),
    )
    md.create_all(engine)

    rows = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]

    with engine.begin() as conn:
        conn = conn.execution_options(duckdb_copy_threshold=1)
        conn.execute(t.insert(), rows)

    with engine.connect() as conn:
        result = conn.execute(select(t.c.id, t.c.name).order_by(t.c.id)).fetchall()
        assert result == [(1, "Ada"), (2, "Grace")]
