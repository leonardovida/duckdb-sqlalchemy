import importlib.util

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select

from duckdb_sqlalchemy import _build_bulk_insert_data


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


def test_build_bulk_insert_data_handles_positional_rows() -> None:
    if (
        importlib.util.find_spec("pandas") is None
        and importlib.util.find_spec("pyarrow") is None
    ):
        pytest.skip("pandas or pyarrow is required for bulk insert fast path")

    rows = [(1, "Ada"), (2, "Grace")]
    data = _build_bulk_insert_data(rows, ["id", "name"])

    assert data is not None
    if hasattr(data, "to_pydict"):
        assert data.to_pydict() == {"id": [1, 2], "name": ["Ada", "Grace"]}
    else:
        assert list(data.columns) == ["id", "name"]
        assert data.to_dict(orient="records") == [
            {"id": 1, "name": "Ada"},
            {"id": 2, "name": "Grace"},
        ]
