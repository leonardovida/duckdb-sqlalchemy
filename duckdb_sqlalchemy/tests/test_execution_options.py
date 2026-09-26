import importlib.util
from collections import UserDict
from typing import Any

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    bindparam,
    create_engine,
    func,
    select,
    text,
)

from duckdb_sqlalchemy import ConnectionWrapper, _build_bulk_insert_data
from duckdb_sqlalchemy._bulk_insert import infer_bulk_insert_column_keys


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


def test_bulk_insert_register_path_preserves_compiled_position_order() -> None:
    if (
        importlib.util.find_spec("pandas") is None
        and importlib.util.find_spec("pyarrow") is None
    ):
        pytest.skip("pandas or pyarrow is required for bulk insert fast path")

    engine = create_engine("duckdb:///:memory:", use_insertmanyvalues=False)
    md = MetaData()
    t = Table(
        "ordered_bulk_insert",
        md,
        Column("z_col", String),
        Column("a_col", String),
        Column("m_col", String),
    )
    md.create_all(engine)

    rows = [
        {"z_col": "Z", "a_col": "A", "m_col": "M"},
        {"z_col": "Z2", "a_col": "A2", "m_col": "M2"},
    ]

    with engine.begin() as conn:
        conn.execution_options(duckdb_copy_threshold=1).execute(t.insert(), rows)

    with engine.connect() as conn:
        result = conn.execute(select(t.c.z_col, t.c.a_col, t.c.m_col)).fetchall()

    assert result == [("Z", "A", "M"), ("Z2", "A2", "M2")]


@pytest.mark.parametrize("threshold", [0, 1])
def test_bulk_insert_respects_schema_translation(
    threshold: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    registered_views: list[str] = []

    def track_register(connection: ConnectionWrapper, name: str, data: Any) -> Any:
        registered_views.append(name)
        return connection.__getattr__("register")(name, data)

    monkeypatch.setattr(ConnectionWrapper, "register", track_register, raising=False)
    engine = create_engine("duckdb:///:memory:", use_insertmanyvalues=False)
    table = Table(
        "bulk_translate",
        MetaData(),
        Column("id", Integer),
        schema="logical",
    )
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE SCHEMA translated")
            connection.exec_driver_sql(
                "CREATE TABLE translated.bulk_translate(id INTEGER)"
            )
            translated = connection.execution_options(
                schema_translate_map={"logical": "translated"},
                duckdb_copy_threshold=threshold,
            )
            translated.execute(table.insert(), [{"id": 1}, {"id": 2}])
            assert translated.execute(
                select(table.c.id).order_by(table.c.id)
            ).all() == [
                (1,),
                (2,),
            ]
            assert len(registered_views) == threshold
    finally:
        engine.dispose()


@pytest.mark.parametrize("threshold", [0, 1])
@pytest.mark.parametrize(
    "case", ["alias", "expression", "sql_default", "literal_default"]
)
def test_bulk_insert_preserves_statement_semantics(threshold: int, case: str) -> None:
    engine = create_engine("duckdb:///:memory:", use_insertmanyvalues=False)
    default = (
        func.length("abc")
        if case == "sql_default"
        else text("3")
        if case == "literal_default"
        else None
    )
    table = Table(
        "bulk_semantics",
        MetaData(),
        Column("id", Integer),
        Column(
            "value",
            Integer,
            key="alias" if case == "alias" else "value",
            default=default,
        ),
    )
    table.create(engine)
    statement = table.insert()
    if case == "alias":
        rows = [{"id": 1, "alias": 3}, {"id": 2, "alias": 4}]
    elif case == "expression":
        statement = statement.values(value=bindparam("input_value") + 1)
        rows = [{"id": 1, "input_value": 2}, {"id": 2, "input_value": 3}]
    else:
        rows = [{"id": 1}, {"id": 2}]
    try:
        with engine.begin() as connection:
            connection.execution_options(duckdb_copy_threshold=threshold).execute(
                statement, rows
            )
            assert connection.execute(select(table).order_by(table.c.id)).all() == [
                (1, 3),
                (2, 4 if case in {"alias", "expression"} else 3),
            ]
    finally:
        engine.dispose()


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


def test_build_bulk_insert_data_handles_mapping_rows() -> None:
    if (
        importlib.util.find_spec("pandas") is None
        and importlib.util.find_spec("pyarrow") is None
    ):
        pytest.skip("pandas or pyarrow is required for bulk insert fast path")

    rows = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
    data = _build_bulk_insert_data(rows, ["id", "name"])

    assert data is not None
    if hasattr(data, "to_pydict"):
        assert data.to_pydict() == {"id": [1, 2], "name": ["Ada", "Grace"]}
    else:
        assert list(data.columns) == ["id", "name"]
        assert data.to_dict(orient="records") == rows


def test_infer_bulk_insert_column_keys_handles_mapping_rows() -> None:
    rows = [UserDict({"id": 1, "name": "Ada"})]

    assert infer_bulk_insert_column_keys(rows) == ["id", "name"]
    assert infer_bulk_insert_column_keys([(1, "Ada")]) is None
