import math
import tempfile
from pathlib import Path
from typing import Any

import duckdb
import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    literal,
    select,
)

import duckdb_sqlalchemy.bulk
from duckdb_sqlalchemy import read_csv, read_csv_auto, read_parquet
from duckdb_sqlalchemy.bulk import (
    _format_copy_options,
    copy_from_csv,
    copy_from_parquet,
    copy_from_rows,
    copy_to_parquet,
)


def test_copy_from_rows_writes_utf8_regardless_of_locale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_named_temporary_file = tempfile.NamedTemporaryFile

    def cp1252_default(*args: Any, **kwargs: Any) -> Any:
        # Simulate a platform whose locale encoding cannot encode the rows.
        kwargs.setdefault("encoding", "cp1252")
        return real_named_temporary_file(*args, **kwargs)

    monkeypatch.setattr(
        duckdb_sqlalchemy.bulk.tempfile, "NamedTemporaryFile", cp1252_default
    )
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t(i INTEGER, label VARCHAR)")

    copy_from_rows(conn, "t", [(1, "日本語"), (2, "ümlaut ✓")], columns=["i", "label"])

    assert conn.execute("SELECT i, label FROM t ORDER BY i").fetchall() == [
        (1, "日本語"),
        (2, "ümlaut ✓"),
    ]


def test_copy_from_rows_keeps_empty_strings_distinct_from_null() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t(i INTEGER, a VARCHAR, b VARCHAR)")

    copy_from_rows(
        conn,
        "t",
        [
            {"i": 1, "a": "", "b": None},
            {"i": 2, "a": None, "b": ""},
            {"i": None, "a": "x", "b": "NULL"},
        ],
        chunk_size=2,
        include_header=True,
    )

    assert conn.execute("SELECT i, a, b FROM t ORDER BY a NULLS FIRST").fetchall() == [
        (2, None, ""),
        (1, "", None),
        (None, "x", "NULL"),
    ]


@pytest.mark.parametrize("key", ["nullstr", "NULL", "Null"])
@pytest.mark.parametrize("value", ["NA", ["NA", "n/a"]])
def test_copy_from_rows_honors_user_null_string(key: str, value: Any) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t(i INTEGER, a VARCHAR)")
    options = {key: value}

    copy_from_rows(
        conn, "t", [(1, "NA"), (2, None), (3, "")], columns=["i", "a"], **options
    )

    assert conn.execute("SELECT i, a FROM t ORDER BY i").fetchall() == [
        (1, None),
        (2, None),
        (3, ""),
    ]


@pytest.mark.parametrize("value", [None, [], [1], 1])
def test_copy_from_rows_rejects_unusable_user_null_string(value: Any) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t(a VARCHAR)")

    if value is None:
        # ``None`` options are dropped, so the default NULL marker applies.
        copy_from_rows(conn, "t", [("",), (None,)], nullstr=value)
        assert conn.execute("SELECT a FROM t").fetchall() == [("",), (None,)]
        return

    with pytest.raises(ValueError, match="nullstr"):
        copy_from_rows(conn, "t", [("x",)], nullstr=value)


@pytest.mark.parametrize(
    "value", [b"\x00\x01", bytearray(b"ab"), memoryview(b"ab")], ids=type
)
def test_copy_from_rows_rejects_binary_values(value: Any) -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t(i INTEGER, payload BLOB)")

    with pytest.raises(TypeError, match="copy_from_parquet"):
        copy_from_rows(conn, "t", [(1, value)], columns=["i", "payload"])

    assert conn.execute("SELECT COUNT(*) FROM t").fetchone() == (0,)


@pytest.mark.parametrize(
    ("translate_map", "logical_schema"),
    [({"tenant_x": "tenant_a"}, "tenant_x"), ({None: "tenant_a"}, None)],
)
def test_copy_helpers_respect_schema_translation(
    tmp_path: Path, translate_map: dict[Any, str], logical_schema: Any
) -> None:
    table = Table(
        "events",
        MetaData(),
        Column("id", Integer),
        Column("label", String),
        schema=logical_schema,
    )
    csv_path = tmp_path / "events.csv"
    csv_path.write_text("2,two\n", encoding="utf-8")
    parquet_path = tmp_path / "events.parquet"
    engine = create_engine("duckdb:///:memory:")
    try:
        with engine.connect() as base:
            base.exec_driver_sql("CREATE SCHEMA tenant_a")
            base.exec_driver_sql(
                "CREATE TABLE tenant_a.events(id INTEGER, label VARCHAR)"
            )
            base.exec_driver_sql(
                f"COPY (SELECT 3 AS id, 'three' AS label) TO '{parquet_path}' "
                "(FORMAT parquet)"
            )
            conn = base.execution_options(schema_translate_map=translate_map)

            copy_from_rows(conn, table, [{"id": 1, "label": "one"}])
            copy_from_csv(conn, table, csv_path)
            copy_from_parquet(conn, table, parquet_path)

            rows = base.exec_driver_sql(
                "SELECT id, label FROM tenant_a.events ORDER BY id"
            ).fetchall()
            assert rows == [(1, "one"), (2, "two"), (3, "three")]
    finally:
        engine.dispose()


def test_copy_options_render_mappings_as_struct_literals() -> None:
    rendered = _format_copy_options(
        {
            "kv_metadata": {"it's": "v'1", "n": 2},
            "field_ids": {"s": {"__duckdb_field_id": 1, "x": 2}},
        }
    )

    assert rendered == (
        " (KV_METADATA {'it''s': 'v''1', 'n': 2},"
        " FIELD_IDS {'s': {'__duckdb_field_id': 1, 'x': 2}})"
    )


@pytest.mark.parametrize("value", [math.inf, -math.inf, math.nan])
def test_copy_options_reject_non_finite_floats(value: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        _format_copy_options({"compression_level": value})
    with pytest.raises(ValueError, match="finite"):
        _format_copy_options({"kv_metadata": {"k": value}})


def test_copy_to_parquet_writes_kv_metadata_and_field_ids(tmp_path: Path) -> None:
    path = tmp_path / "meta.parquet"
    engine = create_engine("duckdb:///:memory:")
    try:
        with engine.connect() as conn:
            copy_to_parquet(
                conn,
                select(literal(1).label("a")),
                path,
                kv_metadata={"owner": "o'brien", "version": 2},
                field_ids={"a": 42},
            )
            metadata = conn.exec_driver_sql(
                "SELECT decode(key), decode(value) "
                f"FROM parquet_kv_metadata('{path}') ORDER BY 1"
            ).fetchall()
            assert metadata == [("owner", "o'brien"), ("version", "2")]
            field_ids = conn.exec_driver_sql(
                f"SELECT field_id FROM parquet_schema('{path}') WHERE name = 'a'"
            ).scalar_one()
            assert field_ids == 42
    finally:
        engine.dispose()


def test_read_csv_mapping_columns_are_passed_to_duckdb(tmp_path: Path) -> None:
    csv_path = tmp_path / "typed.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    engine = create_engine("duckdb:///:memory:")
    try:
        with engine.connect() as conn:
            for helper in (read_csv, read_csv_auto):
                relation = helper(
                    str(csv_path), columns={"a": "VARCHAR", "b": "BIGINT"}, header=True
                )
                row = conn.execute(
                    select(relation.c.a, relation.c.b).select_from(relation)
                ).one()
                assert row == ("1", 2)
                assert isinstance(row[0], str)

            # Sequences keep naming output columns only; DuckDB still infers types.
            relation = read_csv(str(csv_path), columns=["a", "b"], header=True)
            row = conn.execute(select(relation.c.a).select_from(relation)).one()
            assert row == (1,)
    finally:
        engine.dispose()


def test_read_helpers_accept_path_like(tmp_path: Path) -> None:
    csv_path = tmp_path / "events.csv"
    csv_path.write_text("event_id\n1\n2\n", encoding="utf-8")
    other_csv = tmp_path / "more.csv"
    other_csv.write_text("event_id\n3\n", encoding="utf-8")
    parquet_path = tmp_path / "events.parquet"
    engine = create_engine("duckdb:///:memory:")
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql(
                f"COPY (SELECT 5 AS event_id) TO '{parquet_path}' (FORMAT parquet)"
            )
            relations = [
                read_csv(csv_path, columns=["event_id"], header=True),
                read_csv_auto(csv_path, columns=["event_id"]),
                read_csv([csv_path, other_csv], columns=["event_id"], header=True),
                read_parquet(parquet_path, columns=["event_id"]),
                read_parquet([parquet_path], columns=["event_id"]),
            ]
            results = [
                sorted(
                    conn.execute(select(rel.c.event_id).select_from(rel))
                    .scalars()
                    .all()
                )
                for rel in relations
            ]
            assert results == [[1, 2], [1, 2], [1, 2, 3], [5], [5]]
    finally:
        engine.dispose()
