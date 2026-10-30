"""Reflection reports catalog spellings and recreates generated and sequence columns."""

from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    Computed,
    Integer,
    MetaData,
    Numeric,
    Sequence,
    String,
    Table,
    Text,
    insert,
    inspect,
    literal,
    select,
    text,
)
from sqlalchemy.exc import CompileError

from .conftest import EngineFactory


def _reflect(engine, *names, schema=None):  # type: ignore[no-untyped-def]
    metadata = MetaData()
    with engine.connect() as conn:
        metadata.reflect(conn, only=list(names), schema=schema)
    return metadata


def test_index_and_foreign_key_names_use_catalog_spelling(
    make_engine: EngineFactory, db_path: Path, tmp_path: Path
) -> None:
    source = make_engine(db_path)
    with source.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE Customers (Id INTEGER PRIMARY KEY)")
        conn.exec_driver_sql("CREATE TABLE items (Sku VARCHAR, Qty INTEGER)")
        conn.exec_driver_sql("CREATE INDEX ix_items ON items (sku, QTY)")
        conn.exec_driver_sql(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER "
            "REFERENCES customers (id))"
        )
    with source.connect() as conn:
        inspector = inspect(conn)
        assert inspector.get_indexes("items")[0]["column_names"] == ["Sku", "Qty"]
        foreign_key = inspector.get_foreign_keys("orders")[0]
        assert foreign_key["referred_table"] == "Customers"
        assert foreign_key["referred_columns"] == ["Id"]

    metadata = _reflect(source, "Customers", "items", "orders")
    assert sorted(metadata.tables) == ["Customers", "items", "orders"]
    target = make_engine(tmp_path / "copy.duckdb")
    with target.begin() as conn:
        metadata.create_all(conn)
    with target.connect() as conn:
        inspector = inspect(conn)
        assert inspector.get_indexes("items") == [
            {"name": "ix_items", "column_names": ["Sku", "Qty"], "unique": False}
        ]
        assert inspector.get_foreign_keys("orders")[0]["referred_columns"] == ["Id"]


def test_schema_and_database_names_match_case_insensitively(
    make_engine: EngineFactory, db_path: Path, tmp_path: Path
) -> None:
    engine = make_engine(db_path)
    attached = (tmp_path / "other.duckdb").as_posix()
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA Sales")
        conn.exec_driver_sql("CREATE TABLE Sales.orders (id INTEGER)")
        conn.exec_driver_sql("CREATE SEQUENCE OrderSeq")
    with engine.begin() as conn:
        # one transaction writes to one database
        conn.exec_driver_sql(f"ATTACH '{attached}' AS MyDb")
        conn.exec_driver_sql("CREATE TABLE MyDb.main.items (id INTEGER)")
    with engine.connect() as conn:
        inspector = inspect(conn)
        assert inspector.has_schema("sales")
        assert inspector.has_table("orders", schema="sales")
        assert inspector.get_table_names(schema="sales") == ["orders"]
        assert inspector.has_table("items", schema="mydb.main")
        assert inspector.has_sequence("orderseq")

    metadata = MetaData()
    Table("orders", metadata, Column("id", Integer), schema="sales")
    Sequence("orderseq", metadata=metadata)
    with engine.begin() as conn:
        metadata.create_all(conn)  # finds both instead of creating them again
        conn.execute(text("INSERT INTO sales.orders VALUES (1)"))
        assert conn.scalar(text("SELECT count(*) FROM Sales.orders")) == 1


def test_generated_columns_create_and_reflect(
    make_engine: EngineFactory, db_path: Path, tmp_path: Path
) -> None:
    metadata = MetaData()
    Table(
        "g",
        metadata,
        Column("x", Integer),
        Column("doubled", Integer, Computed("x * 2")),
        Column("label", String, Computed("'n' || CAST(x AS VARCHAR)", persisted=False)),
    )
    source = make_engine(db_path)
    with source.begin() as conn:
        metadata.create_all(conn)
        conn.execute(text("INSERT INTO g (x) VALUES (5)"))
    with source.connect() as conn:
        columns = {column["name"]: column for column in inspect(conn).get_columns("g")}
    assert columns["doubled"]["default"] is None
    assert columns["doubled"]["computed"] == {"sqltext": "(x * 2)", "persisted": False}
    assert "computed" not in columns["x"]

    reflected = _reflect(source, "g")
    target = make_engine(tmp_path / "copy.duckdb")
    with target.begin() as conn:
        reflected.create_all(conn)
        conn.execute(insert(reflected.tables["g"]).values(x=7))
        assert conn.execute(text("SELECT * FROM g")).one() == (7, 14, "n7")


def test_stored_generated_columns_raise() -> None:
    metadata = MetaData()
    Table(
        "g", metadata, Column("x", Integer), Column("y", Integer, Computed("x", True))
    )
    from sqlalchemy.schema import CreateTable

    from duckdb_sqlalchemy import Dialect

    with pytest.raises(CompileError, match="only virtual generated columns"):
        CreateTable(metadata.tables["g"]).compile(dialect=Dialect())


def test_reflected_implicit_sequence_is_created_with_the_table(
    make_engine: EngineFactory, db_path: Path, tmp_path: Path
) -> None:
    metadata = MetaData()
    Table(
        "users", metadata, Column("id", Integer, primary_key=True), Column("x", Integer)
    )
    source = make_engine(db_path)
    with source.begin() as conn:
        metadata.create_all(conn)

    reflected = _reflect(source, "users")
    target = make_engine(tmp_path / "copy.duckdb")
    with target.begin() as conn:
        reflected.create_all(conn)
        conn.execute(insert(reflected.tables["users"]).values(x=1))
        conn.execute(insert(reflected.tables["users"]).values(x=2))
        assert conn.execute(text("SELECT id, x FROM users ORDER BY id")).all() == [
            (1, 1),
            (2, 2),
        ]


def test_numeric_literals_keep_decimal_precision(
    make_engine: EngineFactory, db_path: Path
) -> None:
    value = Decimal("0.000000123456789012345678901234")
    metadata = MetaData()
    table = Table(
        "n",
        metadata,
        Column("id", Integer),
        Column("d", Numeric(38, 30), server_default=literal(value, Numeric(38, 30))),
    )
    engine = make_engine(db_path)
    with engine.begin() as conn:
        metadata.create_all(conn)
        statement = insert(table).values(id=1, d=literal(value, Numeric(38, 30)))
        conn.exec_driver_sql(
            str(statement.compile(engine, compile_kwargs={"literal_binds": True}))
        )
        conn.execute(insert(table).values(id=2))
        assert conn.execute(select(table.c.d).order_by(table.c.id)).scalars().all() == [
            value,
            value,
        ]


@pytest.mark.parametrize("column_type", [String, Text])
def test_alembic_sees_no_diff_for_string_defaults_and_text(
    make_engine: EngineFactory, db_path: Path, column_type: type
) -> None:
    pytest.importorskip("alembic")
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    metadata = MetaData()
    Table(
        "t",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("label", column_type, server_default="none"),
        Column("quoted", column_type, server_default="it's"),
    )
    engine = make_engine(db_path)
    with engine.begin() as conn:
        metadata.create_all(conn)
        context = MigrationContext.configure(
            conn, opts={"compare_type": True, "compare_server_default": True}
        )
        assert compare_metadata(context, metadata) == []

        changed = MetaData()
        Table(
            "t",
            changed,
            Column("id", Integer, primary_key=True),
            Column("label", column_type, server_default="other"),
            Column("quoted", column_type, server_default="it's"),
        )
        diffs = compare_metadata(context, changed)
        assert [diff[0][0] for diff in diffs] == ["modify_default"]
