"""Raw driver calls, case-insensitive reflection and catalog lookups."""

from typing import Any

import pytest
from sqlalchemy import (
    Column,
    Enum,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine
from sqlalchemy.sql import quoted_name

from duckdb_sqlalchemy import CursorWrapper


def test_raw_relational_calls_buffer_the_pending_result(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.exec_driver_sql("CREATE TABLE t AS SELECT range AS i FROM range(5000)")
        result = conn.exec_driver_sql("SELECT i FROM t ORDER BY i")
        assert result.fetchone() == (0,)
        raw = conn.connection.driver_connection
        assert len(raw.table("t").limit(3).fetchall()) == 3
        assert len(result.fetchall()) == 4999


def test_raw_relational_writes_block_a_retry_rollback(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE written(i INTEGER)")
    original = CursorWrapper.execute
    calls = 0

    def flaky(cursor: Any, statement: str, *args: Any) -> Any:
        nonlocal calls
        result = original(cursor, statement, *args)
        if statement == "SELECT 42":
            calls += 1
            if calls == 1:
                raise RuntimeError("HTTP Error: 503 Service Unavailable")
        return result

    with pytest.raises(Exception, match="503"):
        with engine.begin() as conn:
            conn.connection.driver_connection.table("written").insert([1])
            monkeypatch.setattr(CursorWrapper, "execute", flaky)
            conn.execution_options(duckdb_retry_count=1).exec_driver_sql("SELECT 42")
    # The retry was declined instead of rolling back the raw write and
    # committing an empty transaction.
    assert calls == 1


def test_reflection_matches_names_case_insensitively(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql('CREATE TABLE "Users"(id INTEGER, name VARCHAR)')
        assert inspect(conn).has_table("users")
        assert inspect(conn).has_table("USERS")
        users = Table("users", MetaData(), autoload_with=conn)
        assert [column.name for column in users.columns] == ["id", "name"]
        metadata = MetaData()
        Table("users", metadata, Column("id", Integer))
        metadata.create_all(conn)  # checkfirst sees the existing table


def test_index_expressions_recreate_from_reflection(engine: Engine) -> None:
    metadata = MetaData()
    table = Table("t", metadata, Column("name", String), Column("it's", Integer))
    Index("ix_expr", func.lower(table.c.name), table.c["it's"])
    Index("ix_literal", func.coalesce(table.c.name, "a,b"))
    with engine.begin() as conn:
        metadata.create_all(conn)
        indexes = {ix["name"]: ix for ix in inspect(conn).get_indexes("t")}
        assert indexes["ix_expr"]["column_names"] == [None, "it's"]
        assert indexes["ix_expr"]["expressions"] == ['(lower("name"))', '"it\'s"']
        assert indexes["ix_literal"]["expressions"] == ["(COALESCE(\"name\", 'a,b'))"]

        reflected = Table("t", MetaData(), autoload_with=conn)
        table.drop(conn)
        reflected.create(conn)
        recreated = {ix["name"] for ix in inspect(conn).get_indexes("t")}
        assert recreated == {"ix_expr", "ix_literal"}


def test_view_definition_follows_schema_and_temp_resolution() -> None:
    # One transaction can only write to one attached database.
    engine = create_engine("duckdb:///:memory:", isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.exec_driver_sql("ATTACH ':memory:' AS db2")
        conn.exec_driver_sql("CREATE VIEW v AS SELECT 1 AS from_memory")
        conn.exec_driver_sql("CREATE VIEW db2.main.v AS SELECT 2 AS from_db2")
        conn.exec_driver_sql("CREATE VIEW db2.main.only2 AS SELECT 3 AS x")
        conn.exec_driver_sql("CREATE TEMP VIEW v AS SELECT 0 AS from_temp")
        inspector = inspect(conn)
        assert "from_db2" in inspector.get_view_definition("v", schema="db2.main")
        assert "AS x" in inspector.get_view_definition("only2", schema="db2.main")
        assert "from_temp" in inspector.get_view_definition("v")
        assert "from_memory" in inspector.get_view_definition("v", schema="main")
    engine.dispose()


def test_enum_types_in_attached_schemas_drop_and_recreate() -> None:
    engine = create_engine("duckdb:///:memory:", isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.exec_driver_sql("ATTACH ':memory:' AS db2")
        conn.exec_driver_sql("CREATE SCHEMA db2.s")
    metadata = MetaData()
    Table("t", metadata, Column("e", Enum("x", "y", name="myenum", schema="db2.s")))
    metadata.create_all(engine)
    with engine.connect() as conn:
        assert engine.dialect.has_type(conn, "myenum", schema="db2.s")
    metadata.drop_all(engine)
    with engine.connect() as conn:
        assert not engine.dialect.has_type(conn, "myenum", schema="db2.s")
    metadata.create_all(engine)
    engine.dispose()


def test_duckdb_reserved_words_do_not_leak_into_postgresql(engine: Engine) -> None:
    table = Table("t", MetaData(), Column("qualify", Integer))
    assert '"qualify"' in str(select(table).compile(dialect=engine.dialect))
    assert '"qualify"' not in str(select(table).compile(dialect=postgresql.dialect()))


def test_quoted_schema_name_with_a_dot_stays_one_identifier(engine: Engine) -> None:
    table = Table(
        "t", MetaData(), Column("a", Integer), schema=quoted_name("my.schema", True)
    )
    assert 'FROM "my.schema".t' in str(select(table).compile(dialect=engine.dialect))
    with engine.begin() as conn:
        conn.execute(text('CREATE SCHEMA "my.schema"'))
        table.create(conn)
        assert inspect(conn).has_table("t", schema=quoted_name("my.schema", True))
