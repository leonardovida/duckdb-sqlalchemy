from contextlib import nullcontext
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import (
    ARRAY,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    func,
    inspect,
    select,
)
from sqlalchemy.types import TypeDecorator

from duckdb_sqlalchemy import CursorWrapper, insert_from_arrow
from duckdb_sqlalchemy.datatypes import Map


class Lower(TypeDecorator):
    impl = String
    cache_ok = True

    def bind_expression(self, value: Any) -> Any:
        return func.lower(value)


@pytest.mark.parametrize("threshold", [0, 1])
def test_bulk_bind_expression_parity(engine: Any, threshold: int) -> None:
    table = Table("lowered", MetaData(), Column("value", Lower()))
    with engine.begin() as connection:
        table.create(connection)
        connection.execution_options(duckdb_copy_threshold=threshold).execute(
            table.insert(), [{"value": "ABC"}, {"value": "DEF"}]
        )
        assert connection.execute(select(table.c.value)).scalars().all() == [
            "abc",
            "def",
        ]


@pytest.mark.parametrize("backend", ["ordinary", "arrow", "pandas"])
def test_map_bulk_parity(engine: Any, backend: str) -> None:
    table = Table(
        "maps", MetaData(), Column("id", Integer), Column("value", Map(String, Integer))
    )
    values = [{"a": 1}, {}, None]
    with engine.begin() as connection:
        table.create(connection)
        with (
            patch(
                "duckdb_sqlalchemy._bulk_insert.build_bulk_insert_arrow_table",
                return_value=None,
            )
            if backend == "pandas"
            else nullcontext()
        ):
            connection.execution_options(
                duckdb_copy_threshold=0 if backend == "ordinary" else 1
            ).execute(
                table.insert(),
                [{"id": i, "value": value} for i, value in enumerate(values)],
            )
        assert (
            connection.execute(select(table.c.value).order_by(table.c.id))
            .scalars()
            .all()
            == values
        )


def test_native_write_prevents_retry_rollback(engine: Any, monkeypatch: Any) -> None:
    original = CursorWrapper.execute
    failure = RuntimeError("HTTP Error: 503 Service Unavailable")
    calls = 0

    def flaky(cursor: Any, statement: str, *args: Any) -> None:
        nonlocal calls
        original(cursor, statement, *args)
        if statement == "SELECT 42":
            calls += 1
            if calls == 1:
                raise failure

    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE written(i INTEGER)")
    with engine.begin() as connection:
        # Retaining the native method before begin must still count its call.
        connection.connection.driver_connection.execute(
            "INSERT INTO written VALUES (1)"
        )
        monkeypatch.setattr(CursorWrapper, "execute", flaky)
        with pytest.raises(RuntimeError) as caught:
            connection.execution_options(duckdb_retry_count=1).exec_driver_sql(
                "SELECT 42"
            )
        assert caught.value is failure
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT * FROM written").all() == [(1,)]
    assert calls == 1


def test_bound_query_side_effect_is_not_retried(engine: Any, monkeypatch: Any) -> None:
    original = CursorWrapper.execute
    failure = RuntimeError("HTTP Error: 503 Service Unavailable")
    statement = "SELECT * FROM query($1)"
    calls = 0

    def flaky(cursor: Any, sql: str, *args: Any) -> None:
        nonlocal calls
        original(cursor, sql, *args)
        if sql == statement:
            calls += 1
            if calls == 1:
                raise failure

    with engine.connect() as connection:
        connection.exec_driver_sql("CREATE SEQUENCE ids")
        connection.commit()
        monkeypatch.setattr(CursorWrapper, "execute", flaky)
        with pytest.raises(RuntimeError) as caught:
            connection.execution_options(duckdb_retry_count=1).exec_driver_sql(
                statement, ("SELECT nextval('ids')",)
            )
        assert caught.value is failure
        connection.rollback()
        assert connection.exec_driver_sql("SELECT currval('ids')").scalar_one() == 1
    assert calls == 1


@pytest.mark.parametrize("copy_from_model", [False, True])
@pytest.mark.parametrize("rename", [False, True])
def test_alembic_batch_preserves_sequence(
    engine: Any, copy_from_model: bool, rename: bool
) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    table = Table(
        "items",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("sku", String),
    )
    with engine.connect() as connection:
        table.create(connection)
        connection.execute(table.insert(), [{"sku": "a"}, {"sku": "b"}])
        connection.commit()
        with Operations(MigrationContext.configure(connection)).batch_alter_table(
            table.name, recreate="always", copy_from=table if copy_from_model else None
        ) as batch:
            batch.create_unique_constraint("uq_items_sku", ["sku"])
            if rename:
                batch.alter_column(
                    "id", new_column_name="identity", existing_type=Integer
                )
        connection.commit()
        connection.exec_driver_sql("INSERT INTO items(sku) VALUES ('c')")
        assert connection.exec_driver_sql("SELECT * FROM items ORDER BY 1").all() == [
            (1, "a"),
            (2, "b"),
            (3, "c"),
        ]
        assert inspect(connection).get_sequence_names() == ["items_id_seq"]
        assert table.info == {}
        table.drop(connection)
        assert inspect(connection).get_sequence_names() == []


@pytest.mark.parametrize("catalog", ["other.db", "other,db", 'other"db'])
def test_quoted_search_path_reflection(engine: Any, catalog: str) -> None:
    with engine.connect() as connection:
        quote = connection.dialect.identifier_preparer.quote_identifier
        connection.exec_driver_sql("CREATE SCHEMA first")
        connection.commit()
        connection.exec_driver_sql(f"ATTACH ':memory:' AS {quote(catalog)}")
        connection.commit()
        connection.exec_driver_sql(f"CREATE TABLE {quote(catalog)}.main.t(v INTEGER)")
        connection.commit()
        connection.exec_driver_sql(
            "SET search_path = ?", (f"first,{quote(catalog)}.main",)
        )
        assert list(connection.exec_driver_sql("SELECT * FROM t").keys()) == ["v"]
        assert inspect(connection).has_table("t")
        assert inspect(connection).get_columns("t")[0]["name"] == "v"


@pytest.mark.parametrize(
    "ddl", ["INTEGER[3]", "INTEGER[3][2]", "INTEGER[3][]", "INTEGER[][3]"]
)
def test_fixed_array_reflection_preserves_ddl(engine: Any, ddl: str) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(f"CREATE TABLE arrays(v {ddl})")
        reflected = Table("arrays", MetaData(), autoload_with=connection)
        assert reflected.c.v.type.compile(dialect=engine.dialect) == ddl
        copied = reflected.to_metadata(MetaData(), name="copied_arrays")
        copied.create(connection)
        assert (
            inspect(connection)
            .get_columns(copied.name)[0]["type"]
            .compile(dialect=engine.dialect)
            == ddl
        )


def test_fixed_array_values_cache_and_indexing(engine: Any) -> None:
    from duckdb_sqlalchemy.datatypes import FixedArray

    three = FixedArray(Integer, 3)
    assert three._static_cache_key != FixedArray(Integer, 2)._static_cache_key
    table = Table("vectors", MetaData(), Column("v", three))
    with engine.begin() as connection:
        table.create(connection)
        connection.execute(table.insert(), [{"v": [1, 2, 3]}, {"v": None}])
        assert connection.execute(select(table.c.v)).scalars().all() == [
            (1, 2, 3),
            None,
        ]
        assert connection.execute(select(table.c.v[1])).scalars().all() == [1, None]
    with engine.begin() as connection:
        with pytest.raises(Exception, match="length 2"):
            connection.execute(table.insert(), {"v": [1, 2]})


@pytest.mark.parametrize(
    "mode, expected",
    [("ignore", [(1, "old"), (2, "new")]), ("replace", [(1, "changed"), (2, "new")])],
)
def test_arrow_conflict_modes(engine: Any, mode: str, expected: Any) -> None:
    pa = pytest.importorskip("pyarrow")
    table = Table(
        "conflicts",
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("value", String),
    )
    with engine.begin() as connection:
        table.create(connection)
        connection.execute(table.insert(), {"id": 1, "value": "old"})
        insert_from_arrow(
            connection,
            table,
            pa.table({"id": [1, 2], "value": ["changed", "new"]}),
            on_conflict=mode,
        )
        assert connection.execute(select(table).order_by(table.c.id)).all() == expected
        assert (
            connection.exec_driver_sql(
                "SELECT count(*) FROM duckdb_views() WHERE view_name LIKE '__duckdb_sa_arrow_%'"
            ).scalar_one()
            == 0
        )


def test_arrow_returning_does_not_buffer_python_rows(
    engine: Any, monkeypatch: Any
) -> None:
    original = CursorWrapper._store_buffered_result

    def store(cursor: Any, rows: Any, description: Any, rowcount: int) -> None:
        assert not isinstance(rows, zip), (
            "Arrow RETURNING must not copy every row into Python"
        )
        original(cursor, rows, description, rowcount)

    table = Table("returned", MetaData(), Column("value", Integer))
    with engine.begin() as connection:
        table.create(connection)
        monkeypatch.setattr(CursorWrapper, "_store_buffered_result", store)
        result = connection.execution_options(duckdb_arrow=True).execute(
            table.insert()
            .from_select(["value"], select(func.unnest([1, 2, 3])))
            .returning(table.c.value)
        )
        assert result.rowcount == 3
        assert result.arrow.to_pylist() == [{"value": 1}, {"value": 2}, {"value": 3}]


def test_arrow_guard_keeps_positional_float_column_precision() -> None:
    from duckdb_sqlalchemy._bulk_insert import build_bulk_insert_arrow_table

    # The unsafe floating column is not the first column.
    assert (
        build_bulk_insert_arrow_table([(1, 2**53 + 1), (2, 1.0)], ["id", "value"])
        is None
    )
    safe = build_bulk_insert_arrow_table([(1, 2**53 + 1), (2, None)], ["id", "value"])
    assert safe.column(1)[0].as_py() == 2**53 + 1


def test_map_list_keeps_regular_bind_processing(engine: Any) -> None:
    table = Table("map_lists", MetaData(), Column("v", ARRAY(Map(String, Integer))))
    values = [[{"a": 1}, {}], None]
    with engine.begin() as connection:
        table.create(connection)
        connection.execution_options(duckdb_copy_threshold=1).execute(
            table.insert(), [{"v": value} for value in values]
        )
        assert connection.execute(select(table.c.v)).scalars().all() == values


def test_offline_batch_reuses_existing_sequence() -> None:
    import io

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="duckdb", opts={"as_sql": True, "output_buffer": buffer}
    )
    table = Table(
        "items",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("sku", String),
    )
    with Operations(context).batch_alter_table(
        "items", recreate="always", copy_from=table
    ) as batch:
        batch.create_unique_constraint("uq_sku", ["sku"])
    sql = buffer.getvalue()
    assert "SERIAL" not in sql
    assert "nextval('items_id_seq')" in sql
    assert "CREATE SEQUENCE" not in sql
    assert "DROP SEQUENCE" not in sql
    assert table.info == {}


def test_fixed_array_alembic_render_and_comparison(engine: Any) -> None:
    from alembic.autogenerate import (
        compare_metadata,
        produce_migrations,
        render_python_code,
    )
    from alembic.migration import MigrationContext

    from duckdb_sqlalchemy.datatypes import FixedArray

    existing = MetaData()
    Table(
        "vectors",
        existing,
        Column("v", FixedArray(Integer, 3)),
        Column("history", ARRAY(FixedArray(Integer, 2))),
    )
    desired = MetaData()
    Table(
        "vectors",
        desired,
        Column("v", FixedArray(Integer, 4)),
        Column("history", ARRAY(FixedArray(Integer, 2))),
    )
    with engine.begin() as connection:
        context = MigrationContext.configure(connection)
        migration = produce_migrations(context, existing)
        code = render_python_code(migration.upgrade_ops, migration_context=context)
        assert "duckdb_sqlalchemy.datatypes.FixedArray(sa.Integer(), 3)" in code
        compile("def upgrade():\n" + code, "<migration>", "exec")
        existing.create_all(connection)
        assert compare_metadata(context, existing) == []
        assert compare_metadata(context, desired)[0][0][0] == "modify_type"
