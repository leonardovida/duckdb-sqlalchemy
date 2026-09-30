from decimal import Decimal
from typing import Any

import duckdb
import pytest
from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
    select,
)
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope

from duckdb_sqlalchemy import ConnectionWrapper, Dialect, copy_from_rows
from duckdb_sqlalchemy.__init__ import DuckDBNumeric
from duckdb_sqlalchemy.alembic_impl import DuckDBImpl
from duckdb_sqlalchemy.datatypes import Struct


def test_interleaving_propagates_fetch_failure_before_replacing_result() -> None:
    failure = duckdb.InvalidInputException("stream failed")

    class Native:
        description = [("value", "INTEGER")]
        rowcount = -1

        def __init__(self) -> None:
            self.calls: list[str] = []

        def fetchall(self) -> Any:
            raise failure

        def execute(self, sql: str) -> None:
            self.calls.append(sql)

    native = Native()
    connection = ConnectionWrapper(native)  # type: ignore[arg-type]
    original = connection.cursor()
    connection._set_pending_cursor(original)
    with pytest.raises(duckdb.InvalidInputException) as caught:
        connection.cursor().execute("SELECT 42")
    assert caught.value is failure
    assert native.calls == []


def test_bulk_pandas_path_preserves_mixed_numeric_integer_precision() -> None:
    pytest.importorskip("pandas")
    from duckdb_sqlalchemy._bulk_insert import build_bulk_insert_dataframe

    big = 2**53 + 1
    data = build_bulk_insert_dataframe([(big,), (1.0,)], ["value"])
    # Declining an unsafe optimization is preferable to changing the value.
    if data is not None:
        assert data["value"][0] == big
        assert int(data["value"][0]) == big


def test_bulk_and_regular_insert_preserve_mixed_numeric_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pandas")
    import duckdb_sqlalchemy._bulk_insert as bulk

    monkeypatch.setattr(bulk, "build_bulk_insert_arrow_table", lambda *args: None)
    engine = create_engine("duckdb:///:memory:")
    table = Table("mixed_numeric", MetaData(), Column("value", BigInteger))
    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execution_options(duckdb_copy_threshold=1).execute(
                table.insert(), [{"value": 2**53 + 1}, {"value": 1.0}]
            )
            assert connection.execute(select(table.c.value).order_by(table.c.value)).all() == [
                (1,),
                (2**53 + 1,),
            ]
    finally:
        engine.dispose()


def _alembic() -> DuckDBImpl:
    from alembic.migration import MigrationContext

    return MigrationContext.configure(dialect=Dialect()).impl  # type: ignore[return-value]


def test_alembic_preserves_spaces_inside_nested_field_names() -> None:
    old = Column("payload", Struct({"a b": Integer}))
    new = Column("payload", Struct({"ab": Integer}))
    assert _alembic().compare_type(old, new)


def test_alembic_distinguishes_literal_defaults_from_expressions() -> None:
    old = Column("value", String)
    new = Column("value", String)
    assert _alembic().compare_server_default(old, new, "(1 + 1)", "'1+1'")


def test_numeric_honors_explicit_decimal_return_scale() -> None:
    numeric = DuckDBNumeric(precision=12, scale=2, decimal_return_scale=4)
    processor = numeric.result_processor(Dialect(), None)
    assert processor(1.2345) == Decimal("1.2345")


def test_foreign_key_search_path_preserves_resolved_parent_schema(engine: Any) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        connection.exec_driver_sql("CREATE SCHEMA aux")
        connection.exec_driver_sql("CREATE TABLE aux.parent (id INTEGER PRIMARY KEY)")
        connection.exec_driver_sql(
            "CREATE TABLE aux.child (parent_id INTEGER REFERENCES aux.parent(id))"
        )
        connection.exec_driver_sql("SET search_path = 'main,aux'")
        foreign_key = inspect(connection).get_foreign_keys("child")[0]
        assert foreign_key["referred_schema"] == "aux"
        assert foreign_key["referred_table"] == "parent"


def test_multi_reflection_honors_temporary_scope(engine: Any) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE regular_item (id INTEGER PRIMARY KEY)")
        connection.exec_driver_sql("CREATE TEMP TABLE temp_item (value VARCHAR)")
        inspector = inspect(connection)
        assert set(inspector.get_multi_columns(scope=ObjectScope.TEMPORARY)) == {
            (None, "temp_item")
        }
        assert set(inspector.get_multi_indexes(scope=ObjectScope.TEMPORARY)) == {
            (None, "temp_item")
        }


@pytest.mark.parametrize(
    "method", ["get_multi_pk_constraint", "get_multi_indexes", "get_multi_table_options"]
)
def test_multi_reflection_honors_view_kind(engine: Any, method: str) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE regular_item (id INTEGER PRIMARY KEY)")
        connection.exec_driver_sql("CREATE VIEW view_item AS SELECT id FROM regular_item")
        result = getattr(inspect(connection), method)(kind=ObjectKind.VIEW)
        assert set(result) == {(None, "view_item")}


def test_copy_from_rows_uses_requested_csv_delimiter(engine: Any) -> None:
    table = Table(
        "csv_dialect", MetaData(), Column("id", Integer), Column("value", String)
    )
    with engine.begin() as connection:
        table.create(connection)
        copy_from_rows(
            connection, table, [(1, 'comma,pipe|quote"')], columns=["id", "value"], delim="|"
        )
        assert connection.execute(select(table)).all() == [(1, 'comma,pipe|quote"')]


@pytest.mark.parametrize(
    "statement",
    [
        "/* query tag */ UPDATE counts SET value = value + 1",
        "WITH ids AS (SELECT 1 AS id) UPDATE counts SET value = value + 1 "
        "WHERE id IN (SELECT id FROM ids)",
    ],
)
def test_dml_rowcount_with_comments_and_ctes(engine: Any, statement: str) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE counts (id INTEGER, value INTEGER)")
        connection.exec_driver_sql("INSERT INTO counts VALUES (1, 0)")
        assert connection.exec_driver_sql(statement).rowcount == 1


def test_arrow_batches_own_the_connection_until_closed() -> None:
    from sqlalchemy import text
    from sqlalchemy.exc import InvalidRequestError

    engine = create_engine("duckdb:///:memory:")
    with engine.connect() as connection:
        result = connection.execution_options(duckdb_arrow=True).execute(
            text("SELECT i FROM range(5) AS t(i)")
        )
        reader = result.batches(2)
        assert reader.read_next_batch().column(0).to_pylist() == [0, 1]
        with pytest.raises(InvalidRequestError, match="Consume or close"):
            connection.execute(text("SELECT 99"))
        with pytest.raises(InvalidRequestError, match="owns"):
            result.fetchone()
        assert [batch.column(0).to_pylist() for batch in reader] == [[2, 3], [4]]
        assert reader.closed
        assert connection.execution_options(duckdb_arrow=False).execute(text("SELECT 99")).scalar() == 99
    engine.dispose()


def test_arrow_batches_early_close_releases_the_connection() -> None:
    from sqlalchemy import text

    engine = create_engine("duckdb:///:memory:")
    with engine.connect() as connection:
        result = connection.execution_options(duckdb_arrow=True).execute(
            text("SELECT i FROM range(100) AS t(i)")
        )
        with result.batches(2) as reader:
            assert reader.read_next_batch().num_rows == 2
        reader.close()
        assert reader.closed
        assert result.closed
        assert connection.execution_options(duckdb_arrow=False).execute(text("SELECT 7")).scalar() == 7
    engine.dispose()


def test_result_close_closes_its_arrow_reader() -> None:
    from sqlalchemy import text

    engine = create_engine("duckdb:///:memory:")
    with engine.connect() as connection:
        result = connection.execution_options(duckdb_arrow=True).execute(
            text("SELECT i FROM range(100) AS t(i)")
        )
        reader = result.batches(2)
        result.close()
        assert reader.closed
        assert connection.execution_options(duckdb_arrow=False).execute(text("SELECT 8")).scalar() == 8
    engine.dispose()


def test_temp_scope_does_not_hide_regular_relations() -> None:
    engine = create_engine("duckdb:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE shadowed (regular_value INTEGER)")
        connection.exec_driver_sql("CREATE TEMP TABLE shadowed (temp_value VARCHAR)")
        inspector = inspect(connection)
        regular = inspector.get_multi_columns(scope=ObjectScope.DEFAULT)
        temporary = inspector.get_multi_columns(scope=ObjectScope.TEMPORARY)
        assert regular[(None, "shadowed")][0]["name"] == "regular_value"
        assert temporary[(None, "shadowed")][0]["name"] == "temp_value"
    engine.dispose()


def test_exhausted_cursor_cannot_read_another_cursors_rows() -> None:
    wrapper = ConnectionWrapper(duckdb.connect())
    first = wrapper.cursor()
    first.execute("SELECT 1")
    assert first.fetchall() == [(1,)]
    second = wrapper.cursor()
    second.execute("SELECT 2")
    assert first.fetchall() == []
    assert second.fetchall() == [(2,)]
    wrapper.close()
