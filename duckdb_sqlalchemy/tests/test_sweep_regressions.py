from decimal import Decimal
from typing import Any

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
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
            assert connection.execute(
                select(table.c.value).order_by(table.c.value)
            ).all() == [
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
    "method",
    ["get_multi_pk_constraint", "get_multi_indexes", "get_multi_table_options"],
)
def test_multi_reflection_honors_view_kind(engine: Any, method: str) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE regular_item (id INTEGER PRIMARY KEY)")
        connection.exec_driver_sql(
            "CREATE VIEW view_item AS SELECT id FROM regular_item"
        )
        result = getattr(inspect(connection), method)(kind=ObjectKind.VIEW)
        assert set(result) == {(None, "view_item")}


def test_copy_from_rows_uses_requested_csv_delimiter(engine: Any) -> None:
    table = Table(
        "csv_dialect", MetaData(), Column("id", Integer), Column("value", String)
    )
    with engine.begin() as connection:
        table.create(connection)
        copy_from_rows(
            connection,
            table,
            [(1, 'comma,pipe|quote"')],
            columns=["id", "value"],
            delim="|",
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
        assert (
            connection.execution_options(duckdb_arrow=False)
            .execute(text("SELECT 99"))
            .scalar()
            == 99
        )
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
        assert (
            connection.execution_options(duckdb_arrow=False)
            .execute(text("SELECT 7"))
            .scalar()
            == 7
        )
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
        assert (
            connection.execution_options(duckdb_arrow=False)
            .execute(text("SELECT 8"))
            .scalar()
            == 8
        )
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


@pytest.mark.parametrize("kind", ["table", "batch", "reader"])
def test_direct_arrow_insert_preserves_typed_values_and_rollback(kind: str) -> None:
    import pyarrow as pa
    from sqlalchemy import text

    from .. import insert_from_arrow

    arrow_table = pa.table(
        {
            "source id": pa.array([2**53 + 1, None], type=pa.int64()),
            "source amount": pa.array(
                [Decimal("1.2345"), None], type=pa.decimal128(18, 4)
            ),
            "source bytes": pa.array([b"\x00\xff", b""], type=pa.binary()),
        }
    )
    data = (
        arrow_table
        if kind == "table"
        else arrow_table.to_batches()[0]
        if kind == "batch"
        else arrow_table.to_reader()
    )
    engine = create_engine("duckdb:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'CREATE TABLE "arrow target" ("target id" BIGINT, amount DECIMAL(18, 4), bytes BLOB)'
        )
    with engine.connect() as connection:
        target = Table("arrow target", MetaData())
        insert_from_arrow(
            connection, target, data, columns=["target id", "amount", "bytes"]
        )
        assert connection.execute(text('SELECT * FROM "arrow target"')).all() == [
            (2**53 + 1, Decimal("1.2345"), b"\x00\xff"),
            (None, None, b""),
        ]
        assert (
            connection.exec_driver_sql(
                "SELECT count(*) FROM duckdb_views() WHERE view_name LIKE '__duckdb_sa_arrow_%'"
            ).scalar()
            == 0
        )
        connection.rollback()
        assert (
            connection.exec_driver_sql('SELECT count(*) FROM "arrow target"').scalar()
            == 0
        )
    engine.dispose()


@pytest.mark.parametrize("backend", ["arrow", "pandas", "ordinary"])
@pytest.mark.parametrize(
    "case", ["decimal", "binary", "timezone", "json", "null_nan", "integer"]
)
def test_bulk_insert_matches_ordinary_bindings(
    backend: str, case: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import math
    from datetime import datetime, timezone

    from sqlalchemy import JSON, DateTime, Float, LargeBinary, Numeric

    from .. import _bulk_insert

    if backend != "arrow":
        monkeypatch.setattr(
            _bulk_insert, "build_bulk_insert_arrow_table", lambda *args: None
        )
    if backend == "ordinary":
        monkeypatch.setattr(
            _bulk_insert, "build_bulk_insert_dataframe", lambda *args: None
        )
    scenarios = {
        "decimal": (
            Numeric(38, 12),
            [Decimal("12345678901234567890.123456789012"), None],
        ),
        "binary": (LargeBinary(), [b"\x00\xff", b"", None]),
        "timezone": (
            DateTime(timezone=True),
            [datetime(2026, 9, 30, 12, 34, 56, 123456, timezone.utc), None],
        ),
        "json": (JSON(), [{"nested": [1, None, "x"]}, None]),
        "null_nan": (Float(), [None, float("nan"), 1.5]),
        "integer": (BigInteger(), [2**53 + 1, None, 1]),
    }
    column_type, values = scenarios[case]
    engine = create_engine("duckdb:///:memory:")
    metadata = MetaData()
    baseline = Table(
        "ordinary_values",
        metadata,
        Column("position", Integer),
        Column("value", column_type),
    )
    candidate = Table(
        "bulk_values",
        metadata,
        Column("position", Integer),
        Column("value", column_type),
    )
    try:
        with engine.begin() as connection:
            metadata.create_all(connection)
            rows = [
                {"position": position, "value": value}
                for position, value in enumerate(values)
            ]
            connection.execution_options(duckdb_copy_threshold=0).execute(
                baseline.insert(), rows
            )
            connection.execution_options(duckdb_copy_threshold=1).execute(
                candidate.insert(), rows
            )
            expected = connection.execute(
                select(baseline).order_by(baseline.c.position)
            ).all()
            actual = connection.execute(
                select(candidate).order_by(candidate.c.position)
            ).all()
            assert len(actual) == len(expected)
            for left, right in zip(actual, expected):
                assert left[0] == right[0]
                if isinstance(right[1], float) and math.isnan(right[1]):
                    assert isinstance(left[1], float) and math.isnan(left[1])
                else:
                    assert left[1] == right[1]
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "options",
    [
        {"delimiter": "|", "quote": "'", "escape": "\\"},
        {"sep": "\t", "quote": "'"},
    ],
)
def test_copy_csv_options_round_trip(options: dict[str, str]) -> None:
    engine = create_engine("duckdb:///:memory:")
    table = Table("csv_options", MetaData(), Column("value", String))
    values = ["pipe|tab\tcomma,quote'backslash\\newline\n", "", None]
    try:
        with engine.begin() as connection:
            table.create(connection)
            copy_from_rows(
                connection,
                table,
                [(value,) for value in values],
                columns=["value"],
                **options,
            )
            assert connection.execute(select(table)).scalars().all() == values
    finally:
        engine.dispose()


def test_closing_connection_closes_arrow_batches_before_pool_reuse() -> None:
    from sqlalchemy import text

    engine = create_engine("duckdb:///:memory:")
    connection = engine.connect()
    result = connection.execution_options(duckdb_arrow=True).execute(
        text("SELECT i FROM range(100) AS t(i)")
    )
    reader = result.batches(2)
    assert reader.read_next_batch().num_rows == 2
    connection.close()
    assert reader.closed
    assert result.closed
    with engine.connect() as reused:
        assert reused.execute(text("SELECT 9")).scalar_one() == 9
    engine.dispose()


@pytest.mark.parametrize("many", [False, True])
def test_non_returning_dml_does_not_expose_duckdb_count_rows(many: bool) -> None:
    engine = create_engine("duckdb:///:memory:")
    table = Table("dml_contract", MetaData(), Column("id", Integer))
    try:
        with engine.begin() as connection:
            table.create(connection)
            rows = [{"id": 1}, {"id": 2}] if many else {"id": 1}
            inserted = connection.execute(table.insert(), rows)
            assert inserted.is_insert and not inserted.returns_rows
            assert inserted.rowcount == (2 if many else 1)
            updated = connection.execute(table.update().values(id=table.c.id + 10))
            assert not updated.returns_rows
            assert updated.rowcount == (2 if many else 1)
            assert connection.execute(select(table)).scalars().all() == (
                [11, 12] if many else [11]
            )
    finally:
        engine.dispose()


def test_generic_float_preserves_double_precision() -> None:
    from sqlalchemy import Float

    engine = create_engine("duckdb:///:memory:")
    table = Table(
        "float_precision",
        MetaData(),
        Column("value", Float(None, asdecimal=True, decimal_return_scale=7)),
    )
    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execute(table.insert(), {"value": Decimal("15.7563827")})
            assert connection.execute(select(table.c.value)).scalar_one() == Decimal(
                "15.7563827"
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("backend", ["arrow", "pandas", "ordinary"])
@settings(max_examples=25, deadline=None)
@given(
    values=st.lists(
        st.tuples(
            st.one_of(st.none(), st.integers(min_value=-(2**63), max_value=2**63 - 1)),
            st.one_of(
                st.none(),
                st.text(
                    alphabet=st.characters(blacklist_categories=("Cs",)), max_size=20
                ),
            ),
        ),
        min_size=2,
        max_size=20,
    )
)
def test_randomized_bulk_values_remain_exact(backend: str, values: Any) -> None:
    from unittest.mock import patch

    from .. import _bulk_insert

    engine = create_engine("duckdb:///:memory:")
    table = Table(
        "random_values",
        MetaData(),
        Column("position", Integer),
        Column("number", BigInteger),
        Column("label", String),
    )
    patches = []
    if backend != "arrow":
        patches.append(
            patch.object(
                _bulk_insert, "build_bulk_insert_arrow_table", return_value=None
            )
        )
    if backend == "ordinary":
        patches.append(
            patch.object(_bulk_insert, "build_bulk_insert_dataframe", return_value=None)
        )
    for replacement in patches:
        replacement.start()
    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execution_options(duckdb_copy_threshold=1).execute(
                table.insert(),
                [
                    {"position": position, "number": number, "label": label}
                    for position, (number, label) in enumerate(values)
                ],
            )
            assert connection.execute(
                select(table).order_by(table.c.position)
            ).all() == [
                (position, number, label)
                for position, (number, label) in enumerate(values)
            ]
    finally:
        for replacement in reversed(patches):
            replacement.stop()
        engine.dispose()


@pytest.mark.parametrize("matches", [0, 1, 2])
def test_returning_preserves_rows_and_affected_count(matches: int) -> None:
    engine = create_engine("duckdb:///:memory:")
    table = Table("returning_counts", MetaData(), Column("id", Integer))
    try:
        with engine.begin() as connection:
            table.create(connection)
            connection.execute(table.insert(), [{"id": 1}, {"id": 2}])
            result = connection.execute(
                table.update()
                .where(table.c.id <= matches)
                .values(id=table.c.id + 10)
                .returning(table.c.id)
            )
            assert result.rowcount == matches
            assert sorted(result.scalars().all()) == list(range(11, 11 + matches))
    finally:
        engine.dispose()


def test_reflected_sequence_default_remains_autoincrement(engine: Any) -> None:
    table = Table(
        "sequence_reflection", MetaData(), Column("id", Integer, primary_key=True)
    )
    with engine.begin() as connection:
        table.create(connection)
        reflected = Table(table.name, MetaData(), autoload_with=connection)
        assert reflected.c.id.autoincrement is True
        result = connection.execute(reflected.insert().returning(reflected.c.id))
        assert result.scalar_one() == 1

def test_arrow_returning_keeps_native_types_and_count(engine: Any) -> None:
    import pyarrow as pa
    from sqlalchemy import Numeric

    table = Table("arrow_returning", MetaData(), Column("value", Numeric(18, 4)))
    with engine.begin() as connection:
        table.create(connection)
        result = connection.execution_options(duckdb_arrow=True).execute(
            table.insert().values(value=Decimal("1.2345")).returning(table.c.value)
        )
        assert result.rowcount == 1
        assert result.arrow.schema.field("value").type == pa.decimal128(18, 4)
        assert result.arrow.to_pylist() == [{"value": Decimal("1.2345")}]

@pytest.mark.parametrize("literal", [False, True])
def test_json_paths_and_scalar_casts_use_duckdb_semantics(engine: Any, literal: bool) -> None:
    from sqlalchemy import JSON

    table = Table("json_paths", MetaData(), Column("value", JSON))
    with engine.begin() as connection:
        table.create(connection)
        connection.execute(table.insert(), {"value": {"a/b~c": [{"value": 42, "flag": True, "label": "héllo", "none": None}]}})
        expressions = [
            table.c.value[("a/b~c", 0, "value")].as_integer(),
            table.c.value[("a/b~c", 0, "flag")].as_boolean(),
            table.c.value[("a/b~c", 0, "label")].as_string(),
            table.c.value[("a/b~c", 0, "none")],
        ]
        statement = select(*expressions)
        if literal:
            result = connection.exec_driver_sql(str(statement.compile(dialect=connection.dialect, compile_kwargs={"literal_binds": True})))
        else:
            result = connection.execute(statement)
        row = result.one()
        assert tuple(row[:3]) == (42, True, "héllo")
        assert row[3] is None or row[3] == "null"
