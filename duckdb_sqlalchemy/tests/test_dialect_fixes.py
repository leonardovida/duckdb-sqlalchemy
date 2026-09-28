from decimal import Decimal
from typing import Any, Iterable

import duckdb
import pytest
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    func,
    inspect,
    select,
    text,
    type_coerce,
)
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError

from duckdb_sqlalchemy import Dialect


def test_numeric_round_trips_exact_decimal(engine: Engine) -> None:
    metadata = MetaData()
    table = Table(
        "decimals",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("exact", Numeric(38, 10)),
        Column("approx", Float),
        Column("as_float", Numeric(10, 2, asdecimal=False)),
    )
    metadata.create_all(engine)
    value = Decimal("12345678901234567890.1234567891")

    with engine.begin() as conn:
        conn.execute(
            table.insert(),
            {"id": 1, "exact": value, "approx": 1.5, "as_float": Decimal("1.25")},
        )
        row = conn.execute(
            select(table.c.exact, table.c.approx, table.c.as_float)
        ).one()
        averaged = conn.execute(
            select(type_coerce(func.avg(table.c.exact), Numeric(38, 10)))
        ).scalar_one()

    assert row.exact == value
    assert isinstance(row.exact, Decimal)
    assert row.approx == 1.5
    assert isinstance(row.approx, float)
    assert row.as_float == 1.25
    assert isinstance(row.as_float, float)
    # avg(DECIMAL) is DOUBLE in DuckDB; Numeric still promises a Decimal.
    assert isinstance(averaged, Decimal)


def test_io_error_mentioning_socket_timeout_keeps_memory_database() -> None:
    engine = create_engine("duckdb:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE keep_me (id INTEGER)"))

    with pytest.raises(sa_exc.DBAPIError) as captured:
        with engine.connect() as conn:
            conn.execute(
                text("SELECT * FROM read_csv('/nonexistent/socket_timeout.csv')")
            )

    assert isinstance(captured.value.orig, duckdb.IOException)
    assert not captured.value.connection_invalidated
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM keep_me")).scalar_one() == 0


def test_is_disconnect_ignores_io_and_http_errors() -> None:
    dialect = Dialect()
    assert not dialect.is_disconnect(
        duckdb.IOException("IO Error: connection timed out on socket"), None, None
    )
    assert not dialect.is_disconnect(
        duckdb.HTTPException("HTTP Error: timeout"), None, None
    )
    assert dialect.is_disconnect(
        duckdb.ConnectionException("Connection Error: Connection already closed!"),
        None,
        None,
    )
    assert dialect.is_disconnect(
        duckdb.OperationalError("connection reset by peer"), None, None
    )


def _run_each(conn: Any, statements: Iterable[str]) -> None:
    # DuckDB allows writing to one attached database per transaction.
    for statement in statements:
        conn.execute(text(statement))
        conn.commit()


@pytest.fixture
def multi_catalog_engine(engine: Engine) -> Engine:
    with engine.connect() as conn:
        _run_each(
            conn,
            [
                "ATTACH ':memory:' AS other",
                "CREATE SCHEMA aux",
                "CREATE TABLE aux.aux_t (id INTEGER)",
                "CREATE TABLE main_t (id INTEGER PRIMARY KEY, v VARCHAR)",
                "CREATE INDEX main_t_v ON main_t (v)",
                "CREATE VIEW main_v AS SELECT 1 AS x",
                "CREATE TABLE other.main_t (other_id INTEGER)",
                "CREATE TABLE other.only_in_other (id INTEGER PRIMARY KEY)",
                "CREATE VIEW other.other_v AS SELECT 1 AS x",
                "CREATE SCHEMA s",
                "CREATE SCHEMA other.s",
                "CREATE TABLE s.t (a INTEGER)",
                "CREATE TABLE other.s.t (b INTEGER)",
                "CREATE TABLE other.s.only_other_s (b INTEGER)",
            ],
        )
    return engine


def test_unqualified_reflection_is_scoped_to_current_schema(
    multi_catalog_engine: Engine,
) -> None:
    with multi_catalog_engine.connect() as conn:
        inspector = inspect(conn)
        assert inspector.get_table_names() == ["main_t"]
        assert inspector.get_view_names() == ["main_v"]
        assert inspector.has_table("main_t")
        assert not inspector.has_table("only_in_other")
        assert not inspector.has_table("aux_t")
        for getter in (
            inspector.get_columns,
            inspector.get_pk_constraint,
            inspector.get_foreign_keys,
            inspector.get_indexes,
            inspector.get_unique_constraints,
        ):
            with pytest.raises(NoSuchTableError):
                getter("only_in_other")
        assert [col["name"] for col in inspector.get_columns("main_t")] == ["id", "v"]
        assert inspector.get_pk_constraint("main_t")["constrained_columns"] == ["id"]
        assert [ix["name"] for ix in inspector.get_indexes("main_t")] == ["main_t_v"]

        metadata = MetaData()
        metadata.reflect(bind=conn)
        assert sorted(metadata.tables) == ["main_t"]
        assert list(metadata.tables["main_t"].c.keys()) == ["id", "v"]

        # Explicitly qualified lookups still reach the attached database.
        assert inspector.has_table("only_in_other", schema="other.main")
        assert inspector.get_pk_constraint("only_in_other", schema="other.main")[
            "constrained_columns"
        ] == ["id"]
        assert inspector.get_view_names(schema="other.main") == ["other_v"]


def test_temp_tables_remain_visible_without_schema(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("CREATE TEMP TABLE temp_only (z INTEGER)"))
        inspector = inspect(conn)
        assert inspector.has_table("temp_only")
        assert [col["name"] for col in inspector.get_columns("temp_only")] == ["z"]


def test_schema_present_in_several_catalogs_prefers_current_database(
    multi_catalog_engine: Engine,
) -> None:
    with multi_catalog_engine.connect() as conn:
        inspector = inspect(conn)
        assert inspector.get_table_names(schema="s") == ["t"]
        assert inspector.has_table("t", schema="s")
        assert [col["name"] for col in inspector.get_columns("t", schema="s")] == ["a"]
        assert not inspector.has_table("only_other_s", schema="s")
        assert sorted(inspector.get_table_names(schema="other.s")) == [
            "only_other_s",
            "t",
        ]
        assert [
            col["name"] for col in inspector.get_columns("t", schema="other.s")
        ] == ["b"]


def test_create_all_creates_table_shadowed_by_other_schema(engine: Engine) -> None:
    with engine.connect() as conn:
        _run_each(
            conn,
            ["CREATE SCHEMA staging", "CREATE TABLE staging.events (id INTEGER)"],
        )
    metadata = MetaData()
    Table("events", metadata, Column("id", Integer), Column("name", String))
    metadata.create_all(engine)

    with engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM duckdb_tables() "
                    "WHERE schema_name = 'main' AND table_name = 'events'"
                )
            ).scalar_one()
            == 1
        )
