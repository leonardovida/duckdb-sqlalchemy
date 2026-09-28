from decimal import Decimal

import duckdb
import pytest
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    Numeric,
    Table,
    create_engine,
    func,
    select,
    text,
    type_coerce,
)
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import Engine

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
