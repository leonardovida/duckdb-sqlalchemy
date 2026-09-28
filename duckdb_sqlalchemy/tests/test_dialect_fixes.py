from decimal import Decimal

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    Numeric,
    Table,
    func,
    select,
    type_coerce,
)
from sqlalchemy.engine import Engine


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
