"""Parquet/CSV export and import, and table functions joined to live tables."""

import datetime
import decimal
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    Date,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    bindparam,
    exc,
    func,
    select,
    text,
)

from ... import (
    copy_from_csv,
    copy_from_parquet,
    copy_from_rows,
    copy_to_parquet,
    read_csv,
    read_parquet,
)
from ...olap import pragma_storage_info
from .conftest import EngineFactory

ROWS = [
    {
        "id": 1,
        "name": 'ada, "the" first',
        "amount": decimal.Decimal("1.10"),
        "day": datetime.date(2026, 1, 1),
    },
    {
        "id": 2,
        "name": None,
        "amount": decimal.Decimal("-2.25"),
        "day": datetime.date(2026, 2, 1),
    },
    {"id": 3, "name": "ünïcødé\nline", "amount": None, "day": None},
]


def _table(metadata: MetaData, name: str) -> Table:
    return Table(
        name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("amount", Numeric(10, 2)),
        Column("day", Date),
    )


def _rows(conn, table: Table) -> list:
    return [
        dict(r) for r in conn.execute(select(table).order_by(table.c.id)).mappings()
    ]


def test_parquet_and_csv_round_trips_preserve_rows(
    tmp_path: Path, db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    metadata = MetaData()
    source = _table(metadata, "source")
    targets = [
        _table(metadata, name) for name in ("from_parquet", "from_csv", "from_rows")
    ]
    metadata.create_all(engine)
    parquet = tmp_path / "out.parquet"
    csv_path = tmp_path / "out.csv"

    with engine.begin() as conn:
        conn.execute(source.insert(), ROWS)
        count = copy_to_parquet(
            conn,
            select(source).where(source.c.id >= bindparam("min_id")),
            parquet,
            parameters={"min_id": 1},
        ).scalar_one()
        assert count == 3
        conn.exec_driver_sql(f"COPY source TO '{csv_path.as_posix()}' (HEADER)")

        copy_from_parquet(conn, targets[0], parquet)
        copy_from_csv(conn, targets[1], csv_path, header=True)
        # Generator input crosses several COPY chunks.
        copy_from_rows(conn, targets[2], (dict(r) for r in ROWS), chunk_size=1)

        for target in targets:
            assert _rows(conn, target) == ROWS, target.name


def test_copy_from_rows_is_transactional(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    metadata = MetaData()
    target = _table(metadata, "target")
    metadata.create_all(engine)
    with pytest.raises(RuntimeError):
        with engine.begin() as conn:
            copy_from_rows(conn, target, ROWS, chunk_size=2)
            assert len(_rows(conn, target)) == 3
            raise RuntimeError("abort")
    with engine.connect() as conn:
        assert _rows(conn, target) == []


def test_table_functions_join_live_tables(
    tmp_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine("duckdb:///:memory:")
    orders_path = tmp_path / "orders.parquet"
    regions_path = tmp_path / "regions.csv"
    regions_path.write_text("code;label\nus;United States\neu;Europe\n")
    metadata = MetaData()
    customers = Table(
        "customers",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("region", String),
    )
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            customers.insert(),
            [
                {"id": 1, "region": "us"},
                {"id": 2, "region": "eu"},
                {"id": 3, "region": "us"},
            ],
        )
        conn.exec_driver_sql(
            f"COPY (SELECT i % 3 + 1 AS customer_id, i AS total FROM range(10) t(i)) "
            f"TO '{orders_path.as_posix()}' (FORMAT parquet)"
        )

        orders = read_parquet(orders_path, columns=["customer_id", "total"]).alias("o")
        regions = read_csv(
            regions_path,
            columns={"code": "VARCHAR", "label": "VARCHAR"},
            delim=";",
            header=True,
        ).alias("r")
        per_customer = (
            select(orders.c.customer_id, func.sum(orders.c.total).label("total"))
            .where(orders.c.total >= bindparam("floor"))
            .group_by(orders.c.customer_id)
            .cte("per_customer")
        )
        query = (
            select(regions.c.label, func.sum(per_customer.c.total).label("total"))
            .join_from(
                per_customer, customers, customers.c.id == per_customer.c.customer_id
            )
            .join(regions, regions.c.code == customers.c.region)
            .group_by(regions.c.label)
            .order_by(regions.c.label)
        )
        # Orders 2..9 go to customer i % 3 + 1: Europe gets 4 and 7.
        assert conn.execute(query, {"floor": 2}).all() == [
            ("Europe", 11),
            ("United States", 33),
        ]

    # Committed rows are in table storage, not transaction-local storage.
    with engine.connect() as conn:
        storage = conn.execute(select(pragma_storage_info("customers"))).mappings()
        assert {row["column_name"] for row in storage} == {"id", "region"}


def test_storage_info_segment_flag_matches_documented_support(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t AS SELECT 1 AS i"))
    with engine.connect() as conn:
        query = select(pragma_storage_info("t", include_segment_info=True))
        try:
            rows = conn.execute(query).mappings().all()
        except exc.ProgrammingError as error:
            # docs/olap.md: released DuckDB 1.5.x rejects the argument.
            assert "Invalid named parameter" in str(error)
        else:
            assert {row["column_name"] for row in rows} == {"i"}
