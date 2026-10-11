"""Applications spanning attached databases and named schemas."""

from pathlib import Path

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    event,
    inspect,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from ... import copy_from_rows
from .conftest import EngineFactory


def _attach(engine, path: Path) -> None:
    @event.listens_for(engine, "connect")
    def attach(dbapi_connection, _record) -> None:
        dbapi_connection.execute(f"ATTACH '{path.as_posix()}' AS warehouse")
        dbapi_connection.execute('CREATE SCHEMA IF NOT EXISTS warehouse."sales data"')


def test_orm_and_bulk_writes_across_attached_schema(
    tmp_path: Path, db_path: Path, make_engine: EngineFactory
) -> None:
    warehouse = tmp_path / "warehouse.duckdb"
    engine = make_engine(db_path)
    _attach(engine, warehouse)

    class Base(DeclarativeBase):
        metadata = MetaData(schema="tenant")

    class Customer(Base):
        __tablename__ = "customers"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]

    class Order(Base):
        __tablename__ = "orders"
        id: Mapped[int] = mapped_column(primary_key=True)
        # DuckDB rejects catalog-qualified REFERENCES, so no FK across ATTACH.
        customer_id: Mapped[int]

    # One logical schema, mapped to a quoted schema in another database.
    translated = engine.execution_options(
        schema_translate_map={"tenant": 'warehouse."sales data"'}
    )
    Base.metadata.create_all(translated)
    with Session(translated) as session:
        session.add_all([Customer(id=1, name="ada"), Customer(id=2, name="bob")])
        session.flush()
        session.add(Order(id=10, customer_id=1))
        session.commit()
    with translated.begin() as conn:
        copy_from_rows(conn, Order.__table__, [{"id": 11, "customer_id": 2}])
        conn.execute(
            Order.__table__.insert(),
            [{"id": i, "customer_id": 1} for i in range(12, 40)],
        )

    engine.dispose()
    reopened = make_engine(db_path)
    _attach(reopened, warehouse)
    with reopened.connect() as conn:
        assert (
            conn.execute(
                text('SELECT count(*) FROM warehouse."sales data".orders')
            ).scalar_one()
            == 30
        )
        inspector = inspect(conn)
        schema = 'warehouse."sales data"'
        assert set(inspector.get_table_names(schema=schema)) == {"customers", "orders"}

        reflected = MetaData()
        orders = Table("orders", reflected, schema=schema, autoload_with=conn)
        customers = Table("customers", reflected, schema=schema, autoload_with=conn)
        query = (
            select(customers.c.name, orders.c.id)
            .join(orders, orders.c.customer_id == customers.c.id)
            .where(orders.c.id < 12)
            .order_by(orders.c.id)
        )
        assert conn.execute(query).all() == [("ada", 10), ("bob", 11)]


def test_same_table_name_in_two_schemas_stays_separate(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    metadata = MetaData()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA staging"))
    tables = [
        Table(
            "items",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("v", String),
            schema=s,
        )
        for s in (None, "staging")
    ]
    Table(
        "links",
        metadata,
        Column("item_id", ForeignKey("staging.items.id")),
        schema="staging",
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(tables[0].insert(), {"id": 1, "v": "main"})
        conn.execute(
            tables[1].insert(), [{"id": 1, "v": "staging"}, {"id": 2, "v": "x"}]
        )
    inspector = inspect(engine)
    assert inspector.get_table_names() == ["items"]
    assert inspector.get_table_names(schema="staging") == ["items", "links"]
    fk = inspector.get_foreign_keys("links", schema="staging")[0]
    assert (fk["referred_schema"], fk["referred_table"]) == ("staging", "items")
    with engine.connect() as conn:
        assert conn.execute(select(tables[0].c.v)).scalars().all() == ["main"]
        assert conn.execute(select(tables[1].c.v).order_by("id")).scalars().all() == [
            "staging",
            "x",
        ]
