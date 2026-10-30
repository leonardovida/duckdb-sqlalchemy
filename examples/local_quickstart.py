"""Use SQLAlchemy with a local DuckDB file: ORM writes, analytics, Parquet.

The same code runs on MotherDuck by changing the URL to duckdb:///md:<database>
(see motherduck_quickstart.py).

Run from the repository: uv run python examples/local_quickstart.py
The temporary directory is removed when the example finishes.
"""

from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import Date, Numeric, String, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from duckdb_sqlalchemy import copy_to_parquet, read_parquet


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    ordered_on: Mapped[date] = mapped_column(Date)
    region: Mapped[str] = mapped_column(String)
    amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))


def main() -> None:
    with TemporaryDirectory() as directory:
        engine = create_engine(f"duckdb:///{Path(directory) / 'shop.duckdb'}")
        try:
            Base.metadata.create_all(engine)

            with Session(engine) as session:
                session.add_all(
                    Order(
                        id=index,
                        ordered_on=date(2026, 1, 1 + index % 3),
                        region=("EMEA", "AMER")[index % 2],
                        amount=Decimal(10 + index),
                    )
                    for index in range(1, 7)
                )
                session.commit()

                for region, orders, revenue in session.execute(
                    select(Order.region, func.count(), func.sum(Order.amount))
                    .group_by(Order.region)
                    .order_by(Order.region)
                ):
                    print(f"{region}: {orders} orders, {revenue} revenue")

            # Export a query to Parquet, then query the file like a table.
            export = Path(directory) / "orders.parquet"
            with engine.begin() as conn:
                copy_to_parquet(conn, select(Order.__table__), export)
                orders = read_parquet(str(export), columns=["id", "amount"])
                largest = conn.execute(
                    select(orders.c.id, orders.c.amount)
                    .order_by(orders.c.amount.desc())
                    .limit(2)
                ).all()
            print(f"largest orders from Parquet: {largest}")
        finally:
            engine.dispose()


if __name__ == "__main__":
    main()
