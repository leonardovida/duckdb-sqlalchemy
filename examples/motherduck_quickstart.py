"""Create, load and query a MotherDuck database with SQLAlchemy.

Requires MOTHERDUCK_TOKEN. The example creates a database named
duckdb_sqlalchemy_quickstart_<random>, loads it through the ORM, reads it back
as rows and as an Arrow table, and drops the database at the end. Pass --keep
to keep the database for inspection in the MotherDuck UI.

Run from the repository: uv run python examples/motherduck_quickstart.py
"""

import os
import secrets
import sys
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from typing import Iterator

from sqlalchemy import Date, Numeric, String, func, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from duckdb_sqlalchemy import create_motherduck_engine


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    ordered_on: Mapped[date] = mapped_column(Date)
    region: Mapped[str] = mapped_column(String)
    amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))


@contextmanager
def scratch_database(prefix: str, keep: bool = False) -> Iterator[str]:
    """Create a uniquely named MotherDuck database and drop it afterwards."""
    name = f"duckdb_sqlalchemy_{prefix}_{secrets.token_hex(4)}"
    # md: without a database name attaches the whole workspace, which is
    # where CREATE DATABASE and DROP DATABASE run.
    workspace = create_motherduck_engine(database="md:")
    try:
        with workspace.begin() as conn:
            conn.execute(text(f"CREATE DATABASE {name}"))
        yield name
    finally:
        if keep:
            print(f"kept database {name}")
        else:
            with workspace.begin() as conn:
                conn.execute(text(f"DROP DATABASE IF EXISTS {name}"))
        workspace.dispose()


def main() -> None:
    if not os.getenv("MOTHERDUCK_TOKEN"):
        raise SystemExit("Set MOTHERDUCK_TOKEN before running this example")

    with scratch_database("quickstart", keep="--keep" in sys.argv) as database:
        # attach_mode=single attaches only this database, which connects faster
        # and keeps BI tools and reflection scoped to it.
        engine = create_motherduck_engine(
            database=f"md:{database}", attach_mode="single"
        )
        try:
            Base.metadata.create_all(engine)

            with Session(engine) as session:
                session.add_all(
                    [
                        Order(
                            id=1,
                            ordered_on=date(2026, 1, 5),
                            region="EMEA",
                            amount=Decimal("120.00"),
                        ),
                        Order(
                            id=2,
                            ordered_on=date(2026, 1, 5),
                            region="AMER",
                            amount=Decimal("80.50"),
                        ),
                        Order(
                            id=3,
                            ordered_on=date(2026, 1, 6),
                            region="EMEA",
                            amount=Decimal("42.25"),
                        ),
                    ]
                )
                session.commit()

                revenue = session.execute(
                    select(Order.region, func.sum(Order.amount).label("revenue"))
                    .group_by(Order.region)
                    .order_by(Order.region)
                ).all()
                for region, total in revenue:
                    print(f"{region}: {total}")

            # Arrow results skip per-row Python conversion, which matters for
            # wide or large reads. They need pyarrow.
            with engine.connect().execution_options(duckdb_arrow=True) as conn:
                table = conn.execute(
                    select(Order.ordered_on, func.count().label("orders"))
                    .group_by(Order.ordered_on)
                    .order_by(Order.ordered_on)
                ).arrow
                print(table.to_pylist())
        finally:
            engine.dispose()


if __name__ == "__main__":
    main()
