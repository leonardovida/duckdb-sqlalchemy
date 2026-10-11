"""ORM application lifecycle on a file database reopened by independent engines."""

import datetime
import decimal
from pathlib import Path
from typing import List, Optional

import pytest
from sqlalchemy import (
    JSON,
    ForeignKey,
    Interval,
    LargeBinary,
    Numeric,
    Sequence,
    String,
    exc,
    func,
    select,
    text,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    selectinload,
)

from .conftest import EngineFactory


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Sequence("accounts_id_seq"), primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    balance: Mapped[decimal.Decimal] = mapped_column(Numeric(18, 4), default=0)
    settings: Mapped[Optional[dict]] = mapped_column(JSON)
    orders: Mapped[List["Order"]] = relationship(
        back_populates="account", cascade="all, delete-orphan", order_by="Order.id"
    )


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Sequence("orders_id_seq"), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    placed_at: Mapped[datetime.datetime]
    lead_time: Mapped[Optional[datetime.timedelta]] = mapped_column(Interval)
    receipt: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    account: Mapped[Account] = relationship(back_populates="orders")


PLACED = datetime.datetime(2026, 1, 2, 3, 4, 5, 678000)


def test_orm_state_survives_reopen_and_sequences_continue(
    db_path: Path, make_engine: EngineFactory
) -> None:
    writer = make_engine(db_path)
    Base.metadata.create_all(writer)
    with Session(writer) as session:
        alice = Account(name="alice", balance=decimal.Decimal("10.1234"))
        alice.settings = {"theme": "dark", "tags": ["a", "b"]}
        alice.orders = [
            Order(placed_at=PLACED, lead_time=datetime.timedelta(days=2, hours=3)),
            Order(placed_at=PLACED, receipt=b"\x00\xffpdf"),
        ]
        session.add_all([alice, Account(name="bob")])
        session.commit()
        first_ids = [order.id for order in alice.orders]

        alice.balance += decimal.Decimal("0.0001")
        alice.orders.pop(0)  # delete-orphan
        session.commit()
    writer.dispose()

    reader = make_engine(db_path)
    with Session(reader) as session:
        accounts = session.scalars(
            select(Account).options(selectinload(Account.orders)).order_by(Account.id)
        ).all()
        assert [a.name for a in accounts] == ["alice", "bob"]
        alice = accounts[0]
        assert alice.balance == decimal.Decimal("10.1235")
        assert alice.settings == {"theme": "dark", "tags": ["a", "b"]}
        assert [(o.id, o.placed_at, o.receipt) for o in alice.orders] == [
            (first_ids[1], PLACED, b"\x00\xffpdf")
        ]

        # Lazy loading on a fresh object, then a new row continues the
        # persisted sequence instead of reusing an id.
        session.expire_all()
        assert session.get(Account, alice.id).orders[0].account.name == "alice"
        alice.orders.append(Order(placed_at=PLACED))
        session.commit()
        assert alice.orders[-1].id > max(first_ids)

        with pytest.raises(exc.IntegrityError):
            session.add(Account(name="alice"))
            session.flush()
        session.rollback()
        assert session.scalar(select(func.count()).select_from(Account)) == 2


def test_read_only_reopen_reads_but_rejects_writes(
    db_path: Path, make_engine: EngineFactory
) -> None:
    writer = make_engine(db_path)
    Base.metadata.create_all(writer)
    with Session(writer) as session:
        session.add(Account(name="carol"))
        session.commit()
    writer.dispose()

    reader = make_engine(f"duckdb:///{db_path.as_posix()}?access_mode=read_only")
    with Session(reader) as session:
        assert session.scalars(select(Account.name)).all() == ["carol"]
        session.add(Account(name="dave"))
        with pytest.raises(exc.DBAPIError, match="read-only"):
            session.flush()


def test_dispose_releases_the_file_for_a_new_writer(
    db_path: Path, make_engine: EngineFactory
) -> None:
    first = make_engine(db_path)
    with first.begin() as conn:
        conn.execute(text("CREATE TABLE t AS SELECT 1 AS i"))
    first.dispose()

    second = make_engine(db_path)
    with second.begin() as conn:
        conn.execute(text("INSERT INTO t VALUES (2)"))
        assert conn.execute(text("SELECT sum(i) FROM t")).scalar_one() == 3
