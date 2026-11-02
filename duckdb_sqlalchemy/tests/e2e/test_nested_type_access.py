"""Named ENUM members of nested types, and STRUCT field and MAP key access."""

import datetime
import decimal
import enum
import uuid
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    Date,
    Enum,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    func,
    insert,
    inspect,
    select,
    text,
)

from ...datatypes import FixedArray, Map, Struct, Union
from .conftest import EngineFactory


class Status(enum.Enum):
    active = "active"
    closed = "closed"


def _enum_types(conn) -> list:
    return conn.scalars(
        text(
            "SELECT type_name FROM duckdb_types() "
            "WHERE logical_type = 'ENUM' AND NOT internal ORDER BY type_name"
        )
    ).all()


def test_create_all_creates_enums_used_inside_nested_types(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    metadata = MetaData()
    accounts = Table(
        "accounts",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("profile", Struct({"status": Enum(Status), "since": Date})),
        Column("by_region", Map(String, Enum("low", "high", name="tier"))),
        Column("flag", Union({"status": Enum(Status), "code": Integer})),
        Column("history", FixedArray(Enum(Status), 2)),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        assert _enum_types(conn) == ["status", "tier"]
        conn.execute(
            insert(accounts),
            [
                {
                    "id": 1,
                    "profile": {
                        "status": Status.closed,
                        "since": datetime.date(2026, 1, 1),
                    },
                    "by_region": {"eu": "high"},
                    "flag": 5,
                    "history": [Status.active, Status.closed],
                }
            ],
        )
        row = conn.execute(select(accounts)).one()
    assert row.profile == {"status": Status.closed, "since": datetime.date(2026, 1, 1)}
    assert row.by_region == {"eu": "high"}
    assert row.history == [Status.active, Status.closed]
    assert inspect(engine).get_columns("accounts")[1]["type"].fields[
        "status"
    ].enums == [
        "active",
        "closed",
    ]

    metadata.drop_all(engine)
    with engine.connect() as conn:
        assert _enum_types(conn) == []


def test_struct_fields_and_map_keys_return_their_declared_types(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    metadata = MetaData()
    orders = Table(
        "orders",
        metadata,
        Column("id", Integer, primary_key=True),
        Column(
            "detail",
            Struct(
                {
                    "Amount": Numeric(10, 2),
                    "status": Enum(Status),
                    "shipping": Struct({"day": Date}),
                }
            ),
        ),
        Column("refs", Map(Enum(Status), String)),
    )
    metadata.create_all(engine)
    ref = str(uuid.UUID(int=7))
    with engine.begin() as conn:
        conn.execute(
            insert(orders),
            [
                {
                    "id": 1,
                    "detail": {
                        "Amount": decimal.Decimal("12.50"),
                        "status": Status.active,
                        "shipping": {"day": datetime.date(2026, 3, 1)},
                    },
                    "refs": {Status.active: ref},
                },
                {
                    "id": 2,
                    "detail": {
                        "Amount": decimal.Decimal("3.00"),
                        "status": Status.closed,
                        "shipping": None,
                    },
                    "refs": {},
                },
            ],
        )
        detail = orders.c.detail
        row = conn.execute(
            select(
                detail["amount"],  # DuckDB matches field names case-insensitively
                detail["status"],
                detail["shipping"]["day"],
                orders.c.refs[Status.active],
                orders.c.refs[Status.closed],
            ).where(orders.c.id == 1)
        ).one()
        assert row == (
            decimal.Decimal("12.50"),
            Status.active,
            datetime.date(2026, 3, 1),
            ref,
            None,
        )
        stmt = select(orders.c.id).where(
            detail["status"] == Status.closed, detail["Amount"] < 5
        )
        assert conn.scalars(stmt).all() == [2]
        assert conn.scalar(select(func.sum(detail["Amount"]))) == decimal.Decimal(
            "15.50"
        )
    with pytest.raises(TypeError, match="named by strings"):
        detail[0]
