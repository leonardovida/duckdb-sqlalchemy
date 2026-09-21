from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    TypeDecorator,
    bindparam,
    create_engine,
    insert,
    select,
    text,
    union_all,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError

from duckdb_sqlalchemy import copy_to_parquet
from duckdb_sqlalchemy import read_parquet as parquet_scan


class UpperText(TypeDecorator[str]):
    impl = String
    cache_ok = True

    def process_bind_param(self, value: str | None, dialect: object) -> str | None:
        return value.upper() if value is not None else None

    def process_result_value(self, value: str | None, dialect: object) -> str | None:
        raise AssertionError("COPY Count must not use SELECT result processors")


@pytest.fixture
def events() -> Iterator[tuple[Engine, Table]]:
    engine = create_engine("duckdb:///:memory:")
    metadata = MetaData()
    table = Table(
        "events",
        metadata,
        Column("id", Integer),
        Column("name", UpperText()),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            table.insert(),
            [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
        )
    try:
        yield engine, table
    finally:
        engine.dispose()


def read_parquet(connection: Connection, path: Path) -> list[tuple[object, ...]]:
    result = connection.exec_driver_sql(
        "SELECT * FROM read_parquet(?) ORDER BY id", (str(path),)
    )
    return [tuple(row) for row in result.fetchall()]


def test_copy_to_parquet_preserves_binds_and_count_result(
    events: tuple[Engine, Table], tmp_path: Path
) -> None:
    engine, table = events
    path = tmp_path / "events export 'quoted'.parquet"
    query = (
        select(table.c.name, table.c.id)
        .where(table.c.id > bindparam("minimum", type_=Integer))
        .where(table.c.name.in_(bindparam("names", expanding=True)))
    )

    with engine.begin() as connection:
        result = copy_to_parquet(
            connection,
            query,
            path,
            parameters={"minimum": 0, "names": ["alpha", "beta"]},
            compression="zstd",
        )

        assert result.keys() == ["Count"]
        assert result.fetchone() == (2,)
        assert read_parquet(connection, path) == [("ALPHA", 1), ("BETA", 2)]


def test_copy_to_parquet_supports_empty_expanding_in(
    events: tuple[Engine, Table], tmp_path: Path
) -> None:
    engine, table = events
    path = tmp_path / "empty.parquet"
    query = select(table).where(table.c.name.in_(bindparam("names", expanding=True)))

    with engine.begin() as connection:
        result = copy_to_parquet(connection, query, path, parameters={"names": []})

        assert result.fetchone() == (0,)
        assert read_parquet(connection, path) == []


def test_copy_to_parquet_accepts_compound_select_and_cte(
    events: tuple[Engine, Table], tmp_path: Path
) -> None:
    engine, table = events
    filtered = select(table).where(table.c.id == bindparam("first_id")).cte("filtered")
    query = union_all(select(filtered), select(table).where(table.c.id == 2))
    path = tmp_path / "compound.parquet"

    with engine.begin() as connection:
        result = copy_to_parquet(connection, query, path, parameters={"first_id": 1})

        assert result.fetchone() == (2,)
        assert read_parquet(connection, path) == [(1, "ALPHA"), (2, "BETA")]


def test_copy_to_parquet_overwrites_and_survives_rollback(
    events: tuple[Engine, Table], tmp_path: Path
) -> None:
    engine, table = events
    path = tmp_path / "overwrite.parquet"
    query = select(table).where(table.c.id == bindparam("id"))

    with engine.connect() as connection:
        transaction = connection.begin()
        copy_to_parquet(connection, query, path, parameters={"id": 1})
        transaction.rollback()

        assert path.exists()
        assert read_parquet(connection, path) == [(1, "ALPHA")]

        copy_to_parquet(connection, query, path, parameters={"id": 2})
        assert read_parquet(connection, path) == [(2, "BETA")]


@pytest.mark.parametrize(
    "invalid",
    [text("SELECT 1"), insert(Table("other", MetaData(), Column("id", Integer)))],
)
def test_copy_to_parquet_rejects_non_select(invalid: object, tmp_path: Path) -> None:
    engine = create_engine("duckdb:///:memory:")
    with engine.connect() as connection:
        with pytest.raises(TypeError, match="Select or CompoundSelect"):
            copy_to_parquet(connection, invalid, tmp_path / "invalid.parquet")


def test_copy_to_parquet_rejects_format_override(
    events: tuple[Engine, Table], tmp_path: Path
) -> None:
    engine, table = events
    with engine.connect() as connection:
        with pytest.raises(ValueError, match="always uses FORMAT parquet"):
            copy_to_parquet(
                connection, select(table), tmp_path / "invalid.parquet", FORMAT="csv"
            )


def test_copy_to_parquet_rejects_executemany_parameters(
    events: tuple[Engine, Table], tmp_path: Path
) -> None:
    engine, table = events
    path = tmp_path / "invalid-parameters.parquet"
    with engine.connect() as connection:
        with pytest.raises(TypeError, match="parameters must be a mapping"):
            copy_to_parquet(
                connection,
                select(table),
                path,
                parameters=[{"id": 1}, {"id": 2}],
            )
    assert not path.exists()


def test_copy_to_parquet_rejects_invalid_option_key(
    events: tuple[Engine, Table], tmp_path: Path
) -> None:
    engine, table = events
    with engine.connect() as connection:
        with pytest.raises(ValueError, match="invalid COPY option key"):
            copy_to_parquet(
                connection,
                select(table),
                tmp_path / "invalid.parquet",
                **{"compression); CREATE TABLE pwned(i INTEGER); --": "zstd"},
            )


@pytest.mark.parametrize("minimum_id", [0, 3])
def test_partitioned_export_round_trip(
    events: tuple[Engine, Table], tmp_path: Path, minimum_id: int
) -> None:
    engine, table = events
    destination = tmp_path / "snapshot"
    query = select(table).where(table.c.id > bindparam("minimum_id"))
    with engine.connect() as connection:
        count = copy_to_parquet(
            connection,
            query,
            destination,
            parameters={"minimum_id": minimum_id},
            partition_by=["name"],
        ).scalar_one()
        connection.rollback()
        if minimum_id == 3:
            assert count == 0
            assert not list(destination.rglob("*.parquet"))
        else:
            assert count == 2
            assert {p.parent.name for p in destination.rglob("*.parquet")} == {
                "name=ALPHA",
                "name=BETA",
            }
            snapshot = parquet_scan(
                str(destination / "**" / "*.parquet"),
                columns=["id", "name"],
                hive_partitioning=True,
            )
            assert connection.execute(
                select(snapshot.c.id, snapshot.c.name).order_by(snapshot.c.id)
            ).all() == [(1, "ALPHA"), (2, "BETA")]


def test_partitioned_export_refuses_existing_snapshot(
    events: tuple[Engine, Table], tmp_path: Path
) -> None:
    engine, table = events
    destination = tmp_path / "snapshot"
    with engine.connect() as connection:
        copy_to_parquet(connection, select(table), destination, partition_by=["name"])
        before = {path: path.read_bytes() for path in destination.rglob("*.parquet")}
        assert before
        with pytest.raises(OperationalError, match="not empty"):
            copy_to_parquet(
                connection,
                select(table).where(table.c.id == 1),
                destination,
                partition_by=["name"],
            )
        connection.rollback()
        assert {
            path: path.read_bytes() for path in destination.rglob("*.parquet")
        } == before
