"""Execution boundaries for the multi-snapshot recipe's native reader options."""

from pathlib import Path

import pytest
from sqlalchemy import exc, literal, select

from ... import copy_to_parquet, read_parquet
from .conftest import EngineFactory


@pytest.mark.parametrize("reverse", [False, True])
def test_added_snapshot_column_requires_name_union(
    tmp_path: Path, db_path: Path, make_engine: EngineFactory, reverse: bool
) -> None:
    engine = make_engine(db_path)
    paths = [tmp_path / "old.parquet", tmp_path / "new.parquet"]
    with engine.connect() as conn:
        copy_to_parquet(conn, select(literal(1).label("id")), paths[0])
        copy_to_parquet(
            conn,
            select(literal("web").label("channel"), literal(2).label("id")),
            paths[1],
        )
        # Name matching works in either file order, including reordered columns.
        scan = read_parquet(
            paths[::-1] if reverse else paths,
            columns=["id", "channel"],
            union_by_name=True,
        )
        assert conn.execute(select(scan).order_by(scan.c.id)).all() == [
            (1, None),
            (2, "web"),
        ]
        default_scan = read_parquet(paths, columns=["id", "channel"])
        with pytest.raises(exc.DBAPIError, match="channel"):
            conn.execute(select(default_scan)).all()


@pytest.mark.parametrize("reverse", [False, True])
def test_name_union_does_not_repair_incompatible_snapshot_types(
    tmp_path: Path, db_path: Path, make_engine: EngineFactory, reverse: bool
) -> None:
    engine = make_engine(db_path)
    paths = [tmp_path / "scalar.parquet", tmp_path / "list.parquet"]
    with engine.connect() as conn:
        copy_to_parquet(conn, select(literal(7).label("value")), paths[0])
        copy_to_parquet(conn, select(literal([7, 8]).label("value")), paths[1])
        scan = read_parquet(
            paths[::-1] if reverse else paths,
            columns=["value"],
            union_by_name=True,
        )
        with pytest.raises(exc.DBAPIError) as error:
            conn.execute(select(scan)).all()
        assert "cast" in str(error.value.orig).lower()
