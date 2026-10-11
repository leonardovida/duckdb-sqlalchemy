"""Engine helpers and type lifecycles exercised against real databases."""

from pathlib import Path

import duckdb
from sqlalchemy import Column, Enum, MetaData, Table, inspect, pool, select, text
from sqlalchemy.engine import make_url

from ... import create_engine_from_paths
from .conftest import EngineFactory


def test_engine_from_paths_rotates_databases_and_per_path_config(
    tmp_path: Path,
) -> None:
    for name in ("a", "b"):
        with duckdb.connect(str(tmp_path / f"{name}.duckdb")) as con:
            con.execute(f"CREATE TABLE origin AS SELECT '{name}' AS name")
    engine = create_engine_from_paths(
        [
            str(tmp_path / "a.duckdb"),
            make_url(f"duckdb:///{(tmp_path / 'b.duckdb').as_posix()}?threads=3"),
        ],
        connect_args={"config": {"memory_limit": "300MB"}},
        poolclass=pool.NullPool,
    )
    try:
        seen = []
        for _ in range(4):
            with engine.connect() as conn:
                seen.append(
                    conn.execute(
                        text(
                            "SELECT name, current_setting('threads'), "
                            "current_setting('memory_limit') FROM origin"
                        )
                    ).one()
                )
    finally:
        engine.dispose()
    names = [row[0] for row in seen]
    assert names == ["a", "b", "a", "b"]
    assert {row[1] for row in seen if row[0] == "b"} == {3}
    assert all(row[2].startswith(("286", "300")) for row in seen)


def test_named_and_inline_enums_create_round_trip_and_drop(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    metadata = MetaData()
    table = Table(
        "t",
        metadata,
        Column("named", Enum("sad", "ok", name="mood")),
        Column("inline", Enum("x", "it's")),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(table.insert(), {"named": "ok", "inline": "it's"})
        assert conn.execute(select(table)).one() == ("ok", "it's")
    assert [e["name"] for e in inspect(engine).get_enums()] == ["mood"]

    # A reflected table recreates even though DuckDB hides the type name.
    reflected = Table("t", MetaData(), autoload_with=engine)
    reflected.to_metadata(MetaData(), name="t_copy").create(engine)
    metadata.drop_all(engine)
    assert inspect(engine).get_enums() == []
    assert inspect(engine).get_table_names() == ["t_copy"]
