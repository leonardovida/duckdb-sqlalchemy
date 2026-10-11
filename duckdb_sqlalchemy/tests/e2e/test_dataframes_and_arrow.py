"""pandas and Arrow data registered with, read from and written to DuckDB."""

from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, text

from ... import insert_from_arrow
from .conftest import EngineFactory

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")


def test_registered_dataframe_and_arrow_table_join_a_live_table(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    people = pd.DataFrame({"id": [1, 2, 3], "name": ["ada", "bob", "cy"]})
    scores = pa.table({"person_id": [1, 1, 3], "score": [10, 5, 7]})
    with engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TABLE teams AS SELECT * FROM (VALUES (1, 'red'), (3, 'blue')) t(id, team)"
            )
        )
        # Both documented registration forms.
        conn.execute(text("register(:name, :df)"), {"name": "people", "df": people})
        conn.exec_driver_sql("register", ("scores", scores))  # type: ignore[arg-type]
        rows = conn.execute(
            text(
                "SELECT p.name, t.team, sum(s.score) AS total FROM people p "
                "JOIN scores s ON s.person_id = p.id JOIN teams t ON t.id = p.id "
                "GROUP BY ALL ORDER BY p.name"
            )
        ).all()
    assert rows == [("ada", "red", 15), ("cy", "blue", 7)]


def test_arrow_results_stream_and_release_the_pooled_connection(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path, pool_size=1, max_overflow=0)
    with engine.begin() as conn:
        conn.execute(
            text("CREATE TABLE t AS SELECT i, i::VARCHAR AS s FROM range(25) r(i)")
        )

    with engine.connect() as conn:
        arrow_conn = conn.execution_options(duckdb_arrow=True)
        table = arrow_conn.execute(text("SELECT * FROM t ORDER BY i")).arrow
        assert table.num_rows == 25 and table.column("s")[3].as_py() == "3"
        with arrow_conn.execute(text("SELECT i FROM t")).batches(10) as reader:
            assert sum(batch.num_rows for batch in reader) == 25
    # With a pool of one, this blocks forever if a reader kept the connection.
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM t")).scalar_one() == 25


def test_insert_from_arrow_and_read_back_with_pandas(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path)
    metadata = MetaData()
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
    )
    metadata.create_all(engine)
    batch = pa.table({"id": [1, 2, 3], "name": ["a", None, "c"]})
    with engine.begin() as conn:
        insert_from_arrow(conn, users, batch)
        insert_from_arrow(
            conn, users, pa.table({"id": [2], "name": ["b"]}), on_conflict="replace"
        )

    with engine.connect() as conn:
        chunks = list(pd.read_sql("SELECT * FROM users ORDER BY id", conn, chunksize=2))
    frame = pd.concat(chunks, ignore_index=True)
    assert frame.to_dict("records") == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]
