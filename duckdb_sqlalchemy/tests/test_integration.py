import os
import pandas as pd
from pytest import mark
from sqlalchemy import __version__, text
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Connection

df = pd.DataFrame([{"a": 1}])


@mark.skipif(not hasattr(Connection, "exec_driver_sql"), reason="Needs exec_driver_sql")
def test_register_driver(conn: Connection) -> None:
    conn.exec_driver_sql("register", ("test_df_driver", df))  # type: ignore[arg-type]
    conn.execute(text("select * from test_df_driver"))


def test_plain_register(conn: Connection) -> None:
    if __version__.startswith("1.3"):
        conn.execute(text("register"), {"name": "test_df", "df": df})
    else:
        conn.execute(text("register(:name, :df)"), {"name": "test_df", "df": df})
    conn.execute(text("select * from test_df"))



@mark.remote_data
@mark.skipif(
    not (os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")),
    reason="Set MOTHERDUCK_TOKEN to run real MotherDuck integration",
)
def test_motherduck() -> None:
    from sqlalchemy import inspect
    from sqlalchemy.pool import QueuePool

    database = os.getenv("MOTHERDUCK_TEST_DATABASE", "")
    engine = create_engine(
        f"duckdb:///md:{database}", poolclass=QueuePool, pool_size=2, max_overflow=0,
    )
    try:
        with engine.connect() as connection:
            assert connection.execute(text("SELECT 42")).scalar_one() == 42
            assert connection.execute(text("SELECT current_database()")).scalar_one()
            assert isinstance(inspect(connection).get_table_names(), list)
            with connection.execution_options(duckdb_arrow=True).execute(
                text("SELECT i FROM range(5) AS t(i)")
            ).batches(2) as reader:
                assert sum(batch.num_rows for batch in reader) == 5
        with engine.connect() as connection:
            assert connection.execute(text("SELECT 43")).scalar_one() == 43
    finally:
        engine.dispose()
