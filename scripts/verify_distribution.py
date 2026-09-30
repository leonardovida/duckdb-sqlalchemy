"""Smoke test the installed distribution with Python's isolated (-I) mode."""

import decimal
import importlib.util
from importlib.metadata import distribution
from pathlib import Path

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, inspect, select, text
from sqlalchemy.orm import Session

import duckdb_sqlalchemy
from duckdb_sqlalchemy import insert_from_arrow


def main() -> None:
    installed = distribution("duckdb-sqlalchemy")
    module_path = Path(duckdb_sqlalchemy.__file__).resolve()
    assert module_path.is_relative_to(Path(installed.locate_file("")).resolve())
    assert installed.version == duckdb_sqlalchemy.__version__
    assert not any(str(path).startswith("duckdb_sqlalchemy/tests/") for path in installed.files or [])
    entrypoint = next(ep for ep in installed.entry_points if ep.group == "sqlalchemy.dialects" and ep.name == "duckdb")
    assert entrypoint.load() is duckdb_sqlalchemy.Dialect

    engine = create_engine("duckdb:///:memory:")
    metadata = MetaData()
    table = Table("distribution_test", metadata, Column("id", Integer, primary_key=True), Column("name", String))
    try:
        metadata.create_all(engine)
        with Session(engine) as session:
            session.execute(table.insert(), {"name": "installed"})
            session.commit()
            assert session.execute(select(table)).one() == (1, "installed")
        assert [column["name"] for column in inspect(engine).get_columns(table.name)] == ["id", "name"]
        with engine.begin() as connection:
            assert connection.execute(text("SELECT CAST(1.2345 AS DECIMAL(18, 4))")).scalar_one() == decimal.Decimal("1.2345")
            if importlib.util.find_spec("pyarrow"):
                import pyarrow as pa
                insert_from_arrow(connection, table, pa.table({"id": [2], "name": ["arrow"]}))
                result = connection.execution_options(duckdb_arrow=True).execute(text("SELECT * FROM distribution_test"))
                with result.batches(1) as reader:
                    assert sum(batch.num_rows for batch in reader) == 2
            if importlib.util.find_spec("pandas"):
                import pandas as pd
                pd.DataFrame({"value": [2**53 + 1, None]}, dtype=object).to_sql("pandas_test", connection, index=False)
                assert connection.execution_options(duckdb_arrow=False).exec_driver_sql("SELECT value FROM pandas_test WHERE value IS NOT NULL").scalar_one() == 2**53 + 1
    finally:
        metadata.drop_all(engine)
        engine.dispose()
    print(f"Installed distribution OK: {installed.version}")


if __name__ == "__main__":
    main()
