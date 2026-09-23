from pathlib import Path

import pytest
from sqlalchemy import bindparam, create_engine, literal, select

from duckdb_sqlalchemy import copy_to_parquet, read_parquet


def test_repeated_exports_use_current_destination_and_options(tmp_path: Path) -> None:
    engine = create_engine("duckdb:///:memory:")
    query = select(bindparam("value").label("value"))
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    try:
        with engine.connect() as connection:
            for path, value, codec in [(first, 1, "uncompressed"), (second, 2, "zstd")]:
                assert (
                    copy_to_parquet(
                        connection,
                        query,
                        path,
                        parameters={"value": value},
                        compression=codec,
                    ).scalar_one()
                    == 1
                )
            for path, value, codec in [(first, 1, "UNCOMPRESSED"), (second, 2, "ZSTD")]:
                assert connection.exec_driver_sql(
                    "SELECT value FROM read_parquet(?)", (str(path),)
                ).all() == [(value,)]
                assert connection.exec_driver_sql(
                    "SELECT DISTINCT compression FROM parquet_metadata(?)", (str(path),)
                ).all() == [(codec,)]
    finally:
        engine.dispose()


@pytest.mark.parametrize("key", ["001", "123", "2026-09-23"])
def test_partition_string_keys_round_trip(tmp_path: Path, key: str) -> None:
    engine = create_engine("duckdb:///:memory:")
    try:
        with engine.connect() as connection:
            query = select(literal(1).label("id"), literal(key).label("key"))
            copy_to_parquet(
                connection, query, tmp_path / "snapshot", partition_by=["key"]
            )
            snapshot = read_parquet(
                str(tmp_path / "snapshot" / "**" / "*.parquet"),
                columns=["id", "key"],
                hive_partitioning=True,
                hive_types={"key": "VARCHAR"},
            )
            assert connection.execute(select(snapshot)).all() == [(1, key)]
    finally:
        engine.dispose()
