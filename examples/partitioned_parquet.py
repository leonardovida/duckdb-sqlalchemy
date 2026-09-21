"""Export a filtered snapshot to partitioned Parquet and query it again.

Run from the repository: uv run python examples/partitioned_parquet.py
The temporary directory is removed when the example finishes.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    bindparam,
    create_engine,
    select,
)

from duckdb_sqlalchemy import copy_to_parquet, read_parquet


def main() -> None:
    engine = create_engine("duckdb:///:memory:")
    events = Table(
        "events",
        MetaData(),
        Column("id", Integer),
        Column("region", String),
    )
    try:
        with TemporaryDirectory() as directory, engine.begin() as connection:
            events.create(connection)
            connection.execute(
                events.insert(),
                [
                    {"id": 1, "region": "eu"},
                    {"id": 2, "region": "us"},
                    {"id": 3, "region": "eu"},
                    {"id": 4, "region": "us"},
                ],
            )
            destination = Path(directory) / "snapshot"
            query = select(events).where(events.c.id >= bindparam("minimum_id"))
            count = copy_to_parquet(
                connection,
                query,
                destination,
                parameters={"minimum_id": 2},
                partition_by=["region"],
                compression="zstd",
            ).scalar_one()
            # An empty partitioned export creates no Parquet files to scan.
            rows = []
            if count:
                snapshot = read_parquet(
                    str(destination / "**" / "*.parquet"),
                    columns=["id", "region"],
                    hive_partitioning=True,
                )
                rows = connection.execute(
                    select(snapshot.c.id, snapshot.c.region).order_by(snapshot.c.id)
                ).all()
            assert count == 3
            assert rows == [(2, "us"), (3, "eu"), (4, "us")]
            assert {p.parent.name for p in destination.rglob("*.parquet")} == {
                "region=eu",
                "region=us",
            }
            print(f"Exported {count} rows across two partitions: {rows}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
