"""Join a partitioned Parquet snapshot to a live DuckDB table.

Run from the repository: uv run python examples/snapshot_live_join.py
The temporary snapshot is removed when the example finishes.
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
    or_,
    select,
)

from duckdb_sqlalchemy import copy_to_parquet, read_parquet


def main() -> None:
    engine = create_engine("duckdb:///:memory:")
    metadata = MetaData()
    events = Table(
        "events",
        metadata,
        Column("id", Integer),
        Column("account_id", Integer),
        Column("region", String),
    )
    accounts = Table(
        "accounts",
        metadata,
        Column("id", Integer),
        Column("tier", String),
    )
    try:
        with TemporaryDirectory() as directory, engine.begin() as connection:
            metadata.create_all(connection)
            connection.execute(
                events.insert(),
                [
                    {"id": 1, "account_id": 10, "region": "123"},
                    {"id": 2, "account_id": 11, "region": None},
                    {"id": 3, "account_id": 10, "region": "eu"},
                ],
            )
            connection.execute(
                accounts.insert(),
                [{"id": 10, "tier": "pro"}, {"id": 11, "tier": "free"}],
            )

            destination = Path(directory) / "snapshot"
            count = copy_to_parquet(
                connection,
                select(events).where(events.c.id >= bindparam("export_minimum")),
                destination,
                parameters={"export_minimum": 1},
                partition_by=["region"],
            ).scalar_one()
            assert count == 3

            # The dimension remains live after the event snapshot is written.
            connection.execute(
                accounts.update().where(accounts.c.id == 11).values(tier="pro")
            )
            snapshot = read_parquet(
                str(destination / "**" / "*.parquet"),
                columns=["id", "account_id", "region"],
                hive_partitioning=True,
                hive_types={"region": "VARCHAR"},
            )
            query = (
                select(snapshot.c.id, snapshot.c.region, accounts.c.tier)
                .join(accounts, snapshot.c.account_id == accounts.c.id)
                .where(snapshot.c.id >= bindparam("report_minimum"))
                .where(
                    or_(
                        snapshot.c.region == bindparam("region"),
                        snapshot.c.region.is_(None),
                    )
                )
                .where(accounts.c.tier == bindparam("tier"))
                .order_by(snapshot.c.id)
            )
            rows = connection.execute(
                query, {"report_minimum": 1, "region": "123", "tier": "pro"}
            ).all()
            assert rows == [(1, "123", "pro"), (2, None, "pro")]
            print(f"Snapshot joined to live accounts: {rows}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
