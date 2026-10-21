"""Query disjoint Parquet snapshots after adding an optional column.

Run from the repository: uv run python examples/snapshot_schema_evolution.py
The temporary database and snapshots are removed when the example finishes.
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
from sqlalchemy.sql.selectable import FromClause, Select

from duckdb_sqlalchemy import copy_to_parquet, read_parquet


def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        engine = create_engine(f"duckdb:///{root / 'app.duckdb'}")
        metadata = MetaData()
        events = Table(
            "events",
            metadata,
            Column("id", Integer),
            Column("account_id", String),
            Column("region", String),
            Column("channel", String),
        )
        accounts = Table(
            "accounts", metadata, Column("id", String), Column("tier", String)
        )
        try:
            with engine.begin() as conn:
                metadata.create_all(conn)
                conn.execute(
                    events.insert(),
                    [
                        {
                            "id": 1,
                            "account_id": "001",
                            "region": "123",
                            "channel": None,
                        },
                        {"id": 2, "account_id": "1", "region": None, "channel": None},
                        {
                            "id": 3,
                            "account_id": "001",
                            "region": "001",
                            "channel": "web",
                        },
                        {
                            "id": 4,
                            "account_id": "1",
                            "region": "2026-10-21",
                            "channel": "email",
                        },
                    ],
                )
                conn.execute(
                    accounts.insert(),
                    [{"id": "001", "tier": "pro"}, {"id": "1", "tier": "free"}],
                )
                # These batches are disjoint. Repeated full snapshots would duplicate rows.
                old = select(events.c.id, events.c.account_id, events.c.region).where(
                    events.c.id <= bindparam("boundary")
                )
                # The newer export adds channel and changes the physical column order.
                new = select(
                    events.c.channel, events.c.region, events.c.account_id, events.c.id
                ).where(events.c.id > bindparam("boundary"))
                paths = []
                for name, query, expected_count in [
                    ("batch-1", old, 2),
                    ("batch-2", new, 2),
                    ("batch-empty", old.where(events.c.id < 0), 0),
                ]:
                    destination = root / name
                    count = copy_to_parquet(
                        conn,
                        query,
                        destination,
                        parameters={"boundary": 2},
                        partition_by=["region"],
                    ).scalar_one()
                    assert count == expected_count
                    # Empty partitioned exports create no files to scan.
                    if count:
                        paths.append(str(destination / "**" / "*.parquet"))
                assert len(paths) == 2

                conn.execute(
                    accounts.update().where(accounts.c.id == "1").values(tier="pro")
                )
                snapshot = read_parquet(
                    paths,
                    columns=["id", "account_id", "region", "channel"],
                    union_by_name=True,
                    hive_partitioning=True,
                    hive_types={"region": "VARCHAR"},
                )

                def report(source: FromClause) -> Select:
                    return (
                        select(
                            source.c.id,
                            source.c.account_id,
                            source.c.region,
                            source.c.channel,
                            accounts.c.tier,
                        )
                        .join(accounts, source.c.account_id == accounts.c.id)
                        .where(source.c.id >= bindparam("minimum_id"))
                        .where(
                            accounts.c.id.in_(bindparam("account_ids", expanding=True))
                        )
                        .order_by(source.c.id)
                    )

                parameters = {"minimum_id": 1, "account_ids": ["001", "1"]}
                rows = conn.execute(report(snapshot), parameters).all()
                assert rows == conn.execute(report(events), parameters).all()
                assert rows == [
                    (1, "001", "123", None, "pro"),
                    (2, "1", None, None, "pro"),
                    (3, "001", "001", "web", "pro"),
                    (4, "1", "2026-10-21", "email", "pro"),
                ]
                # Reuse the query with different expanding binds and an empty selection.
                query = report(snapshot)
                for account_ids, expected in [(["001"], [rows[0], rows[2]]), ([], [])]:
                    assert (
                        conn.execute(
                            query, {"minimum_id": 1, "account_ids": account_ids}
                        ).all()
                        == expected
                    )
                print(f"Combined snapshots joined to live accounts: {rows}")
        finally:
            engine.dispose()


if __name__ == "__main__":
    main()
