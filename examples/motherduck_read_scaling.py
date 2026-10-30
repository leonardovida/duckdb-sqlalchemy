"""Serve many concurrent readers from MotherDuck read scaling replicas.

A read scaling token connects to a pool of read-only Duckling replicas
(4 by default, up to 16). MotherDuck routes all connections that share a
session_name to the same replica, so the session_name decides how readers
spread across the pool:

- One engine per end user, with session_name derived from the user id. Each
  user keeps hitting the same replica and reuses its warm cache. This suits
  customer-facing dashboards.
- One shared engine whose pooled connections rotate across a fixed set of
  session names. This suits a backend that serves anonymous or batch reads.

Replicas lag the writer by up to a few minutes. When a reader must see a
write right away, run CREATE SNAPSHOT on the writer and REFRESH DATABASE on
the reader.

Set MOTHERDUCK_TOKEN to a read-write token (used to create and load the
example database) and MOTHERDUCK_READ_SCALING_TOKEN to a read scaling token
for the same account. Without MOTHERDUCK_READ_SCALING_TOKEN the example runs
every reader on the read-write Duckling and skips REFRESH DATABASE, which only
works on read scaling connections. The example creates a database named
duckdb_sqlalchemy_read_scaling_<random> and drops it at the end. Pass --keep
to keep it.

Run from the repository: uv run python examples/motherduck_read_scaling.py
"""

import os
import secrets
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import lru_cache
from typing import Iterator, Optional

from sqlalchemy import Engine, text

from duckdb_sqlalchemy import (
    create_engine_from_paths,
    create_motherduck_engine,
    stable_session_name,
)

READER_POOL_SIZE = 4


def read_token() -> Optional[str]:
    # CI passes an unconfigured secret as an empty string.
    return os.getenv("MOTHERDUCK_READ_SCALING_TOKEN") or None


@lru_cache(maxsize=1024)
def engine_for_user(database: str, user_id: str) -> Engine:
    """Return the cached engine that pins one end user to one replica."""
    # Hash the user id so the session name does not expose it. Use your own
    # secret salt in production.
    session_name = stable_session_name(user_id, salt=os.getenv("SESSION_SALT", "demo"))
    return create_motherduck_engine(
        database=f"md:{database}",
        attach_mode="single",
        session_name=session_name,
        # Passing the token here keeps it out of the engine URL and overrides
        # MOTHERDUCK_TOKEN for this engine only.
        motherduck_token=read_token() or os.environ["MOTHERDUCK_TOKEN"],
        pool_size=2,
        max_overflow=2,
        pool_pre_ping=True,
    )


def shared_reader_pool(database: str) -> Engine:
    """Return one engine whose connections spread across the replica pool."""
    paths = [
        f"md:{database}?attach_mode=single&session_name=reader-{index}"
        for index in range(READER_POOL_SIZE)
    ]
    # Each new pooled connection takes the next path, so a pool of
    # READER_POOL_SIZE connections holds one connection per replica.
    return create_engine_from_paths(
        paths,
        connect_args={
            "config": {
                "motherduck_token": read_token() or os.environ["MOTHERDUCK_TOKEN"]
            }
        },
        pool_size=READER_POOL_SIZE,
        max_overflow=0,
        pool_pre_ping=True,
    )


def dashboard_query(engine: Engine, user_id: str) -> str:
    with engine.connect() as conn:
        orders, revenue = conn.execute(
            text("SELECT count(*), sum(amount) FROM orders WHERE customer = :user"),
            {"user": user_id},
        ).one()
    return f"{user_id}: {orders} orders, {revenue} revenue"


@contextmanager
def scratch_database(prefix: str, keep: bool = False) -> Iterator[str]:
    """Create a uniquely named MotherDuck database and drop it afterwards."""
    name = f"duckdb_sqlalchemy_{prefix}_{secrets.token_hex(4)}"
    workspace = create_motherduck_engine(database="md:")
    try:
        with workspace.begin() as conn:
            conn.execute(text(f"CREATE DATABASE {name}"))
        yield name
    finally:
        if keep:
            print(f"kept database {name}")
        else:
            with workspace.begin() as conn:
                conn.execute(text(f"DROP DATABASE IF EXISTS {name}"))
        workspace.dispose()


def main() -> None:
    if not os.getenv("MOTHERDUCK_TOKEN"):
        raise SystemExit("Set MOTHERDUCK_TOKEN before running this example")
    if read_token() is None:
        print(
            "MOTHERDUCK_READ_SCALING_TOKEN is not set: readers share the "
            "read-write Duckling and REFRESH DATABASE is skipped"
        )

    with scratch_database("read_scaling", keep="--keep" in sys.argv) as database:
        writer = create_motherduck_engine(
            database=f"md:{database}", attach_mode="single"
        )
        pool = shared_reader_pool(database)
        users = [f"customer-{index}" for index in range(8)]
        try:
            with writer.begin() as conn:
                conn.execute(
                    text(
                        "CREATE TABLE orders AS "
                        "SELECT 'customer-' || (i % 8) AS customer, "
                        "round(10 + (i % 97) * 1.5, 2) AS amount "
                        "FROM range(10000) AS t(i)"
                    )
                )
            # Publish the committed table to the read scaling replicas now
            # instead of waiting for the next automatic sync. Run the snapshot
            # after the write commits: inside the write transaction it
            # discards the uncommitted changes.
            with writer.connect() as conn:
                conn.execute(text(f"CREATE SNAPSHOT OF {database}"))

            if read_token() is not None:
                with pool.connect() as conn:
                    conn.execute(text(f"REFRESH DATABASE {database}"))

            # Simulate eight dashboard users loading at the same time, each
            # pinned to its own replica.
            with ThreadPoolExecutor(max_workers=len(users)) as executor:
                for line in executor.map(
                    lambda user: dashboard_query(engine_for_user(database, user), user),
                    users,
                ):
                    print(line)

            # Anonymous reads through the shared pool.
            with ThreadPoolExecutor(max_workers=READER_POOL_SIZE) as executor:
                totals = list(
                    executor.map(lambda user: dashboard_query(pool, user), users)
                )
            print(f"shared pool served {len(totals)} queries")
        finally:
            writer.dispose()
            pool.dispose()
            for user in users:
                engine_for_user(database, user).dispose()
            engine_for_user.cache_clear()


if __name__ == "__main__":
    main()
