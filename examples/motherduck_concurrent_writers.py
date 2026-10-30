"""Write to one MotherDuck database from several processes at once.

Every connection made with the same read-write token runs on the same
Duckling, so separate processes (workers, orchestrator tasks, web servers)
share one DuckDB instance in the cloud. DuckDB checks for conflicts when a
transaction commits:

- Inserts from different processes do not conflict, so appends run side by side.
- When two transactions update the same row, the first commit wins and the
  others fail with "Conflict on update". Retrying the whole transaction is safe.

Requires MOTHERDUCK_TOKEN. The example creates a database named
duckdb_sqlalchemy_writers_<random> and drops it at the end. Pass --keep to keep it.

Run from the repository: uv run python examples/motherduck_concurrent_writers.py
"""

import multiprocessing
import os
import random
import secrets
import sys
import time
from contextlib import contextmanager
from typing import Callable, Iterator, Tuple, TypeVar

import duckdb
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import DBAPIError

from duckdb_sqlalchemy import create_motherduck_engine

WORKERS = 4
ROWS_PER_WORKER = 5_000
INCREMENTS_PER_WORKER = 5

T = TypeVar("T")


def is_write_conflict(exc: DBAPIError) -> bool:
    return isinstance(exc.orig, duckdb.TransactionException) and (
        "conflict" in str(exc.orig).lower()
    )


def run_with_conflict_retry(
    engine: Engine,
    work: Callable[[Connection], T],
    attempts: int = 10,
) -> Tuple[T, int]:
    """Run work in a transaction, retrying it when another writer won a conflict.

    The failed transaction is rolled back before the retry, so work must
    re-read whatever it depends on rather than reuse values from the attempt
    that lost. Returns the result and the number of retries.
    """
    for attempt in range(1, attempts + 1):
        try:
            with engine.begin() as conn:
                return work(conn), attempt - 1
        except DBAPIError as exc:
            if not is_write_conflict(exc) or attempt == attempts:
                raise
            # Jitter keeps the losing writers from colliding again in lockstep.
            time.sleep(random.uniform(0.05, 0.2) * attempt)
    raise AssertionError("unreachable")


def writer(database: str, worker_id: int) -> Tuple[int, int]:
    """One writer process: append a batch, then bump a shared counter."""
    # Each process creates its own engine. DuckDB connections cannot cross
    # process boundaries, and MotherDuck engines use NullPool by default.
    engine = create_motherduck_engine(database=f"md:{database}", attach_mode="single")
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO events "
                    "SELECT :worker, i, now() FROM range(:rows) AS t(i)"
                ),
                {"worker": worker_id, "rows": ROWS_PER_WORKER},
            )

        retries = 0
        for _ in range(INCREMENTS_PER_WORKER):
            _, lost = run_with_conflict_retry(
                engine,
                lambda conn: conn.execute(
                    text("UPDATE counters SET value = value + 1 WHERE name = 'jobs'")
                ),
            )
            retries += lost
        return worker_id, retries
    finally:
        engine.dispose()


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
    # The dialect reads either spelling of the token variable.
    if not (os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")):
        raise SystemExit("Set MOTHERDUCK_TOKEN before running this example")

    with scratch_database("writers", keep="--keep" in sys.argv) as database:
        engine = create_motherduck_engine(
            database=f"md:{database}", attach_mode="single"
        )
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "CREATE TABLE events "
                        "(worker INTEGER, seq BIGINT, created_at TIMESTAMPTZ)"
                    )
                )
                conn.execute(
                    text(
                        "CREATE TABLE counters (name VARCHAR PRIMARY KEY, value INTEGER)"
                    )
                )
                conn.execute(text("INSERT INTO counters VALUES ('jobs', 0)"))

            # The DuckDB Python client is not fork-safe. Use spawn (or
            # forkserver) and create engines inside each child process.
            context = multiprocessing.get_context("spawn")
            with context.Pool(WORKERS) as pool:
                results = pool.starmap(
                    writer, [(database, worker) for worker in range(WORKERS)]
                )
            for worker_id, retries in results:
                print(f"worker {worker_id}: retried {retries} conflicting updates")

            with engine.connect() as conn:
                rows, writers = conn.execute(
                    text("SELECT count(*), count(DISTINCT worker) FROM events")
                ).one()
                jobs = conn.execute(
                    text("SELECT value FROM counters WHERE name = 'jobs'")
                ).scalar_one()
            print(f"{rows} rows appended by {writers} processes")
            print(f"counter = {jobs} (expected {WORKERS * INCREMENTS_PER_WORKER})")
            assert rows == WORKERS * ROWS_PER_WORKER
            assert jobs == WORKERS * INCREMENTS_PER_WORKER
        finally:
            engine.dispose()


if __name__ == "__main__":
    main()
