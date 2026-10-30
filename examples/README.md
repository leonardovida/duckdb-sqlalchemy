# Examples

Runnable scripts that show duckdb-sqlalchemy end to end. Run them from the
repository root with `uv run python examples/<name>.py`, or with `python`
after `pip install -e ".[dev]"`.

## Local DuckDB

| Script | Shows |
| --- | --- |
| [`local_quickstart.py`](local_quickstart.py) | ORM models on a DuckDB file, aggregates, and a Parquet export queried as a table |
| [`partitioned_parquet.py`](partitioned_parquet.py) | Exporting a filtered snapshot to Hive-partitioned Parquet and reading it back |
| [`snapshot_live_join.py`](snapshot_live_join.py) | Joining a Parquet snapshot to a live table |

## MotherDuck

These scripts need `MOTHERDUCK_TOKEN`. Each one creates a database named
`duckdb_sqlalchemy_<example>_<random>` and drops it when it finishes, so they
are safe to run against a real account. Pass `--keep` to keep what the example
created and inspect it in the MotherDuck UI.

| Script | Shows |
| --- | --- |
| [`motherduck_quickstart.py`](motherduck_quickstart.py) | Creating and loading a database through the ORM, then reading it as rows and as Arrow |
| [`motherduck_concurrent_writers.py`](motherduck_concurrent_writers.py) | Several processes writing the same database on one Duckling, with retries for conflicting updates |
| [`motherduck_read_scaling.py`](motherduck_read_scaling.py) | Reader pools on read scaling replicas: one engine per end user, a shared pool across replicas, and `CREATE SNAPSHOT` for fresh reads |
| [`motherduck_dive.py`](motherduck_dive.py) | Building the data for a Dive and deploying the Dive from Python, with versions and a release status |

`motherduck_read_scaling.py` also reads `MOTHERDUCK_READ_SCALING_TOKEN`.
Without it, every reader runs on the read-write Duckling, so the example runs
but does not exercise the replicas.

See the [MotherDuck guide](https://leonardovida.github.io/duckdb-sqlalchemy/motherduck.html)
and the [Dives guide](https://leonardovida.github.io/duckdb-sqlalchemy/dives.html)
for the patterns behind these scripts.
