---
layout: default
title: duckdb-sqlalchemy documentation
---

# duckdb-sqlalchemy documentation

`duckdb-sqlalchemy` is the SQLAlchemy dialect for DuckDB and MotherDuck. Install
it with `pip install duckdb-sqlalchemy` and connect with
`create_engine("duckdb:///path/to/file.duckdb")` or
`create_engine("duckdb:///md:my_db")`.

## Start here

| I want to | Guide |
| --- | --- |
| See what the dialect provides | [Overview](overview) |
| Install it and run a first query | [Getting started](getting-started) |
| Move from `duckdb_engine` | [Migration from duckdb_engine](migration-from-duckdb-engine) |

## Connect and configure

| I want to | Guide |
| --- | --- |
| Write a URL for a file, `:memory:` or MotherDuck | [Connection URLs](connection-urls) |
| Set DuckDB settings, extensions, filesystems or the pool | [Configuration](configuration) |
| Set up MotherDuck tokens, attach modes, reader pools and retries | [MotherDuck](motherduck) |
| Build and deploy MotherDuck Dives from Python | [MotherDuck Dives](dives) |

## Work with data

| I want to | Guide |
| --- | --- |
| Query Parquet and CSV files, load rows with COPY, or attach databases | [OLAP workflows](olap) |
| Use DataFrames or notebooks | [Pandas and Jupyter](pandas-jupyter) |
| Choose an insert or Arrow path and measure it | [Performance and exact values](performance) |
| Run schema migrations | [Alembic integration](alembic) |

## Reference

| I want to | Guide |
| --- | --- |
| Look up a type, an execution option, reflection rules or an unsupported statement | [Types and caveats](types-and-caveats) |
| Check what changed in a version before upgrading | [Breaking and behavior changes](breaking-changes) |
| See which tools use the dialect | [Ecosystem adoption](adoption) |

## Examples

See [examples/](https://github.com/leonardovida/duckdb-sqlalchemy/tree/main/examples)
for runnable scripts, including MotherDuck reader pools, concurrent writers and
Dive deployment.
