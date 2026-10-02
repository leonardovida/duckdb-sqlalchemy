---
layout: default
title: Performance and exact values
---

# Performance and exact values

Choose the data path that matches the source and the required result shape.
Run the benchmarks on your hardware and representative data before changing
production thresholds.

## Ordinary and bulk inserts

SQLAlchemy's normal insert path handles bound parameters, defaults, expressions,
type decorators and RETURNING. For eligible multi-row inserts the dialect
registers Arrow data (or pandas as a fallback), then executes one
`INSERT ... SELECT`. The default `duckdb_copy_threshold` is 10000.

```python
with engine.begin() as conn:
    conn.execution_options(duckdb_copy_threshold=10000).execute(table.insert(), rows)
```

Set the threshold to 0 to use the ordinary path. Successful conversion is
prepared once and reused by execution. A batch with a large integer mixed
with floating-point values stays on the ordinary path to prevent rounding
during inference. The pandas fallback also declines NaN batches, because
DuckDB's pandas scan treats NaN as NULL while ordinary bindings preserve NaN.

## Direct Arrow ingestion

When data already has an Arrow schema, avoid creating Python row dictionaries
or a pandas DataFrame:

```python
import pyarrow as pa
from sqlalchemy import Column, BigInteger, MetaData, Table
from duckdb_sqlalchemy import insert_from_arrow

events = Table("events", MetaData(), Column("id", BigInteger))
with engine.begin() as conn:
    events.create(conn)
    insert_from_arrow(conn, events, pa.table({"id": [9007199254740993, None]}))
```

`insert_from_arrow(conn, table, data, columns=None)` accepts an Arrow Table,
RecordBatch or RecordBatchReader. Target columns correspond positionally to
the source schema; by default it uses the source names. Names are quoted,
including spaces and reserved words. A reader is consumed by insertion.
Temporary registration is removed on success or failure.

The connection controls commit and rollback. This API uses already-typed Arrow
values and DuckDB's target-column casts; SQLAlchemy bind processors and
client-side defaults do not run. Use ordinary SQLAlchemy inserts when those
processors or defaults are part of your data contract.

For typed reloads, `on_conflict="ignore"` keeps existing rows whose keys conflict.
`on_conflict="replace"` applies DuckDB's native `INSERT OR REPLACE` semantics.
Supply all required target columns for replacement, particularly NOT NULL
columns, and test the table's constraints. The default plain INSERT continues
to raise on conflicts. Conflict modes do not change transaction ownership.

Structured write statements with RETURNING buffer their returned rows to provide
an exact affected-row count for ORM version checks. Arrow RETURNING uses a materialized Arrow table; the bounded batch interface
is intended for SELECT results.

## Bounded Arrow reads

`duckdb_arrow=True` exposes both a full Arrow table and batch reads:

```python
from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execution_options(duckdb_arrow=True).execute(
        text("SELECT i FROM range(1000000) AS t(i)")
    )
    with result.batches(batch_size=65536) as reader:
        for batch in reader:
            process(batch)
```

`.batches()` returns a reader with `schema`, `read_next_batch()`,
`read_all()`, iteration and `close()`. Iteration holds one batch at a time.
`read_all()` materializes the remaining stream. Exhaustion, explicit close,
the reader context manager, result close or connection rollback/close releases ownership. A partially
consumed reader must be consumed or closed before another statement runs on
the same connection. Attempts to fetch rows from that active result raise
`InvalidRequestError`. A separate connection can run independent work.

Ordinary tuple results are buffered when another statement runs before they
are exhausted. That preserves their rows but can use substantial memory.
Fetch failures propagate instead of turning into empty successful results.

Arrow RETURNING uses the Arrow table's row count without constructing a
Python tuple copy. Automatic Python-row inserts retain ordinary execution for
SQL-side bind expressions and MAP columns (including lists of MAP), because
replacing their compiled SQL with inferred Arrow can change values or fail
casts. These batches favor correctness over the register optimization. Direct
typed Arrow ingestion remains available for already-typed SQL values.

Arrow inference checks original integer precision only in inferred floating
columns. Integer/string columns avoid the redundant Python safety scan, while
mixed integers/floats still decline lossy inference.

## Reflection

SQLAlchemy 2.1's multi-table existence checks use a catalog batch plus one
visibility query, regardless of the number of names. Bulk reflection filters
table/view kind and temporary/default scope before resolving shadows.
Enum metadata is queried only for enum columns.

## Reproduce timings and memory

```sh
pip install -e ".[dev]"
python benchmarks/run.py --rows 100000 --repeats 3 --output benchmark.json
```

The benchmark records startup, native and SQLAlchemy insert paths, bulk
registration, direct Arrow and pandas ingestion, tuple/Arrow/batch reads and
reflection. Each sample runs in a fresh process and checks exact stored values
above 2**53, row counts and aggregate checksums.

JSON contains dependency/platform versions, median and per-sample elapsed
times, Python allocations measured with tracemalloc, process peak RSS where
available and query counts. RSS includes native libraries and process setup;
Python allocation measurements exclude Arrow/DuckDB native allocations.
The timed direct Arrow case starts with an existing Arrow table, while Core
insert includes construction and processing of Python parameters. These
measurements represent different source-data paths.

CI runs a 10000-row smoke benchmark and compares existing paths against
reviewed baseline commit `6b41e798d7525a94176ada4e6a7ac1cb3d648df3`
on the same runner/dependencies. Raw JSON is uploaded as `benchmark-results`.
Correctness and the reflection query budget are required checks. Timing
differences are recorded; noisy CI timing alone does not fail a merge.

For large loads, COPY from Parquet can avoid Python value conversion entirely;
see [analytical queries and COPY helpers](olap). CSV serialization honors
single-character delimiter (`delim`, `delimiter` or `sep`), quote and escape
options. Prefer typed Arrow/Parquet for binary, nested and exact decimal data.

The [reviewed-contracts comparison](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/benchmarks/results/reviewed-contracts-2026-10-02.json)
uses Python 3.14.0, DuckDB 1.5.5, SQLAlchemy 2.0.52 and Arrow 25.0.1 on the same
host, with two warmups and five repeated samples per implementation:

| Workload | Released baseline median (range) | Fixed median (range) |
| --- | ---: | ---: |
| Convert 50,000 mapping rows × 8 integer columns | 82.995 ms (78.268–85.645) | 38.639 ms (32.951–40.667) |
| Arrow RETURNING, 100,000 rows × 2 columns | 67.327 ms (65.156–73.569) | 6.507 ms (6.225–6.685) |

These are 53.4% and 90.3% elapsed-time reductions for the measured workloads.
RETURNING Python peak allocation fell from 17,278,562 to 4,876 bytes.
Conversion excludes input-row construction. Count, boundary values and full
Arrow-table equality were verified. `tracemalloc` excludes native DuckDB/Arrow allocations, so this is not
a process-memory or general throughput claim. Raw samples and variation are in
the linked JSON.

## Profiling and query tags

SQLAlchemy's `echo=True` logs statements and parameters; use `hide_parameters=True`
when values should stay out of logs. For a DuckDB query profile, run these
commands on the same connection as the query and consume its result fully:

```python
with engine.connect() as conn:
    conn.exec_driver_sql("PRAGMA enable_profiling='json'")
    conn.exec_driver_sql("PRAGMA profiling_output='query-profile.json'")
    try:
        conn.exec_driver_sql("SELECT sum(i) FROM range(1000000) AS t(i)").all()
    finally:
        conn.exec_driver_sql("PRAGMA disable_profiling")
```

The JSON file records operators, timings and cardinalities. Compare identical
queries, data and DuckDB settings. Profiles contain SQL text, and the output
file is written outside transaction rollback.

A fixed SQL comment can identify a workload without changing parameter values:

```python
from sqlalchemy import event

@event.listens_for(engine, "before_cursor_execute", retval=True)
def tag_query(conn, cursor, statement, parameters, context, executemany):
    return "/* workload: nightly-rollup */ " + statement, parameters
```

Use a constant tag or validate dynamic labels before including them in SQL.
Commented writes retain the dialect's DML row counts.

## Recorded comparison

A same-runner CI smoke comparison at commit
`4a8e2c00426322c4e6e688aa816c3a6d04e2acd4` used 10000 rows and two
fresh-process samples per case. SQLAlchemy, DuckDB, pandas and Arrow versions
are recorded in the [raw benchmark job](https://github.com/leonardovida/duckdb-sqlalchemy/actions/runs/36757147809/job/110030166866).

| Path | Reviewed baseline | This sweep |
| --- | ---: | ---: |
| Core inserts | 0.316 s | 0.317 s |
| Bulk registration | 0.183 s | 0.194 s |
| pandas registration | 0.061 s | 0.089 s |
| Batched existence reflection | 0.214 s / 201 queries | 0.013 s / 2 queries |
| Direct typed Arrow inserts | unavailable | 0.025 s |

Reflection improved roughly 17 times in this sample. Core insert speed was
unchanged; bulk and pandas paths pay for checks that prevent silent coercion.
Direct Arrow avoids Python parameter construction when the source is already
typed Arrow. Two samples are a smoke comparison; rerun on representative
production data before treating timing differences as stable estimates.
