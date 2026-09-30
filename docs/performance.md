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
the reader context manager or result close releases ownership. A partially
consumed reader must be consumed or closed before another statement runs on
the same connection. Attempts to fetch rows from that active result raise
`InvalidRequestError`. A separate connection can run independent work.

Ordinary tuple results are buffered when another statement runs before they
are exhausted. That preserves their rows but can use substantial memory.
Fetch failures propagate instead of turning into empty successful results.

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
