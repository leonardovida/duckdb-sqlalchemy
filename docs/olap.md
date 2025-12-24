# OLAP workflows

DuckDB exposes analytics-friendly table functions like `read_parquet` and `read_csv_auto`. The helpers in `duckdb_sqlalchemy.olap` make these easy to use with SQLAlchemy.

```python
from sqlalchemy import select
from duckdb_sqlalchemy import read_parquet, read_csv_auto

parquet = read_parquet("data/events.parquet", columns=["event_id", "ts"])
stmt = select(parquet.c.event_id, parquet.c.ts)

csv = read_csv_auto("data/events.csv", columns=["event_id", "ts"])
stmt = select(csv.c.event_id, csv.c.ts)
```

## ATTACH for multi-database analytics

DuckDB can query across multiple databases in a single session:

```python
from sqlalchemy import create_engine, text

conn = create_engine("duckdb:///local.duckdb").connect()
conn.execute(text("ATTACH 'analytics.duckdb' AS analytics"))
rows = conn.execute(text("SELECT * FROM analytics.events LIMIT 10")).fetchall()
```

## Notes

- Column naming for table functions requires SQLAlchemy >= 1.4 (uses `table_valued`).
