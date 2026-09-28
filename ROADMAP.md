# Project Tracker

Last updated: 2026-09-28

## MotherDuck performance plan (duckdb-sqlalchemy)

### Data movement performance
- [ ] Optimize _bulk_insert_via_register (avoid copies)
- [x] Add COPY from parquet/csv examples (`bulk.py`, `docs/olap.md`)
- [ ] Explore appender/chunked ingest improvements

### Observability
- [ ] Example: DuckDB logging/profiling
- [ ] Lightweight query tagging guidance
