# Project Tracker

Last updated: 2026-09-30

## MotherDuck performance plan (duckdb-sqlalchemy)

### Data movement performance
- [x] Reuse prepared bulk conversion and avoid retained positional transposition
- [x] Direct typed Arrow ingestion and bounded Arrow result batches
- [x] Exact numeric/NaN fallback and differential semantic validation
- [x] Reproducible benchmarks against the reviewed baseline
- [x] Add COPY from parquet/csv examples (`bulk.py`, `docs/olap.md`)
- [x] Arrow RecordBatchReader ingestion for chunked sources
- [ ] Evaluate native appender APIs against benchmark and bind-semantics requirements

### Observability
- [x] Example: DuckDB profiling and SQLAlchemy logging
- [x] Lightweight query tagging guidance
