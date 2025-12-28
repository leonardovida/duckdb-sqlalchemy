# Documentation

This folder contains focused, task-oriented guides that keep the main README short. Start with the section closest to your workflow and follow the links as needed.

## Guides

- `overview.md` - High-level overview and quick start
- `getting-started.md` - Minimal install + setup walkthrough
- `migration-from-duckdb-engine.md` - Migration guide from older dialects
- `connection-urls.md` - URL formats, helpers, and manual escaping
- `motherduck.md` - Connection patterns and MotherDuck-specific options
- `configuration.md` - `connect_args`, extension preloads, and filesystem registration
- `olap.md` - Table functions (`read_parquet`, `read_csv_auto`) and ATTACH examples
- `pandas-jupyter.md` - DataFrame registration and notebook usage
- `types-and-caveats.md` - Supported types, parameter binding, and gotchas
- `alembic.md` - Alembic integration notes
- `seo-checklist.md` - Docs indexability checklist

## Project references

- `../CHANGELOG.md` - Release notes
- `../ROADMAP.md` - Dialect upgrade roadmap and PR checklists

## Examples

The `examples/` directory contains runnable scripts. If you want a full end-to-end walkthrough, start with `examples/sqlalchemy_example.py`.
