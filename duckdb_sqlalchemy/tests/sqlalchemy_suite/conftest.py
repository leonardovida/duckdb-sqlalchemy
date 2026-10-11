import os

import pytest

if not os.getenv("DUCKDB_SQLA_SUITE"):
    pytest.skip(
        "SQLAlchemy suite tests are disabled. Set DUCKDB_SQLA_SUITE=1 to enable.",
        allow_module_level=True,
    )


# Run after SQLAlchemy's plugin, which rebuilds items per backend.
@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items):
    # DuckDB divides DECIMAL operands as DOUBLE, so the 20-digit quotient that
    # SQLAlchemy 2.1.4 expects to be exactly 7 comes back as 7.000000000000001.
    skip = pytest.mark.skip(reason="DuckDB DECIMAL division returns DOUBLE")
    for item in items:
        if item.name.startswith("test_truediv_divisor[numeric_many_decimals-"):
            item.add_marker(skip)
