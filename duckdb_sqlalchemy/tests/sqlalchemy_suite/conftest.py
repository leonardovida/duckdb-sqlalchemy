import os

import pytest

if not os.getenv("DUCKDB_SQLA_SUITE"):
    pytest.skip(
        "SQLAlchemy suite tests are disabled. Set DUCKDB_SQLA_SUITE=1 to enable.",
        allow_module_level=True,
    )

pytest_plugins = "sqlalchemy.testing.plugin.pytestplugin"
