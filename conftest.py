import os

pytest_plugins = (
    ["sqlalchemy.testing.plugin.pytestplugin"] if os.getenv("DUCKDB_SQLA_SUITE") else []
)
