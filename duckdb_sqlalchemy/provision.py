"""SQLAlchemy's upstream test provisioning hooks; imported only by its harness."""

from typing import Any

from sqlalchemy.testing import provision


@provision.temp_table_keyword_args.for_db("duckdb")
def _temporary_table_arguments(cfg: Any, engine: Any) -> dict[str, Any]:
    return {"prefixes": ["TEMPORARY"]}


@provision.post_configure_engine.for_db("duckdb")
def _create_test_schemas(url: Any, engine: Any, follower_ident: Any) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS test_schema_2")
