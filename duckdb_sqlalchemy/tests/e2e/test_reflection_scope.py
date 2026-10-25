"""Scope and kind select relations before resolving shadowed names."""

from pathlib import Path

import pytest
from sqlalchemy import MetaData, Table, inspect, select
from sqlalchemy.engine.reflection import ObjectKind, ObjectScope

from .conftest import EngineFactory


@pytest.mark.parametrize(
    "names", [None, ["TABLE_SHADOW", "VIEW_SHADOW", "missing"], []]
)
@pytest.mark.parametrize(
    ("schema", "scope", "kind", "expected"),
    [
        (None, ObjectScope.DEFAULT, ObjectKind.ANY, ("main_table", "main_view")),
        (None, ObjectScope.TEMPORARY, ObjectKind.ANY, ("temp_view", "temp_table")),
        (None, ObjectScope.ANY, ObjectKind.ANY, ("temp_view", "temp_table")),
        (None, ObjectScope.ANY, ObjectKind.TABLE, ("main_table", "temp_table")),
        (None, ObjectScope.ANY, ObjectKind.VIEW, ("temp_view", "main_view")),
        ("main", ObjectScope.ANY, ObjectKind.ANY, ("main_table", "main_view")),
        ("main", ObjectScope.TEMPORARY, ObjectKind.ANY, ("temp_view", "temp_table")),
        ("app.main", ObjectScope.TEMPORARY, ObjectKind.ANY, ()),
        ("app.main", ObjectScope.ANY, ObjectKind.ANY, ("main_table", "main_view")),
        ("temp.main", ObjectScope.DEFAULT, ObjectKind.ANY, ()),
        ("temp.main", ObjectScope.ANY, ObjectKind.ANY, ("temp_view", "temp_table")),
        ('"other catalog".main', ObjectScope.ANY, ObjectKind.ANY, ("attached_table",)),
        ('"other.schema"', ObjectScope.TEMPORARY, ObjectKind.ANY, ()),
        ('"other.schema"', ObjectScope.ANY, ObjectKind.ANY, ("schema_table",)),
    ],
)
def test_multi_reflection_selects_scope_and_kind_before_shadowing(
    db_path: Path,
    make_engine: EngineFactory,
    schema: str | None,
    scope: ObjectScope,
    kind: ObjectKind,
    expected: tuple[str, ...],
    names: list[str] | None,
) -> None:
    engine = make_engine(db_path, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.exec_driver_sql("ATTACH ':memory:' AS \"other catalog\"")
        conn.exec_driver_sql('CREATE SCHEMA "other.schema"')
        locations = {
            "main_table": ("app.main.table_shadow", "TABLE"),
            "main_view": ("app.main.view_shadow", "VIEW"),
            "temp_view": ("temp.main.table_shadow", "TEMP VIEW"),
            "temp_table": ("temp.main.view_shadow", "TEMP TABLE"),
            "attached_table": ('"other catalog".main.table_shadow', "TABLE"),
            "schema_table": ('"other.schema".table_shadow', "TABLE"),
        }
        for label, (target, ddl_kind) in locations.items():
            if "VIEW" in ddl_kind:
                conn.exec_driver_sql(
                    f"CREATE {ddl_kind} {target} AS SELECT '{label}' AS {label}"
                )
                comment_kind = "VIEW"
            else:
                conn.exec_driver_sql(f"CREATE {ddl_kind} {target} ({label} VARCHAR)")
                conn.exec_driver_sql(f"INSERT INTO {target} VALUES ('{label}')")
                comment_kind = "TABLE"
            # DuckDB 1.3 loses a temp table's temporary flag after COMMENT ON.
            if not ddl_kind.startswith("TEMP"):
                conn.exec_driver_sql(f"COMMENT ON {comment_kind} {target} IS '{label}'")

        inspector = inspect(conn)
        options = dict(schema=schema, filter_names=names, scope=scope, kind=kind)
        columns = inspector.get_multi_columns(**options)
        comments = inspector.get_multi_table_comment(**options)
        table_options = inspector.get_multi_table_options(**options)
        expected_columns = {}
        for label in expected if names != [] else ():
            target, ddl_kind = locations[label]
            namespace, table_name = target.rsplit(".", 1)
            requested_name = table_name.upper() if names else table_name
            key = (schema, requested_name)
            expected_columns[key] = [label]
            # Explicit catalogs make readback independent of temp shadowing.
            reflected = Table(
                requested_name, MetaData(), schema=namespace, autoload_with=conn
            )
            assert conn.execute(select(reflected)).all() == [(label,)]
            expected_comment = None if ddl_kind.startswith("TEMP") else label
            assert comments[key] == {"text": expected_comment}
        assert {
            key: [column["name"] for column in value] for key, value in columns.items()
        } == expected_columns
        assert set(comments) == set(table_options) == set(expected_columns)
