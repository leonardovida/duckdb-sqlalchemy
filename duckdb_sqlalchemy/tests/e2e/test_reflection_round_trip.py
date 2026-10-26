"""Reflected tables recreate the same schema and hold the same data."""

from pathlib import Path

import pytest
from sqlalchemy import MetaData, Table, inspect, select, text
from sqlalchemy.engine.reflection import ObjectScope

from .conftest import EngineFactory

SOURCE_DDL = """
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE parent (id INTEGER PRIMARY KEY, code VARCHAR UNIQUE);
CREATE TABLE source (
    id BIGINT PRIMARY KEY,
    parent_id INTEGER REFERENCES parent (id),
    tiny TINYINT, small SMALLINT, huge HUGEINT, ubig UBIGINT, utiny UTINYINT,
    flag BOOLEAN NOT NULL DEFAULT false,
    ratio REAL, score DOUBLE, amount DECIMAL(18, 4), wide DECIMAL(38, 10),
    label VARCHAR DEFAULT 'none', payload BLOB, uid UUID, doc JSON,
    day DATE, at_time TIME, at_ts TIMESTAMP, at_tz TIMESTAMPTZ, span INTERVAL,
    feeling mood,
    tags VARCHAR[], matrix INTEGER[][], fixed DOUBLE[3],
    profile STRUCT(city VARCHAR, "zip code" INTEGER, geo STRUCT(lat DOUBLE, lon DOUBLE)),
    attrs MAP(VARCHAR, INTEGER[]),
    choice UNION(num INTEGER, str VARCHAR),
    CHECK (score >= 0)
);
COMMENT ON TABLE source IS 'every type';
COMMENT ON COLUMN source.label IS 'display label';
CREATE INDEX ix_source_day ON source (day);
INSERT INTO parent VALUES (1, 'a');
INSERT INTO source VALUES (
    1, 1, -128, 32767, 170141183460469231731687303715884105727,
    18446744073709551615, 255, true, 1.5, 2.25, 12345678901234.5678,
    1234567890123456789012345678.0123456789, 'ten', '\\x00\\xff'::BLOB,
    '00000000-0000-0000-0000-000000000001', '{"a": [1, null]}',
    DATE '2026-01-31', TIME '23:59:59.999999', TIMESTAMP '2026-01-31 01:02:03.456789',
    TIMESTAMPTZ '2026-01-31 01:02:03+00', INTERVAL 1 YEAR + INTERVAL 2 DAYS,
    'happy', ['x', NULL], [[1, 2], [3]], [1.0, 2.0, 3.0],
    {'city': 'Rome', 'zip code': 100, 'geo': {'lat': 41.9, 'lon': 12.5}},
    MAP {'k': [1, 2]}, union_value(str := 'text'),
);
INSERT INTO source (id, score) VALUES (2, 0);
"""


def _signature(conn, table: str) -> list:
    return conn.execute(
        text(
            "SELECT column_name, data_type, is_nullable FROM duckdb_columns() "
            "WHERE table_name = :t ORDER BY column_index"
        ),
        {"t": table},
    ).all()


def test_reflect_clone_and_copy_every_supported_type(
    make_engine: EngineFactory,
) -> None:
    engine = make_engine("duckdb:///:memory:")
    with engine.begin() as conn:
        conn.exec_driver_sql(SOURCE_DDL)

    reflected = MetaData()
    source = Table("source", reflected, autoload_with=engine)
    assert source.comment == "every type"
    assert source.c.label.comment == "display label"
    clone = source.to_metadata(reflected, name="clone")
    for index in clone.indexes:  # index names are schema-wide
        index.name = f"{index.name}_clone"
    clone.create(engine)

    with engine.begin() as conn:
        conn.execute(clone.insert().from_select(list(source.c.keys()), source.select()))
        assert _signature(conn, "clone") == _signature(conn, "source")
        for left, right in (("source", "clone"), ("clone", "source")):
            assert (
                conn.execute(
                    text(f"SELECT count(*) FROM (FROM {left} EXCEPT ALL FROM {right})")
                ).scalar_one()
                == 0
            )
        # Python values survive a reflected-type round trip unchanged.
        original = conn.execute(source.select().order_by(source.c.id)).all()
        copied = conn.execute(clone.select().order_by(clone.c.id)).all()
        assert copied == original

    def strip(value: dict) -> dict:
        return {key: item for key, item in value.items() if key != "name"}

    inspector = inspect(engine)
    for getter in ("get_pk_constraint", "get_foreign_keys", "get_check_constraints"):
        source_value = getattr(inspector, getter)("source")
        clone_value = getattr(inspector, getter)("clone")
        if isinstance(source_value, list):
            assert [strip(v) for v in clone_value] == [strip(v) for v in source_value]
        else:
            assert strip(clone_value) == strip(source_value)
    assert [i["column_names"] for i in inspector.get_indexes("clone")] == [["day"]]


@pytest.mark.parametrize(
    ("schema", "temporary", "expected"),
    [
        (None, False, "local"),
        (None, True, "temp"),
        ("main", True, "local"),
        ('"other.schema"', True, "schema"),
        ('"other catalog"."other.schema"', True, "attached"),
    ],
)
def test_multi_metadata_stays_with_the_selected_relation(
    db_path: Path,
    make_engine: EngineFactory,
    schema: str | None,
    temporary: bool,
    expected: str,
) -> None:
    engine = make_engine(db_path, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.exec_driver_sql("ATTACH ':memory:' AS \"other catalog\"")
        conn.exec_driver_sql('CREATE SCHEMA "other.schema"')
        conn.exec_driver_sql('CREATE SCHEMA "other catalog"."other.schema"')
        locations = [
            ("main", "local", ""),
            ('"other.schema"', "schema", ""),
            ('"other catalog"."other.schema"', "attached", ""),
        ]
        if temporary:
            locations.append(("temp.main", "temp", "TEMP"))
        for namespace, label, modifier in locations:
            target = f'{namespace}."Shared Table"'
            conn.exec_driver_sql(
                f"CREATE {modifier} TABLE {target} ("
                f"{label}_id INTEGER PRIMARY KEY, {label}_value VARCHAR UNIQUE, "
                f'payload STRUCT("field name" INTEGER[]), CHECK ({label}_id > 0))'
            )
            # Create in reverse name order to exercise reflection ordering.
            conn.exec_driver_sql(f"CREATE INDEX {label}_z ON {target} ({label}_id)")
            conn.exec_driver_sql(f"CREATE INDEX {label}_a ON {target} ({label}_value)")
            conn.exec_driver_sql(
                f"INSERT INTO {target} VALUES (1, '{label}', "
                "{'field name': [1, NULL]})"
            )

        requested = "SHARED TABLE"
        inspector = inspect(conn)
        options = dict(
            schema=schema,
            filter_names=[requested, "missing"],
            scope=ObjectScope.ANY,
        )
        key = (schema, requested)
        columns = inspector.get_multi_columns(**options)
        assert list(columns) == [key]
        assert [column["name"] for column in columns[key]] == [
            f"{expected}_id",
            f"{expected}_value",
            "payload",
        ]
        primary_keys = inspector.get_multi_pk_constraint(**options)
        assert list(primary_keys) == [key]
        assert primary_keys[key]["constrained_columns"] == [f"{expected}_id"]
        unique = inspector.get_multi_unique_constraints(**options)
        assert list(unique) == [key]
        assert [item["column_names"] for item in unique[key]] == [[f"{expected}_value"]]
        checks = inspector.get_multi_check_constraints(**options)
        assert list(checks) == [key]
        assert len(checks[key]) == 1
        assert f"{expected}_id" in checks[key][0]["sqltext"]
        indexes = inspector.get_multi_indexes(**options)
        assert list(indexes) == [key]
        assert [index["name"] for index in indexes[key]] == [
            f"{expected}_a",
            f"{expected}_z",
        ]
        assert [index["column_names"] for index in indexes[key]] == [
            [f"{expected}_value"],
            [f"{expected}_id"],
        ]
        # DuckDB also searches temp.main for a main-qualified SQL name. Use
        # the catalog explicitly when reading the persistent main table.
        read_schema = f'"{db_path.stem}".main' if schema == "main" else schema
        reflected = Table(requested, MetaData(), schema=read_schema, autoload_with=conn)
        assert conn.execute(select(reflected)).all() == [
            (1, expected, {"field name": [1, None]})
        ]
        for getter in (
            inspector.get_multi_columns,
            inspector.get_multi_pk_constraint,
            inspector.get_multi_unique_constraints,
            inspector.get_multi_check_constraints,
            inspector.get_multi_indexes,
        ):
            assert getter(schema=schema, filter_names=[], scope=ObjectScope.ANY) == {}


def test_multi_foreign_keys_stay_with_the_selected_schema(
    db_path: Path, make_engine: EngineFactory
) -> None:
    engine = make_engine(db_path, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.exec_driver_sql('CREATE SCHEMA "other schema"')
        for namespace, column in (("main", "local_id"), ('"other schema"', "other_id")):
            conn.exec_driver_sql(
                f"CREATE TABLE {namespace}.parent (id INTEGER PRIMARY KEY)"
            )
            conn.exec_driver_sql(
                f'CREATE TABLE {namespace}."Child Table" '
                f"({column} INTEGER REFERENCES {namespace}.parent(id))"
            )
        inspector = inspect(conn)
        for namespace, column in (("main", "local_id"), ('"other schema"', "other_id")):
            options = dict(schema=namespace, filter_names=["CHILD TABLE", "missing"])
            result = inspector.get_multi_foreign_keys(**options)
            assert list(result) == [(namespace, "CHILD TABLE")]
            foreign_key = result[(namespace, "CHILD TABLE")][0]
            assert foreign_key["constrained_columns"] == [column]
            assert foreign_key["referred_table"] == "parent"
            assert foreign_key["referred_columns"] == ["id"]
            assert (
                inspector.get_multi_foreign_keys(schema=namespace, filter_names=[])
                == {}
            )
