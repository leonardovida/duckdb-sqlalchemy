"""Reflected tables recreate the same schema and hold the same data."""

from sqlalchemy import MetaData, Table, inspect, text

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
