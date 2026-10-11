import decimal
import json
import warnings
from typing import Any, Callable, Dict, Generator, Tuple, Type, cast
from uuid import uuid4

from hypothesis import assume, example, given, settings
from hypothesis import strategies as st
from packaging.version import Version
from pytest import fixture, importorskip, mark
from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    Numeric,
    Sequence,
    String,
    Table,
    bindparam,
    func,
    inspect,
    literal_column,
    schema,
    select,
    text,
)
from sqlalchemy import cast as cast_
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import FLOAT, JSON

from .. import Dialect
from .._supports import duckdb_version, has_uhugeint_support
from ..datatypes import Map, Struct, Union, Variant, types


@mark.parametrize("coltype", types)
@mark.skipif(not has_uhugeint_support, reason="duckdb version too old")
def test_unsigned_integer_type(
    engine: Engine, session: Session, coltype: Type[Integer]
) -> None:
    Base = declarative_base()

    tname = "table"
    table = type(
        "Table",
        (Base,),
        {
            "__tablename__": tname,
            "id": Column(Integer, primary_key=True, default=0),
            "a": Column(coltype),
        },
    )
    Base.metadata.create_all(engine)

    has_table = cast(
        Callable[[str], bool],
        engine.has_table if hasattr(engine, "has_table") else inspect(engine).has_table,
    )

    assert has_table(tname)

    session.add(table(a=1))
    session.commit()

    assert session.query(table).one()


@mark.remote_data()
def test_raw_json(engine: Engine) -> None:
    importorskip("duckdb", "0.9.3.dev4040")

    with engine.connect() as conn:
        assert conn.execute(text("load json"))

        assert conn.execute(text("select {'Hello': 'world'}::JSON")).fetchone() == (
            {"Hello": "world"},
        )


@mark.remote_data()
def test_custom_json_serializer() -> None:
    def default(o: Any) -> Any:
        if isinstance(o, decimal.Decimal):
            return {"__tag": "decimal", "value": str(o)}

    def object_hook(pairs: Dict[str, Any]) -> Any:
        if pairs.get("__tag", None) == "decimal":
            return decimal.Decimal(pairs["value"])
        else:
            return pairs

    engine = create_engine(
        "duckdb://",
        json_serializer=json.JSONEncoder(default=default).encode,
        json_deserializer=json.JSONDecoder(object_hook=object_hook).decode,
    )

    Base = declarative_base()

    class Entry(Base):
        __tablename__ = "test_json"
        id = Column(Integer, Sequence("id_seq"), primary_key=True)
        data = Column(JSON, nullable=False)

    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        session = Session(bind=conn)

        data = {"hello": decimal.Decimal("42")}

        session.add(Entry(data=data))  # type: ignore[call-arg]
        session.commit()

        (res,) = session.execute(select(Entry)).one()

        assert res.data == data


def test_json(engine: Engine, session: Session) -> None:
    base = declarative_base()

    class Entry(base):
        __tablename__ = "test_json"

        id = Column(Integer, primary_key=True, default=0)
        meta = Column(JSON, nullable=False)

    base.metadata.create_all(bind=engine)

    session.add(Entry(meta={"hello": "world"}))  # type: ignore[call-arg]
    session.commit()

    result = session.query(Entry).one()

    assert result.meta == {"hello": "world"}


def test_table_autoload_reflects_json_enum_and_array_types(engine: Engine) -> None:
    with engine.begin() as con:
        con.execute(text("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"))
        con.execute(text("CREATE TABLE t (payload JSON, tags VARCHAR[], state mood)"))

    table = Table("t", MetaData(), autoload_with=engine)

    assert isinstance(table.c.payload.type, JSON)
    assert isinstance(table.c.tags.type, sqltypes.ARRAY)
    assert type(table.c.tags.type.item_type) is String
    assert isinstance(table.c.state.type, sqltypes.Enum)
    assert list(table.c.state.type.enums) == ["sad", "ok", "happy"]


def test_metadata_reflect_preserves_json_enum_and_array_types(engine: Engine) -> None:
    with engine.begin() as con:
        con.execute(text("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"))
        con.execute(text("CREATE TABLE t (payload JSON, tags VARCHAR[], state mood)"))

    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables["t"]

    assert isinstance(table.c.payload.type, JSON)
    assert isinstance(table.c.tags.type, sqltypes.ARRAY)
    assert type(table.c.tags.type.item_type) is String
    assert isinstance(table.c.state.type, sqltypes.Enum)
    assert list(table.c.state.type.enums) == ["sad", "ok", "happy"]


def test_variant_round_trip(engine: Engine, session: Session) -> None:
    importorskip("duckdb", "1.5.0")

    base = declarative_base()

    class Entry(base):
        __tablename__ = "test_variant"

        id = Column(Integer, primary_key=True, default=0)
        payload = Column(Variant)

    base.metadata.create_all(bind=engine)

    rows = [
        Entry(id=1, payload=42),
        Entry(id=2, payload="hello"),
        Entry(id=3, payload={"a": 1, "b": [1, 2]}),
        Entry(id=4, payload=[1, 2, 3]),
        Entry(id=5, payload=None),
    ]
    session.add_all(rows)  # type: ignore[arg-type]
    session.commit()

    result = session.query(Entry).order_by(Entry.id).all()

    assert [row.payload for row in result] == [
        42,
        "hello",
        {"a": 1, "b": [1, 2]},
        [1, 2, 3],
        None,
    ]


def test_variant_reflection(engine: Engine) -> None:
    importorskip("duckdb", "1.5.0")

    with engine.begin() as con:
        con.execute(text("CREATE TABLE t (payload VARIANT, tags VARIANT[])"))

    table = Table("t", MetaData(), autoload_with=engine)

    assert isinstance(table.c.payload.type, Variant)
    assert isinstance(table.c.tags.type, sqltypes.ARRAY)
    assert isinstance(table.c.tags.type.item_type, Variant)


def test_uuid(engine: Engine, session: Session) -> None:
    importorskip("duckdb", "0.7.1")
    base = declarative_base()

    class Entry(base):
        __tablename__ = "test_uuid"

        id = Column(UUID, primary_key=True, default=0)

    base.metadata.create_all(bind=engine)

    ident = uuid4()

    session.add(Entry(id=ident))  # type: ignore[call-arg]
    session.commit()

    result = session.query(Entry).one()

    assert result.id == ident


def test_double_reflection_uses_double_type(engine: Engine) -> None:
    importorskip("sqlalchemy", "2.0.0")

    with engine.begin() as con:
        con.execute(
            text(
                "CREATE TABLE t (double_col DOUBLE, float8_col FLOAT8, float4_col FLOAT4)"
            )
        )

    table = Table("t", MetaData(), autoload_with=engine)

    assert type(table.c.double_col.type) is sqltypes.DOUBLE
    assert type(table.c.float8_col.type) is sqltypes.DOUBLE
    # 4-byte floats stay 4 bytes when the reflected table is created again.
    assert type(table.c.float4_col.type) is sqltypes.REAL


def test_pk_and_index_reflection(engine: Engine) -> None:
    importorskip("sqlalchemy", "2.0.0")

    with engine.begin() as con:
        con.execute(
            text("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)")
        )
        con.execute(text("CREATE INDEX idx_t_name_age ON t(name, age)"))

    inspector = inspect(engine)
    pk = inspector.get_pk_constraint("t")
    indexes = inspector.get_indexes("t")
    multi_pk = dict(inspector.get_multi_pk_constraint(filter_names=["t"]))
    multi_indexes = dict(inspector.get_multi_indexes(filter_names=["t"]))

    assert pk["name"] == "t_id_pkey"
    assert pk["constrained_columns"] == ["id"]
    assert indexes == [
        {
            "name": "idx_t_name_age",
            "column_names": ["name", "age"],
            "unique": False,
        }
    ]
    assert multi_pk[(None, "t")]["constrained_columns"] == ["id"]
    assert multi_indexes[(None, "t")] == indexes


def test_pk_reflection_on_legacy_duckdb(engine: Engine) -> None:
    with engine.begin() as con:
        con.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR)"))

        inspector = inspect(con)
        pk = inspector.get_pk_constraint("t")
        multi_pk = dict(inspector.get_multi_pk_constraint(filter_names=["t"]))
        reflected = Table("t", MetaData(), autoload_with=con)

    expected_name = "t_id_pkey" if duckdb_version >= Version("1.1.0") else None
    assert pk == {"name": expected_name, "constrained_columns": ["id"]}
    assert multi_pk[(None, "t")] == pk
    assert [column.name for column in reflected.primary_key] == ["id"]


def test_all_types_reflection(engine: Engine) -> None:
    importorskip("sqlalchemy", "1.4.0")
    importorskip("duckdb", "0.5.1")

    with warnings.catch_warnings(record=True) as capture, engine.connect() as conn:
        conn.execute(text("create table t2 as select * from test_all_types()"))
        table = Table("t2", MetaData(), autoload_with=conn)
        for col in table.columns:
            name = col.name
            if name.endswith("_enum") and duckdb_version < Version("0.7.1"):
                continue
            if "tuple" in name or name == "empty_struct":
                # Struct needs at least one field
                assert col.type == sqltypes.NULLTYPE, name
            else:
                assert col.type != sqltypes.NULLTYPE, name
            if name in ("struct", "map", "union"):
                assert isinstance(col.type, (Struct, Map, Union)), name
        assert not capture


def test_tuple_reflection_is_deliberately_unsupported() -> None:
    reflected = Dialect()._reflect_duckdb_data_type(
        "TUPLE(INTEGER, VARCHAR)",
        None,
        {},
        "test tuple",
    )

    assert reflected == sqltypes.NULLTYPE


def test_timestamptz_ns_reflection() -> None:
    reflected = Dialect()._reflect_duckdb_data_type(
        "TIMESTAMPTZ_NS",
        None,
        {},
        "test timestamp",
    )

    assert isinstance(reflected, sqltypes.TIMESTAMP)
    assert reflected.timezone is True


def test_timezone_reflection_preserves_timezone_in_ddl(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE timezone_source "
                "(observed_at TIMESTAMPTZ, observed_time TIMETZ)"
            )
        )
        source = Table("timezone_source", MetaData(), autoload_with=conn)

    reflected_timestamp = source.c.observed_at.type
    assert isinstance(reflected_timestamp, sqltypes.TIMESTAMP)
    assert reflected_timestamp.timezone is True
    reflected_time = source.c.observed_time.type
    assert isinstance(reflected_time, sqltypes.TIME)
    assert reflected_time.timezone is True

    clone = source.to_metadata(MetaData(), name="timezone_clone")
    ddl = str(schema.CreateTable(clone).compile(dialect=engine.dialect))
    assert "TIMESTAMP WITH TIME ZONE" in ddl
    assert "TIME WITH TIME ZONE" in ddl


def test_nested_type_reflection_round_trips(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE nested_source ("
                'profile STRUCT(city VARCHAR, "zip code" INTEGER, '
                "amount DECIMAL(10, 2)), "
                "tags MAP(VARCHAR, INTEGER[]), "
                "choice UNION(num INTEGER, str VARCHAR), "
                'history STRUCT("at" TIMESTAMPTZ, v HUGEINT)[])'
            )
        )
        source = Table("nested_source", MetaData(), autoload_with=conn)

        profile = source.c.profile.type
        assert isinstance(profile, Struct)
        assert [name for name, _ in profile._fields or ()] == [
            "city",
            "zip code",
            "amount",
        ]
        assert isinstance(source.c.tags.type, Map)
        assert isinstance(source.c.choice.type, Union)
        history = source.c.history.type
        assert isinstance(history, sqltypes.ARRAY)
        assert isinstance(history.item_type, Struct)

        clone = source.to_metadata(MetaData(), name="nested_clone")
        clone.create(conn)
        columns = {
            row[0]: row[1]
            for row in conn.execute(
                text(
                    "SELECT column_name, data_type FROM duckdb_columns() "
                    "WHERE table_name = :name ORDER BY column_index"
                ),
                {"name": "nested_source"},
            )
        }
        cloned = {
            row[0]: row[1]
            for row in conn.execute(
                text(
                    "SELECT column_name, data_type FROM duckdb_columns() "
                    "WHERE table_name = :name ORDER BY column_index"
                ),
                {"name": "nested_clone"},
            )
        }
        assert cloned == columns

        conn.execute(
            clone.insert().values(
                profile={"city": "Ghent", "zip code": 9000, "amount": None},
                tags={"a": [1, 2]},
            )
        )
        row = conn.execute(select(clone.c.profile, clone.c.tags)).one()
        assert row.profile == {"city": "Ghent", "zip code": 9000, "amount": None}
        assert row.tags == {"a": [1, 2]}


def test_nested_types_with_enum_members_reflect_as_null_type(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE TYPE mood AS ENUM ('sad', 'happy')"))
        conn.execute(
            text(
                "CREATE TABLE nested_enum ("
                "s STRUCT(m mood), l STRUCT(m mood[]), k MAP(mood, INTEGER))"
            )
        )
        table = Table("nested_enum", MetaData(), autoload_with=conn)

    for column in table.columns:
        assert column.type == sqltypes.NULLTYPE, column.name


@mark.parametrize(
    "field, expected",
    [
        ("a INTEGER", ("a", "INTEGER")),
        ('"my field" VARCHAR', ("my field", "VARCHAR")),
        ('"say ""hi""" STRUCT(x INTEGER)', ('say "hi"', "STRUCT(x INTEGER)")),
    ],
)
def test_split_duckdb_field(field: str, expected: Any) -> None:
    assert Dialect()._split_duckdb_field(field) == expected


def test_nested_types(engine: Engine, session: Session) -> None:
    importorskip("duckdb", "0.5.0")  # nested types require at least duckdb 0.5.0
    base = declarative_base()

    class Entry(base):
        __tablename__ = "test_struct"

        id = Column(Integer, primary_key=True, default=0)
        struct = Column(Struct(fields={"name": String}))
        map = Column(Map(String, Integer))
        # union = Column(Union(fields={"name": String, "age": Integer}))

    base.metadata.create_all(bind=engine)

    struct_data = {"name": "Edgar"}
    map_data = {"one": 1, "two": 2}

    session.add(Entry(struct=struct_data, map=map_data))  # type: ignore[call-arg]
    session.commit()

    result = session.query(Entry).one()

    assert result.struct == struct_data
    assert result.map == map_data


@mark.parametrize("value", [None, {}, {"one": 1, "missing": None}])
def test_map_round_trip(engine: Engine, value: Any) -> None:
    table = Table("test_map_values", MetaData(), Column("value", Map(String, Integer)))
    table.create(engine)

    with engine.begin() as connection:
        connection.execute(table.insert().values(value=value))
        assert connection.execute(select(table.c.value)).scalar() == value


def test_double_nested_types(engine: Engine, session: Session) -> None:
    """Test for https://github.com/leonardovida/duckdb-sqlalchemy/issues/1138"""
    importorskip("duckdb", "0.5.0")  # nested types require at least duckdb 0.5.0
    base = declarative_base()

    class Entry(base):
        __tablename__ = "test_struct"

        id = Column(Integer, primary_key=True, default=0)
        outer = Column(Struct({"inner": Struct({"val": Integer})}))

    base.metadata.create_all(bind=engine)

    outer = {"inner": {"val": 42}}

    session.add(Entry(outer=outer))  # type: ignore[call-arg]
    session.commit()

    result = session.query(Entry).one()

    assert result.outer == outer


def test_div_is_floordiv(engine: Engine) -> None:
    test_table = Table(
        "test_table", MetaData(), Column("value", FLOAT), Column("eur2usd_rate", FLOAT)
    )

    stmt = test_table.c.value / test_table.c.eur2usd_rate

    assert str(stmt.compile(engine)) == "test_table.value / test_table.eur2usd_rate"


@fixture
def exact_engine() -> Generator[Engine, None, None]:
    engine = create_engine("duckdb:///:memory:", exact_decimal_division=True)
    try:
        yield engine
    finally:
        engine.dispose()


def _half_away_from_zero(value: decimal.Decimal, exponent: int) -> decimal.Decimal:
    context = decimal.Context(prec=100, rounding=decimal.ROUND_HALF_UP)
    return context.quantize(value, decimal.Decimal(1).scaleb(exponent))


@mark.parametrize(
    "left, right, type_, expected",
    [
        ("5.02797279921331664759", "0.71828182845904523537", Numeric(22, 20), "7"),
        ("1", "2.00", Numeric(10, 2), "0.5"),
        ("1", "3", Numeric(10, 2), "0.3333333333333333"),
        ("2", "3", Numeric(10, 2), "0.6666666666666667"),
        ("-2", "3", Numeric(10, 2), "-0.6666666666666667"),
        ("1", "0", Numeric(10, 2), None),
        ("1", "NULL", Numeric(10, 2), None),
    ],
)
def test_exact_decimal_division(
    exact_engine: Engine, left: str, right: str, type_: Numeric, expected: Any
) -> None:
    expr = cast_(literal_column(left), type_) / cast_(literal_column(right), type_)
    with exact_engine.connect() as conn:
        result = conn.scalar(select(expr))
    assert result == (None if expected is None else decimal.Decimal(expected))


def test_exact_decimal_division_uses_declared_column_types(
    exact_engine: Engine,
) -> None:
    table = Table(
        "ledger",
        MetaData(),
        Column("amount", Numeric(18, 6)),
        Column("rate", Numeric(12, 8)),
        Column("units", BigInteger),
        Column("total", Integer),
    )
    table.metadata.create_all(exact_engine)
    row = {
        "amount": decimal.Decimal("1234.567891"),
        "rate": decimal.Decimal("0.00000003"),
        "units": 7,
        "total": 10,
    }
    with exact_engine.begin() as conn:
        conn.execute(table.insert(), row)
        amount_per_rate, amount_per_unit, units_per_total = conn.execute(
            select(
                table.c.amount / table.c.rate,
                table.c.amount / table.c.units,
                table.c.units / table.c.total,
            )
        ).one()

    quotient = decimal.Context(prec=100).divide
    for value, numerator, denominator in [
        (amount_per_rate, row["amount"], row["rate"]),
        (amount_per_unit, row["amount"], decimal.Decimal(row["units"])),
    ]:
        assert isinstance(value, decimal.Decimal)
        exponent = value.as_tuple().exponent
        assert isinstance(exponent, int) and exponent <= -16
        assert value == _half_away_from_zero(quotient(numerator, denominator), exponent)
    # Integer division keeps DuckDB's DOUBLE result.
    assert units_per_total == decimal.Decimal("0.7")


def _many_decimals(divisor: Callable[[Any], Any] = lambda value: value) -> Any:
    type_ = Numeric(22, 20)
    return cast_(literal_column("5.02797279921331664759"), type_) / divisor(
        cast_(literal_column("0.71828182845904523537"), type_)
    )


def test_exact_decimal_division_keeps_double_without_declared_types(
    exact_engine: Engine,
) -> None:
    wide = Numeric(38, 10)  # the quotient may need 38 integer digits
    huge = cast_(literal_column("9999999999999999999999999999"), wide) / cast_(
        literal_column("0.0000000001"), wide
    )
    with exact_engine.connect() as conn:
        assert float(conn.scalar(select(huge))) == 1e38
        # A function hides the divisor's DECIMAL type, so DOUBLE is used.
        untyped = conn.scalar(select(_many_decimals(lambda d: func.nullif(d, 0))))
        assert untyped != 7


def test_decimal_division_is_double_by_default(engine: Engine) -> None:
    with engine.connect() as conn:
        assert conn.scalar(select(_many_decimals())) != 7


@st.composite
def _decimal_operand(draw: Any) -> Tuple[decimal.Decimal, int, int]:
    precision = draw(st.integers(1, 38))
    scale = draw(st.integers(0, precision))
    digits = draw(st.integers(1, precision))
    unscaled = draw(st.integers(-(10**digits) + 1, 10**digits - 1))
    value = decimal.Decimal(unscaled).scaleb(-scale, decimal.Context(prec=100))
    return value, precision, scale


@settings(max_examples=200, deadline=None)
@given(left=_decimal_operand(), right=_decimal_operand())
@example(  # result scale 38
    left=(decimal.Decimal("-0." + "9" * 38), 38, 38),
    right=(decimal.Decimal(7), 1, 0),
)
@example(  # largest exact quotient: 22 integer digits
    left=(decimal.Decimal("9" * 22), 22, 0),
    right=(decimal.Decimal("-0.0000000000000001"), 16, 16),
)
def test_exact_decimal_division_matches_python_decimal(
    left: Tuple[decimal.Decimal, int, int],
    right: Tuple[decimal.Decimal, int, int],
) -> None:
    (a, p1, s1), (b, p2, s2) = left, right
    assume(p1 - s1 + s2 <= 22)  # wider quotients keep DuckDB's DOUBLE
    expr = cast_(bindparam("a", a), Numeric(p1, s1)) / cast_(
        bindparam("b", b), Numeric(p2, s2)
    )
    with _EXACT_ENGINE.connect() as conn:
        result = conn.scalar(select(expr))
    if b == 0:
        assert result is None
        return
    assert isinstance(result, decimal.Decimal)
    exponent = result.as_tuple().exponent
    assert isinstance(exponent, int) and exponent <= -16
    exact = decimal.Context(prec=100).divide(a, b)
    assert result == _half_away_from_zero(exact, exponent)


# hypothesis examples share one engine instead of a function-scoped fixture
_EXACT_ENGINE = create_engine("duckdb:///:memory:", exact_decimal_division=True)
