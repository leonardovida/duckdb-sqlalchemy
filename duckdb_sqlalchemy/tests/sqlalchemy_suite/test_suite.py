import re

from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    func,
    inspect,
    literal_column,
    testing,
    text,
)
from sqlalchemy.sql import sqltypes
from sqlalchemy.testing import mock
from sqlalchemy.testing.suite import *  # noqa: F401,F403
from sqlalchemy.testing.suite import ComponentReflectionTest as _Components
from sqlalchemy.testing.suite import ComponentReflectionTestExtra as _Reflection
from sqlalchemy.testing.suite import CTETest as _CTE
from sqlalchemy.testing.suite import JoinTest as _Join
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNames


class CTETest(_CTE):
    @classmethod
    def define_tables(cls, metadata):
        # DuckDB rejects self-referencing inserts; CTE behavior is independent
        # of a physical foreign-key constraint on this hierarchy.
        for name in ("some_table", "some_other_table"):
            Table(
                name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("data", String(50)),
                Column("parent_id", Integer),
            )


class ComponentReflectionTestExtra(_Reflection):
    @testing.combinations(
        sqltypes.String, sqltypes.VARCHAR, sqltypes.CHAR, argnames="type_"
    )
    @testing.requires.table_reflection
    def test_string_length_reflection(self, connection, metadata, type_):
        # DuckDB canonicalizes CHAR/VARCHAR(n) to unbounded VARCHAR.
        reflected = self._type_round_trip(connection, metadata, type_(52))[0]
        assert isinstance(reflected, sqltypes.String)
        assert reflected.length is None

    @testing.combinations(
        (Integer, text("10"), r"'?10'?"),
        (Integer, "10", r"'?10'?"),
        (Boolean, literal_column("true"), r"CASTtASBOOLEAN|true"),
        (Integer, text("3 + 5"), r"3\+5"),
        (Integer, text("(3 * 5)"), r"3\*5"),
        (sqltypes.DateTime, func.now(), r"current_timestamp|now|getdate"),
        (Integer, literal_column("3") + literal_column("5"), r"3\+5"),
        argnames="datatype, default, expected_reg",
    )
    @testing.requires.server_defaults
    def test_server_defaults(
        self, metadata, connection, datatype, default, expected_reg
    ):
        table = Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("thecol", datatype, server_default=default),
        )
        table.create(connection)
        reflected = inspect(connection).get_columns("t")[1]["default"]
        sanitized = re.sub(r"[\\(\\) \\']", "", reflected)
        assert re.match(expected_reg, sanitized, re.IGNORECASE)


class ComponentReflectionTest(_Components):
    def _adjust_sort(self, result, expected, key):
        # DuckDB generates names but returns constraints in declaration order.
        for objects in (result, expected):
            for constraints in objects.values():
                constraints.sort(key=key)

    def _without_constraint_comments(self, expected):
        for value in expected.values():
            for constraint in value if isinstance(value, list) else [value]:
                constraint["comment"] = None
        return expected

    def exp_pks(self, *args, **kwargs):
        return self._without_constraint_comments(super().exp_pks(*args, **kwargs))

    def exp_fks(self, *args, **kwargs):
        return self._without_constraint_comments(super().exp_fks(*args, **kwargs))

    def exp_ucs(self, *args, **kwargs):
        expected = self._without_constraint_comments(super().exp_ucs(*args, **kwargs))
        for constraints in expected.values():
            for constraint in constraints:
                constraint["name"] = mock.ANY
        return expected

    def exp_ccs(self, *args, **kwargs):
        return self._without_constraint_comments(super().exp_ccs(*args, **kwargs))

    @testing.requires.schema_reflection
    def test_get_schema_names(self, connection):
        # Existing public contract uses qualified catalog.schema names.
        database = connection.exec_driver_sql("SELECT current_database()").scalar_one()
        assert (
            f"{database}.{testing.config.test_schema}"
            in inspect(connection).get_schema_names()
        )

    @testing.requires.schema_reflection
    def test_get_schema_names_w_translate_map(self, connection):
        translated = connection.execution_options(schema_translate_map={"foo": "bar"})
        self.test_get_schema_names(translated)

    @testing.requires.temp_table_reflection
    def test_reflect_table_temp_table(self, connection):
        table = self.tables[self.temp_table_name()]
        reflected = Table(table.name, MetaData(), autoload_with=connection)
        assert [(c.name, c.nullable) for c in reflected.c] == [
            (c.name, c.nullable) for c in table.c
        ]
        assert isinstance(reflected.c.name.type, String)
        assert reflected.c.name.type.length is None

    @testing.combinations(
        False, (True, testing.requires.schemas), argnames="use_schema"
    )
    @testing.requires.unique_constraint_reflection
    def test_get_unique_constraints(self, connection, metadata, use_schema):
        schema = testing.config.test_schema if use_schema else None
        table = Table(
            "unique_columns",
            metadata,
            Column("a", Integer),
            Column("b", Integer),
            Column("c", Integer),
            UniqueConstraint("a", "b", name="original_name"),
            UniqueConstraint("c", name="i have spaces"),
            schema=schema,
        )
        table.create(connection)
        reflected = inspect(connection).get_unique_constraints(
            table.name, schema=schema
        )
        assert {tuple(c["column_names"]) for c in reflected} == {("a", "b"), ("c",)}
        assert all(c["name"] for c in reflected)

    @testing.combinations(
        "uq_email", "UQ_email", "mixedCaseUQ", "uq.with.dots", argnames="name"
    )
    @testing.requires.unique_constraint_reflection
    def test_get_unique_constraints_quoted_name(self, connection, metadata, name):
        table = Table(
            "test_table",
            metadata,
            Column("email", String),
            UniqueConstraint("email", name=name),
        )
        table.create(connection)
        constraint = inspect(connection).get_unique_constraints(table.name)[0]
        assert constraint["column_names"] == ["email"]
        assert constraint["name"] == "test_table_email_key"

    @testing.requires.schema_reflection
    @testing.requires.schema_create_delete
    def test_schema_cache(self, connection):
        inspector = inspect(connection)
        database = connection.exec_driver_sql("SELECT current_database()").scalar_one()
        name = f"{database}.foo_bar"
        assert name not in inspector.get_schema_names()
        assert not inspector.has_schema("foo_bar")
        connection.exec_driver_sql("CREATE SCHEMA foo_bar")
        try:
            assert name not in inspector.get_schema_names()
            assert not inspector.has_schema("foo_bar")
            inspector.clear_cache()
            assert name in inspector.get_schema_names()
            assert inspector.has_schema("foo_bar")
        finally:
            connection.exec_driver_sql("DROP SCHEMA foo_bar")


class JoinTest(_Join):
    # Native FK checks cannot delete children and parents in one transaction.
    # Recreate the tables so every join still exercises real FK inference.
    run_create_tables = "each"


class LongNameBlowoutTest(_LongNames):
    @testing.combinations("fk", "pk", "ix", "ck", "uq", argnames="type_")
    def test_long_convention_name(self, type_, metadata, connection):
        original, reflected = getattr(self, type_)(metadata, connection)
        assert len(original) > 255
        if reflected is None:
            assert type_ == "fk"
            reflected = inspect(connection).get_foreign_keys(
                "b_related_things_of_value"
            )[0]["name"]
        assert reflected
        if type_ == "ix":
            assert original.startswith(reflected[:-5])
        else:
            # DuckDB regenerates constraint names instead of storing DDL names.
            assert reflected != original
