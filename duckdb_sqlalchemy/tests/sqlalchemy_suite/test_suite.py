from sqlalchemy.testing.suite import *  # noqa: F401,F403

from sqlalchemy import Boolean, Column, Integer, String, Table, func, literal_column, text
from sqlalchemy import testing
from sqlalchemy.testing.suite import ComponentReflectionTestExtra as _Reflection
from sqlalchemy.testing.suite import CTETest as _CTE
from sqlalchemy.sql import sqltypes


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
    @testing.combinations(sqltypes.String, sqltypes.VARCHAR, sqltypes.CHAR, argnames="type_")
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
    def test_server_defaults(self, metadata, connection, datatype, default, expected_reg):
        super().test_server_defaults(metadata, connection, datatype, default, expected_reg)
