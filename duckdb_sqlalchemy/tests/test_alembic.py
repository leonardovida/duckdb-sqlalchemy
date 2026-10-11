import io
import subprocess
import sys
import textwrap
from typing import Any, List

import pytest
from sqlalchemy import (
    ARRAY,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    func,
    inspect,
    select,
    true,
)
from sqlalchemy.engine import Engine

pytest.importorskip("alembic")

from alembic.autogenerate import (  # noqa: E402
    compare_metadata,
    produce_migrations,
    render_python_code,  # noqa: E402
)
from alembic.autogenerate.api import AutogenContext  # noqa: E402
from alembic.autogenerate.render import _repr_type  # noqa: E402
from alembic.migration import MigrationContext  # noqa: E402
from alembic.operations import Operations  # noqa: E402

from ..alembic_impl import DuckDBImpl, _normalize_default  # noqa: E402
from ..datatypes import Map, Struct, Union  # noqa: E402


def _metadata(
    *,
    age_default: str = "18",
    named_unique: bool = True,
    extra_unique: bool = False,
    profile: Any = None,
) -> MetaData:
    metadata = MetaData()
    columns: List[Any] = [
        Column("id", Integer, primary_key=True),
        Column("email", String, comment="login email"),
        Column("code", String),
        Column("age", Integer, server_default=age_default),
        Column("active", Boolean, server_default=true()),
        Column("created_at", DateTime, server_default=func.now()),
        Column("price", Numeric(10, 2), server_default="1.50"),
        Column("profile", profile or Struct({"city": String, "zip": Integer})),
        Column("tags", Map(String, Integer)),
        Column("choice", Union({"num": Integer, "str": String})),
        Column("history", ARRAY(Struct({"at": DateTime, "v": Integer}))),
        CheckConstraint("age >= 0", name="ck_users_age"),
    ]
    if named_unique:
        columns.append(UniqueConstraint("email", name="uq_users_email"))
    if extra_unique:
        columns.append(UniqueConstraint("code", name="uq_users_code"))
    Table("users", metadata, *columns, comment="app users")
    return metadata


def _context(conn: Any, metadata: MetaData) -> MigrationContext:
    return MigrationContext.configure(
        conn,
        opts={
            "target_metadata": metadata,
            "compare_server_default": True,
            "compare_type": True,
        },
    )


def _diff_kinds(conn: Any, metadata: MetaData) -> List[str]:
    kinds = []
    for diff in compare_metadata(_context(conn, metadata), metadata):
        kinds.append(diff[0][0] if isinstance(diff, list) else diff[0])
    return kinds


def test_dialect_uses_duckdb_impl(engine: Engine) -> None:
    with engine.connect() as conn:
        assert isinstance(MigrationContext.configure(conn).impl, DuckDBImpl)


def test_autogenerate_is_empty_after_create_all(engine: Engine) -> None:
    with engine.begin() as conn:
        _metadata().create_all(conn)
        assert compare_metadata(_context(conn, _metadata()), _metadata()) == []


def test_autogenerate_reports_real_changes(engine: Engine) -> None:
    with engine.begin() as conn:
        _metadata().create_all(conn)
        assert _diff_kinds(conn, _metadata(age_default="21")) == ["modify_default"]
        assert _diff_kinds(conn, _metadata(named_unique=False)) == ["remove_constraint"]
        assert _diff_kinds(conn, _metadata(extra_unique=True)) == ["add_constraint"]
        wider = Struct({"city": String, "zip": Integer, "country": String})
        assert _diff_kinds(conn, _metadata(profile=wider)) == ["modify_type"]
        assert _diff_kinds(conn, _metadata(profile=Map(String, Integer))) == [
            "modify_type"
        ]


def test_render_nested_types_as_valid_python(engine: Engine) -> None:
    metadata = _metadata()
    with engine.connect() as conn:
        context = _context(conn, metadata)
        migration = produce_migrations(context, metadata)
        code = render_python_code(
            migration.upgrade_ops,  # type: ignore[arg-type]
            migration_context=context,
        )
        autogen_context = AutogenContext(
            context,
            opts={
                "sqlalchemy_module_prefix": "sa.",
                "alembic_module_prefix": "op.",
                "user_module_prefix": None,
            },
        )
        rendered = _repr_type(Map(String, Integer), autogen_context)

    assert (
        "duckdb_sqlalchemy.datatypes.Struct({'city': sa.String(), "
        "'zip': sa.Integer()})" in code
    )
    assert "duckdb_sqlalchemy.datatypes.Map(sa.String(), sa.Integer())" in code
    assert (
        "sa.ARRAY(duckdb_sqlalchemy.datatypes.Struct({'at': sa.DateTime(), "
        "'v': sa.Integer()}))" in code
    )
    assert rendered == "duckdb_sqlalchemy.datatypes.Map(sa.String(), sa.Integer())"
    assert "import duckdb_sqlalchemy.datatypes" in autogen_context.imports
    compile(f"def upgrade():\n{code}", "<migration>", "exec")


def test_alter_column_comment(engine: Engine) -> None:
    with engine.begin() as conn:
        _metadata().create_all(conn)
        op = Operations(MigrationContext.configure(conn))
        op.alter_column("users", "code", comment="it's a code", existing_type=String)
        op.alter_column("users", "email", comment=None, existing_type=String)
        comments = {
            column["name"]: column["comment"]
            for column in inspect(conn).get_columns("users")
        }

    assert comments["code"] == "it's a code"
    assert comments["email"] is None


def test_offline_migration_sql() -> None:
    buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="duckdb", opts={"as_sql": True, "output_buffer": buffer}
    )
    op = Operations(context)
    op.create_table(
        "items",
        Column("id", Integer, primary_key=True),
        Column("sku", String, comment="stock keeping unit"),
    )
    op.alter_column("items", "sku", comment="SKU", existing_type=String)
    sql = buffer.getvalue()

    assert isinstance(context.impl, DuckDBImpl)
    assert "CREATE SEQUENCE IF NOT EXISTS items_id_seq" in sql
    assert "COMMENT ON COLUMN items.sku IS 'stock keeping unit'" in sql
    assert "COMMENT ON COLUMN items.sku IS 'SKU'" in sql


@pytest.mark.parametrize(
    "value, boolean, expected",
    [
        ("'18'", False, "18"),
        ("CAST('t' AS BOOLEAN)", True, "true"),
        ("(true)", True, "true"),
        ("CAST(1.50 AS DECIMAL(10,2))", False, "1.50"),
        ("NOW()", False, "now()"),
        ("'it''s'", False, "'it''s'"),
        ("'Mixed'", False, "'Mixed'"),
        ("(1 + 1)", False, "1+1"),
        ("gen_random_uuid()::VARCHAR", False, "gen_random_uuid()"),
        ("CAST(gen_random_uuid() AS VARCHAR)", False, "gen_random_uuid()"),
        ("UPPER('a b')", False, "upper('a b')"),
        ("'a' || 'b'", False, "'a'||'b'"),
    ],
)
def test_normalize_default(value: str, boolean: bool, expected: str) -> None:
    assert _normalize_default(value, boolean) == expected


def _run(code: str) -> str:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_impl_registers_when_alembic_is_imported_first() -> None:
    output = _run(
        """
        import alembic.ddl.impl
        import sqlalchemy as sa

        sa.create_engine("duckdb:///:memory:").connect().close()
        print(alembic.ddl.impl._impls["duckdb"].__name__)
        """
    )
    assert output == "DuckDBImpl"


def test_impl_registers_when_dialect_is_imported_first() -> None:
    output = _run(
        """
        import sqlalchemy as sa
        import duckdb_sqlalchemy

        import alembic.ddl.impl

        sa.create_engine("duckdb:///:memory:").connect().close()
        print(alembic.ddl.impl._impls["duckdb"].__name__)
        """
    )
    assert output == "DuckDBImpl"


def test_user_defined_impl_keeps_precedence() -> None:
    output = _run(
        """
        from alembic.ddl.impl import DefaultImpl, _impls
        import sqlalchemy as sa


        class AlembicDuckDBImpl(DefaultImpl):
            __dialect__ = "duckdb"


        sa.create_engine("duckdb:///:memory:").connect().close()
        import duckdb_sqlalchemy.alembic_impl

        print(_impls["duckdb"].__name__)
        """
    )
    assert output == "AlembicDuckDBImpl"


def _prepare_rename_schema(connection: Any, schema: Any) -> None:
    if schema == '"other.db"."odd schema"':
        connection.exec_driver_sql("ATTACH ':memory:' AS \"other.db\"")
        connection.commit()
        connection.exec_driver_sql('CREATE SCHEMA "other.db"."odd schema"')
    elif schema is not None:
        quote = connection.dialect.identifier_preparer.quote
        connection.exec_driver_sql(f"CREATE SCHEMA {quote(schema)}")


@pytest.mark.parametrize(
    "schema", [None, "analytics", "odd schema", '"other.db"."odd schema"']
)
def test_rename_table_in_schema(engine: Engine, schema: Any) -> None:
    table = Table("old.table", MetaData(), Column("value", Integer), schema=schema)
    with engine.connect() as connection:
        _prepare_rename_schema(connection, schema)
        table.create(connection)
        connection.execute(table.insert(), {"value": 42})
        connection.commit()
        Operations(MigrationContext.configure(connection)).rename_table(
            table.name, "new.table", schema=schema
        )
        renamed = table.to_metadata(MetaData(), name="new.table")
        assert connection.execute(select(renamed.c.value)).scalars().all() == [42]
        assert not inspect(connection).has_table(table.name, schema=schema)


@pytest.mark.parametrize(
    "schema", [None, "analytics", "odd schema", '"other.db"."odd schema"']
)
@pytest.mark.parametrize("copy_from_model", [False, True])
def test_batch_recreation_in_schema(
    engine: Engine, schema: Any, copy_from_model: bool
) -> None:
    table = Table(
        "items",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("sku", String, comment="stock keeping unit"),
        schema=schema,
    )
    with engine.connect() as connection:
        _prepare_rename_schema(connection, schema)
        table.create(connection)
        connection.execute(table.insert(), [{"sku": "a"}, {"sku": "b"}])
        connection.commit()
        with Operations(MigrationContext.configure(connection)).batch_alter_table(
            table.name,
            schema=schema,
            recreate="always",
            copy_from=table if copy_from_model else None,
        ) as batch:
            batch.create_unique_constraint("uq_items_sku", ["sku"])
        connection.commit()
        connection.execute(table.insert(), {"sku": "c"})
        assert connection.execute(select(table).order_by(table.c.id)).all() == [
            (1, "a"),
            (2, "b"),
            (3, "c"),
        ]
        inspector = inspect(connection)
        assert inspector.get_sequence_names(schema=schema) == ["items_id_seq"]
        assert inspector.get_unique_constraints(table.name, schema=schema)[0][
            "column_names"
        ] == ["sku"]
        assert (
            inspector.get_columns(table.name, schema=schema)[1]["comment"]
            == "stock keeping unit"
        )
        assert table.info == {}
        assert not inspector.has_table("_alembic_tmp_items", schema=schema)
        if schema in (None, "analytics"):
            table.append_constraint(UniqueConstraint("sku", name="uq_items_sku"))
            # Named schemas reflect as catalog.schema. Match that namespace
            # when checking Alembic's schema-inclusive autogeneration.
            autogen_schema = schema
            metadata = table.metadata
            if schema is not None:
                database = connection.exec_driver_sql(
                    "SELECT current_database()"
                ).scalar_one()
                quote = connection.dialect.identifier_preparer.quote
                autogen_schema = f"{quote(database)}.{quote(schema)}"
                metadata = MetaData()
                table.to_metadata(metadata, schema=autogen_schema)
            context = MigrationContext.configure(
                connection,
                opts={
                    "include_schemas": True,
                    "include_name": lambda name, type_, parents: (
                        type_ != "schema" or name == autogen_schema
                    ),
                    "compare_server_default": True,
                },
            )
            assert compare_metadata(context, metadata) == []


def test_offline_rename_in_schema() -> None:
    buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="duckdb", opts={"as_sql": True, "output_buffer": buffer}
    )
    Operations(context).rename_table("old.table", "new.table", schema="odd schema")
    assert buffer.getvalue().strip() == (
        'ALTER TABLE "odd schema"."old.table" RENAME TO "new.table";'
    )


@pytest.mark.parametrize("logical_schema", [None, "logical"])
def test_rename_table_respects_schema_translation(
    engine: Engine, logical_schema: Any
) -> None:
    table = Table(
        "old.table", MetaData(), Column("value", Integer), schema=logical_schema
    )
    with engine.connect() as connection:
        connection.exec_driver_sql('CREATE SCHEMA "odd schema"')
        connection.exec_driver_sql('CREATE TABLE "old.table" (value INTEGER)')
        connection.exec_driver_sql('INSERT INTO "old.table" VALUES (99)')
        translated = connection.execution_options(
            schema_translate_map={logical_schema: "odd schema"}
        )
        table.create(translated)
        translated.execute(table.insert(), {"value": 42})
        connection.commit()
        Operations(MigrationContext.configure(translated)).rename_table(
            table.name, "new.table", schema=logical_schema
        )
        renamed = table.to_metadata(MetaData(), name="new.table")
        assert translated.execute(select(renamed.c.value)).scalars().all() == [42]
        assert connection.exec_driver_sql('SELECT * FROM "old.table"').all() == [(99,)]
        assert not inspect(connection).has_table(table.name, schema="odd schema")
