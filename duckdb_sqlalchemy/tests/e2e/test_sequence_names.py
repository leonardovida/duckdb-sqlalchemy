"""`duckdb_sequence_name` names the sequence behind an autoincrement primary key."""

from pathlib import Path
from typing import Any, List, Optional, Tuple

import pytest
from sqlalchemy import (
    BigInteger,
    Column,
    Identity,
    Integer,
    MetaData,
    String,
    Table,
    insert,
    select,
    text,
)
from sqlalchemy.engine import Connection
from sqlalchemy.exc import CompileError, ProgrammingError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from .conftest import EngineFactory


def _sequences(conn: Connection) -> List[Tuple[str, str]]:
    return [
        tuple(row)
        for row in conn.execute(
            text(
                "SELECT schema_name, sequence_name FROM duckdb_sequences() "
                "WHERE NOT temporary ORDER BY 1, 2"
            )
        )
    ]


@pytest.mark.parametrize("identity", [True, False], ids=["identity", "plain"])
def test_named_sequence_backs_the_key_and_drops_with_the_table(
    make_engine: EngineFactory, db_path: Path, identity: bool
) -> None:
    metadata = MetaData()
    widget = Table(
        "widget",
        metadata,
        Column(
            "id",
            BigInteger,
            *([Identity()] if identity else []),
            primary_key=True,
            duckdb_sequence_name="seq_widget",
        ),
        Column("name", String),
    )
    engine = make_engine(db_path)
    with engine.begin() as conn:
        metadata.create_all(conn)
        assert _sequences(conn) == [("main", "seq_widget")]
        ids = conn.execute(
            insert(widget).returning(widget.c.id), [{"name": "a"}, {"name": "b"}]
        )
        assert ids.scalars().all() == [1, 2]
        # Clients that name the sequence draw from the same one.
        conn.execute(text("INSERT INTO widget VALUES (nextval('seq_widget'), 'c')"))
        assert conn.execute(select(widget.c.id).order_by(widget.c.id)).all() == [
            (1,),
            (2,),
            (3,),
        ]

    engine.dispose()
    with make_engine(db_path).begin() as conn:
        metadata.drop_all(conn)
        assert _sequences(conn) == []


def test_named_sequence_follows_schema_and_schema_translate_map(
    make_engine: EngineFactory, db_path: Path
) -> None:
    metadata = MetaData()
    table = Table(
        "items",
        metadata,
        Column("id", Integer, primary_key=True, duckdb_sequence_name="item_ids"),
        Column("v", String),
        schema="logical",
    )
    with make_engine(db_path).begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA physical")
        translated = conn.execution_options(
            schema_translate_map={"logical": "physical"}
        )
        metadata.create_all(translated)
        assert _sequences(conn) == [("physical", "item_ids")]
        translated.execute(insert(table), [{"v": "a"}, {"v": "b"}])
        assert translated.execute(select(table.c.id).order_by(table.c.id)).all() == [
            (1,),
            (2,),
        ]
        metadata.drop_all(translated)
        assert _sequences(conn) == []


@pytest.mark.parametrize("schema", [None, "sales"])
def test_insert_without_returning_draws_from_the_named_sequence(
    make_engine: EngineFactory, db_path: Path, schema: Optional[str]
) -> None:
    metadata = MetaData()
    table = Table(
        "events",
        metadata,
        Column(
            "id",
            BigInteger,
            Identity(),
            primary_key=True,
            duckdb_sequence_name="seq_events",
        ),
        Column("v", String),
        schema=schema,
        implicit_returning=False,
    )
    with make_engine(db_path).begin() as conn:
        if schema:
            conn.exec_driver_sql(f"CREATE SCHEMA {schema}")
        metadata.create_all(conn)
        first = conn.execute(insert(table).values(v="a")).inserted_primary_key
        second = conn.execute(insert(table).values(v="b")).inserted_primary_key
        assert (tuple(first), tuple(second)) == ((1,), (2,))
        assert conn.execute(
            select(table.c.id, table.c.v).order_by(table.c.id)
        ).all() == [
            (1, "a"),
            (2, "b"),
        ]


def test_insert_without_returning_follows_schema_translate_map(
    make_engine: EngineFactory, db_path: Path
) -> None:
    metadata = MetaData()
    table = Table(
        "events",
        metadata,
        Column("id", Integer, primary_key=True, duckdb_sequence_name="seq_events"),
        Column("v", String),
        schema="logical",
        implicit_returning=False,
    )
    with make_engine(db_path).begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA physical")
        translated = conn.execution_options(
            schema_translate_map={"logical": "physical"}
        )
        metadata.create_all(translated)
        key = translated.execute(insert(table).values(v="a")).inserted_primary_key
        assert tuple(key) == (1,)
        assert translated.execute(select(table.c.id, table.c.v)).all() == [(1, "a")]


def test_existing_sequence_of_that_name_fails_the_create(
    make_engine: EngineFactory, db_path: Path
) -> None:
    metadata = MetaData()
    Table(
        "widget",
        metadata,
        Column("id", Integer, primary_key=True, duckdb_sequence_name="shared"),
    )
    engine = make_engine(db_path)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SEQUENCE shared START 100")
    with pytest.raises(ProgrammingError, match="already exists"):
        with engine.begin() as conn:
            metadata.create_all(conn)
    with engine.connect() as conn:
        # Neither adopted nor dropped: the other sequence keeps its position.
        assert conn.execute(text("SELECT nextval('shared')")).scalar_one() == 100
        assert not engine.dialect.has_table(conn, "widget")


def test_alembic_create_drop_create_cycle(make_engine: EngineFactory) -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    def create(op: Operations) -> None:
        op.create_table(
            "users",
            Column("id", Integer, primary_key=True, duckdb_sequence_name="seq_users"),
            Column("name", String),
        )

    engine = make_engine("duckdb:///:memory:")
    with pytest.raises(ProgrammingError, match="already exists"):
        with engine.begin() as conn:
            op = Operations(MigrationContext.configure(conn))
            create(op)
            # op.drop_table builds a table without columns, so it cannot tell
            # which sequence to drop.
            op.drop_table("users")
            create(op)
    with engine.begin() as conn:
        op = Operations(MigrationContext.configure(conn))
        create(op)  # upgrade
        op.drop_table("users")  # downgrade, which must drop the sequence too
        op.execute("DROP SEQUENCE seq_users")
        create(op)  # upgrade again
        conn.execute(text("INSERT INTO users (name) VALUES ('a')"))
        assert conn.execute(text("SELECT name FROM users")).all() == [("a",)]
        assert _sequences(conn) == [("main", "seq_users")]


def _users(metadata: MetaData) -> Table:
    return Table(
        "users",
        metadata,
        Column(
            "id",
            Integer,
            Identity(),
            primary_key=True,
            duckdb_sequence_name="seq_users",
        ),
        Column("name", String),
    )


def test_alembic_batch_recreate_with_copy_from_keeps_the_sequence(
    make_engine: EngineFactory, db_path: Path
) -> None:
    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    metadata = MetaData()
    users = _users(metadata)
    engine = make_engine(db_path)
    with engine.begin() as conn:
        metadata.create_all(conn)
        conn.execute(insert(users), [{"name": "a"}, {"name": "b"}])
    with engine.begin() as conn:
        op = Operations(MigrationContext.configure(conn))
        with op.batch_alter_table("users", recreate="always", copy_from=users) as batch:
            batch.add_column(Column("email", String))
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO users (name) VALUES ('c')"))
        rows = conn.execute(text("SELECT id, name, email FROM users ORDER BY id"))
        assert rows.all() == [(1, "a", None), (2, "b", None), (3, "c", None)]
        assert _sequences(conn) == [("main", "seq_users")]


def test_offline_alembic_batch_recreate_with_copy_from(
    make_engine: EngineFactory, db_path: Path
) -> None:
    import io

    pytest.importorskip("alembic")
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    metadata = MetaData()
    users = _users(metadata)
    engine = make_engine(db_path)
    with engine.begin() as conn:
        metadata.create_all(conn)
        conn.execute(insert(users), [{"name": "a"}])

    buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="duckdb",
        opts={"as_sql": True, "output_buffer": buffer, "literal_binds": True},
    )
    with Operations.context(context):
        op = Operations(context)
        with op.batch_alter_table("users", recreate="always", copy_from=users) as batch:
            batch.add_column(Column("email", String))

    with engine.connect() as conn:
        autocommit = conn.execution_options(isolation_level="AUTOCOMMIT")
        autocommit.exec_driver_sql(buffer.getvalue())
        autocommit.exec_driver_sql("INSERT INTO users (name) VALUES ('b')")
        rows = autocommit.exec_driver_sql("SELECT id, name FROM users ORDER BY id")
        assert rows.all() == [(1, "a"), (2, "b")]
        assert _sequences(autocommit) == [("main", "seq_users")]


def test_named_sequence_for_a_temporary_table(make_engine: EngineFactory) -> None:
    metadata = MetaData()
    table = Table(
        "scratch",
        metadata,
        Column("id", Integer, primary_key=True, duckdb_sequence_name="scratch_ids"),
        Column("v", String),
        prefixes=["TEMPORARY"],
    )
    with make_engine("duckdb:///:memory:").connect() as conn:
        metadata.create_all(conn)
        conn.execute(insert(table), [{"v": "a"}, {"v": "b"}])
        assert conn.execute(select(table.c.id).order_by(table.c.id)).all() == [
            (1,),
            (2,),
        ]
        temporary = conn.execute(
            text("SELECT sequence_name FROM duckdb_sequences() WHERE temporary")
        )
        assert temporary.scalars().all() == ["scratch_ids"]


@pytest.mark.parametrize(
    "column",
    [
        Column("n", Integer, duckdb_sequence_name="seq_n"),
        Column("code", String, duckdb_sequence_name="seq_code"),
    ],
    ids=["integer", "string"],
)
def test_name_on_a_column_without_a_sequence_raises(
    make_engine: EngineFactory, column: Any
) -> None:
    metadata = MetaData()
    Table(
        "t",
        metadata,
        Column("id", Integer, primary_key=True, duckdb_sequence_name="seq_t"),
        column,
    )
    engine = make_engine("duckdb:///:memory:")
    with engine.connect() as conn:
        # Outside a transaction, a sequence created first would be left behind.
        autocommit = conn.execution_options(isolation_level="AUTOCOMMIT")
        with pytest.raises(CompileError, match="duckdb_sequence_name"):
            metadata.create_all(autocommit)
        assert _sequences(autocommit) == []


def test_orm_mapped_column_names_its_sequence(
    make_engine: EngineFactory, db_path: Path
) -> None:
    class Base(DeclarativeBase):
        pass

    class Widget(Base):
        __tablename__ = "widget"

        id: Mapped[int] = mapped_column(
            BigInteger, Identity(), primary_key=True, duckdb_sequence_name="seq_widget"
        )
        name: Mapped[str]

    engine = make_engine(db_path)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all([Widget(name="a"), Widget(name="b")])
        session.commit()
        assert session.scalars(select(Widget.id).order_by(Widget.id)).all() == [1, 2]
    with engine.connect() as conn:
        assert _sequences(conn) == [("main", "seq_widget")]
