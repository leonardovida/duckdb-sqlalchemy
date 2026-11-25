"""Alembic support for DuckDB.

Importing this module registers :class:`DuckDBImpl` as Alembic's implementation
for the ``duckdb`` dialect, unless another implementation is registered
already (for example an ``AlembicDuckDBImpl`` class defined in ``env.py``).
The dialect imports it on its own once Alembic is loaded, so ``env.py`` needs
no implementation class.
"""

import re
from typing import Any, Collection, Dict, List, Optional, Set, Tuple

import alembic.util
from alembic.autogenerate.compare import comparators as _comparators
from alembic.autogenerate.render import _repr_type
from alembic.ddl import impl as _alembic_impl
from alembic.ddl.base import (
    ColumnComment,
    RenameTable,
    format_column_name,
    format_table_name,
)
from alembic.ddl.impl import ComparisonResult, DefaultImpl
from alembic.util.sqla_compat import is_expression_index
from packaging.version import Version
from sqlalchemy import inspect, schema, text
from sqlalchemy import types as sqltypes
from sqlalchemy.ext.compiler import compiles

from . import (
    _column_needs_implicit_sequence,
    _implicit_sequence_ddl_name,
    _strip_enclosing_parentheses,
)
from ._supports import duckdb_version
from .datatypes import (
    DuckDBInteger,
    FixedArray,
    Geometry,
    Map,
    Struct,
    TimeNs,
    TimestampNs,
    Union,
    Variant,
)

__all__ = ["DuckDBImpl"]

_previous_impl = _alembic_impl._impls.get("duckdb")

# DuckDB 1.x rejects ALTER TABLE ... ADD COLUMN with NOT NULL.
_ADDS_NOT_NULL_COLUMNS = duckdb_version >= Version("2.0.0.dev0")


def _float_width(ddl: str) -> int:
    return 4 if ddl.strip().upper() in ("REAL", "FLOAT", "FLOAT4") else 8


# DuckDB stores these spellings of its integer types under the canonical name.
_INTEGER_ALIASES = {
    "uint8": "utinyint",
    "uint16": "usmallint",
    "uint32": "uinteger",
    "uint64": "ubigint",
    "varint": "bignum",
}


def _integer_name(ddl: str) -> str:
    name = ddl.strip().lower()
    return _INTEGER_ALIASES.get(name, name)


class DuckDBImpl(DefaultImpl):
    """Alembic implementation for DuckDB."""

    __dialect__ = "duckdb"
    transactional_ddl = True

    def prep_table_for_batch(self, batch_impl: Any, table: Any) -> None:
        new_table = batch_impl.new_table
        # DuckDB computes generated columns and rejects values for them, so
        # the copy into the new table leaves them out.
        for name, column in batch_impl.columns.items():
            if column.computed is not None and name in batch_impl.column_transfers:
                batch_impl.column_transfers[name].pop("expr", None)
        # Alembic builds the new table without the table comment.
        if new_table.comment is None:
            comment = table.comment
            if comment is None and not self.as_sql:
                inspector = inspect(self.connection)
                assert inspector is not None
                comment = inspector.get_table_comment(
                    table.name, schema=table.schema
                ).get("text")
            new_table.comment = comment
        implicit = [
            column
            for column in new_table.columns
            if _column_needs_implicit_sequence(column)
        ]
        if not implicit:
            return
        # copy_from models do not carry reflected defaults. Reuse the actual
        # sequence so copying explicit ids cannot reset its position.
        if self.as_sql:
            preparer = self.dialect.identifier_preparer
            defaults = {
                column.name: "nextval("
                + self.dialect.statement_compiler(
                    self.dialect, None
                ).render_literal_value(
                    _implicit_sequence_ddl_name(preparer, column), sqltypes.String()
                )
                + ")"
                for column in table.columns
                if _column_needs_implicit_sequence(column)
            }
        else:
            inspector = inspect(self.connection)
            assert inspector is not None
            defaults = {
                column["name"]: column["default"]
                for column in inspector.get_columns(table.name, schema=table.schema)
            }
        source_names = {
            column.name: name for name, column in batch_impl.columns.items()
        }
        for column in implicit:
            default = defaults.get(source_names.get(column.name, column.name))
            if default and default.lower().startswith("nextval("):
                column.server_default = schema.DefaultClause(text(default))
                # A copy_from column keeps duckdb_sequence_name, which would
                # now reject the column as not backed by an implicit sequence.
                column.dialect_options["duckdb"]["sequence_name"] = None
                preserved = getattr(self, "_preserved_batch_sequences", set())
                preserved.add(table)
                self._preserved_batch_sequences = preserved

    # DuckDB rejects DROP COLUMN, RENAME COLUMN, type and nullability changes
    # on a table that has an index, even after DROP INDEX in the same
    # transaction. Those operations recreate the table in batch mode instead,
    # which rebuilds the remaining indexes afterwards.

    def drop_index(self, index: Any, **kw: Any) -> None:
        super().drop_index(index, **kw)
        dropped = getattr(self, "_tables_with_dropped_index", set())
        dropped.add((index.table.schema, index.table.name))
        self._tables_with_dropped_index = dropped

    def drop_column(
        self, table_name: str, column: Any, schema: Optional[str] = None, **kw: Any
    ) -> None:
        indexes = self._indexes_blocking_alter(table_name, schema)
        if indexes is None:
            return super().drop_column(table_name, column, schema=schema, **kw)
        with self._recreate(table_name, schema) as batch:
            # Like PostgreSQL's DROP COLUMN, drop the column's own indexes,
            # including expression indexes that use it.
            for index in indexes:
                if _index_uses_column(index, column.name):
                    batch.drop_index(index["name"])
            batch.drop_column(column.name)

    def add_column(
        self, table_name: str, column: Any, schema: Optional[str] = None, **kw: Any
    ) -> None:
        # DuckDB 1.x rejects ADD COLUMN ... NOT NULL, and DuckDB 2.x rejects it
        # on a table that has an index. Recreate the table there instead.
        if (
            column.nullable is False
            and not self.as_sql
            and (
                not _ADDS_NOT_NULL_COLUMNS
                or self._indexes_blocking_alter(table_name, schema) is not None
            )
        ):
            added = column._copy()
            # Alembic creates the column's indexes and constraints afterwards.
            added.index = None
            added.unique = None
            with self._recreate(table_name, schema) as batch:
                batch.add_column(added)
            return
        super().add_column(table_name, column, schema=schema, **kw)

    def alter_column(self, table_name: str, column_name: str, **kw: Any) -> None:
        schema = kw.get("schema")
        blocked = any(kw.get(key) is not None for key in ("nullable", "type_", "name"))
        if not blocked or self._indexes_blocking_alter(table_name, schema) is None:
            return super().alter_column(table_name, column_name, **kw)
        options = {
            key: value
            for key, value in kw.items()
            if key not in ("schema", "name", "autoincrement", "existing_autoincrement")
        }
        if kw.get("name") is not None:
            options["new_column_name"] = kw["name"]
        with self._recreate(table_name, schema) as batch:
            batch.alter_column(column_name, **options)

    def _indexes_blocking_alter(
        self, table_name: str, schema: Optional[str]
    ) -> Optional[List[Dict[str, Any]]]:
        """Return the table's indexes when they block ALTER, else None."""
        if self.as_sql:
            return None  # offline scripts cannot reflect the table
        inspector = inspect(self.connection)
        assert inspector is not None
        indexes = list(inspector.get_indexes(table_name, schema=schema))
        dropped = getattr(self, "_tables_with_dropped_index", ())
        if indexes or (schema, table_name) in dropped:
            return indexes
        return None

    def _recreate(self, table_name: str, schema: Optional[str]) -> Any:
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        operations = Operations(MigrationContext.configure(self.connection))
        return operations.batch_alter_table(
            table_name, schema=schema, recreate="always"
        )

    def drop_table(self, table: Any, **kw: Any) -> None:
        preserved = getattr(self, "_preserved_batch_sequences", set())
        if table not in preserved:
            return super().drop_table(table, **kw)
        preserved.discard(table)
        marker = "_duckdb_preserve_implicit_sequence"
        previous = table.info.get(marker)
        table.info[marker] = True
        try:
            super().drop_table(table, **kw)
        finally:
            if previous is None:
                table.info.pop(marker, None)
            else:
                table.info[marker] = previous

    def compare_server_default(
        self,
        inspector_column: Any,
        metadata_column: Any,
        rendered_metadata_default: Any,
        rendered_inspector_default: Any,
    ) -> bool:
        # The dialect backs autoincrement primary keys with an implicit
        # sequence, which reflects as a nextval(...) default.
        if (
            metadata_column.primary_key
            and metadata_column is metadata_column.table._autoincrement_column
            and metadata_column.server_default is None
        ):
            return False
        metadata_arg = getattr(metadata_column.server_default, "arg", None)
        if isinstance(metadata_arg, str):
            # SQLAlchemy emits a plain string default as a quoted literal, and
            # DuckDB reflects it that way. Alembic passes the bare string.
            rendered_metadata_default = "'" + metadata_arg.replace("'", "''") + "'"
        if rendered_inspector_default == rendered_metadata_default:
            return False
        if None in (rendered_inspector_default, rendered_metadata_default):
            return True
        # DuckDB stores defaults in its own spelling: '1' for 1, (1 + 1) for
        # 1 + 1, CAST('t' AS BOOLEAN) for true. Compare the normalized text
        # instead of querying, which would abort the migration transaction
        # on an error.
        boolean = isinstance(inspector_column.type, sqltypes.Boolean)
        column_type = self.dialect.type_compiler_instance.process(inspector_column.type)
        return _normalize_default(
            rendered_inspector_default, boolean, column_type
        ) != _normalize_default(rendered_metadata_default, boolean, column_type)

    def compare_type(self, inspector_column: Any, metadata_column: Any) -> bool:
        # Alembic compares type classes, so a STRUCT that gained a field
        # looks unchanged. Compare nested types member by member instead.
        inspector_type = inspector_column.type
        metadata_type = metadata_column.type
        if (
            isinstance(inspector_type, FixedArray)
            or isinstance(metadata_type, FixedArray)
            or (_is_nested(inspector_type) and _is_nested(metadata_type))
        ):
            return self._nested_type_differs(inspector_type, metadata_type)
        if isinstance(inspector_type, sqltypes.Enum) and isinstance(
            metadata_type, sqltypes.Enum
        ):
            if not inspector_type.name:
                # DuckDB does not report which named type a column uses.
                return list(inspector_type.enums) != list(metadata_type.enums)
        elif _is_varchar(inspector_type) and _is_varchar(metadata_type):
            # TEXT, CHAR(n), NCHAR, VARCHAR(n) and STRING are all VARCHAR in
            # DuckDB, which keeps no length, so they reflect as String().
            return False
        elif isinstance(inspector_type, DuckDBInteger) or isinstance(
            metadata_type, DuckDBInteger
        ):
            # UInt32 creates a UINTEGER column, which reflects as UInteger.
            compiler = self.dialect.type_compiler_instance
            return _integer_name(compiler.process(inspector_type)) != _integer_name(
                compiler.process(metadata_type)
            )
        elif isinstance(inspector_type, sqltypes.Float) and isinstance(
            metadata_type, sqltypes.Float
        ):
            # FLOAT, FLOAT4 and REAL are one 4-byte type in DuckDB.
            compiler = self.dialect.type_compiler_instance
            return _float_width(compiler.process(inspector_type)) != _float_width(
                compiler.process(metadata_type)
            )
        return super().compare_type(inspector_column, metadata_column)

    def _nested_type_differs(self, inspector_type: Any, metadata_type: Any) -> bool:
        """Compare STRUCT, UNION, MAP and array members with compare_type.

        Members then follow the rules for top-level columns: VARCHAR lengths,
        integer aliases, float widths and ENUM labels.
        """
        inspector_type = _unwrap_type(inspector_type)
        metadata_type = _unwrap_type(metadata_type)
        for kind in (Struct, Union, Map, FixedArray, sqltypes.ARRAY):
            if isinstance(inspector_type, kind) != isinstance(metadata_type, kind):
                return True
        if isinstance(inspector_type, (Struct, Union)):
            inspector_fields = list(inspector_type._fields or ())
            metadata_fields = list(metadata_type._fields or ())
            return [name for name, _ in inspector_fields] != [
                name for name, _ in metadata_fields
            ] or any(
                self._nested_type_differs(inspector_member, metadata_member)
                for (_, inspector_member), (_, metadata_member) in zip(
                    inspector_fields, metadata_fields
                )
            )
        if isinstance(inspector_type, Map):
            return self._nested_type_differs(
                inspector_type.key_type, metadata_type.key_type
            ) or self._nested_type_differs(
                inspector_type.value_type, metadata_type.value_type
            )
        if isinstance(inspector_type, FixedArray):
            return inspector_type.size != metadata_type.size or (
                self._nested_type_differs(
                    inspector_type.item_type, metadata_type.item_type
                )
            )
        if isinstance(inspector_type, sqltypes.ARRAY):
            return (inspector_type.dimensions or 1) != (
                metadata_type.dimensions or 1
            ) or self._nested_type_differs(
                inspector_type.item_type, metadata_type.item_type
            )
        return self.compare_type(
            schema.Column("member", inspector_type),
            schema.Column("member", metadata_type),
        )

    def compare_indexes(self, metadata_index: Any, reflected_index: Any) -> Any:
        # The generic comparison only compares column names, so every
        # expression index looked changed. Compare the expressions' SQL.
        if not (
            is_expression_index(metadata_index) or is_expression_index(reflected_index)
        ):
            return super().compare_indexes(metadata_index, reflected_index)
        changes = []
        unique = self._compare_index_unique(metadata_index, reflected_index)
        if unique:
            changes.append(unique)
        metadata_sql = [self._index_element_sql(e) for e in metadata_index.expressions]
        reflected_sql = [
            self._index_element_sql(e) for e in reflected_index.expressions
        ]
        if metadata_sql != reflected_sql:
            changes.append(f"expressions {reflected_sql} to {metadata_sql}")
        if changes:
            return ComparisonResult.Different(changes)
        return ComparisonResult.Equal()

    def _index_element_sql(self, element: Any) -> str:
        if isinstance(element, str):
            sql = element
        else:
            sql = str(
                element.compile(
                    dialect=self.dialect,
                    compile_kwargs={"literal_binds": True, "include_table": False},
                )
            )
        return _normalize_index_sql(sql)

    def correct_for_autogen_constraints(
        self,
        conn_uniques: Any,
        conn_indexes: Any,
        metadata_unique_constraints: Any,
        metadata_indexes: Any,
    ) -> None:
        # DuckDB replaces constraint names with its own (``items_sku_key``), so
        # a named unique constraint in the model is matched by its columns.
        if not isinstance(conn_uniques, set) or not isinstance(
            metadata_unique_constraints, set
        ):
            return
        conn_names = {uc.name for uc in conn_uniques}
        conn_by_columns: Dict[Tuple[str, ...], List[Any]] = {}
        for uc in conn_uniques:
            conn_by_columns.setdefault(_column_names(uc.columns), []).append(uc)
        for uc in list(metadata_unique_constraints):
            if not isinstance(uc.name, str) or uc.name in conn_names:
                continue
            matches = conn_by_columns.get(_column_names(uc.columns))
            if matches:
                conn_uniques.discard(matches.pop())
                metadata_unique_constraints.discard(uc)

    def render_type(self, type_obj: Any, autogen_context: Any) -> Any:
        # Alembic's default rendering uses repr(), which prints member types
        # as classes (<class 'sqlalchemy...Integer'>) inside these types.
        if isinstance(type_obj, FixedArray):
            autogen_context.imports.add("import duckdb_sqlalchemy.datatypes")
            item = _repr_type(type_obj.item_type, autogen_context)
            return f"duckdb_sqlalchemy.datatypes.FixedArray({item}, {type_obj.size})"
        if isinstance(type_obj, (Struct, Union, Map)):
            autogen_context.imports.add("import duckdb_sqlalchemy.datatypes")
            name = f"duckdb_sqlalchemy.datatypes.{type(type_obj).__name__}"
            if isinstance(type_obj, Map):
                key = _repr_type(type_obj.key_type, autogen_context)
                value = _repr_type(type_obj.value_type, autogen_context)
                return f"{name}({key}, {value})"
            fields = ", ".join(
                f"{field!r}: {_repr_type(member, autogen_context)}"
                for field, member in type_obj._fields or ()
            )
            return f"{name}({{{fields}}})"
        if isinstance(type_obj, _SCALAR_DUCKDB_TYPES):
            autogen_context.imports.add("import duckdb_sqlalchemy.datatypes")
            return f"duckdb_sqlalchemy.datatypes.{type(type_obj).__name__}()"
        if isinstance(type_obj, sqltypes.ARRAY) and _contains_duckdb_type(
            type_obj.item_type
        ):
            item = _repr_type(type_obj.item_type, autogen_context)
            dimensions = (
                f", dimensions={type_obj.dimensions}" if type_obj.dimensions else ""
            )
            return f"sa.ARRAY({item}{dimensions})"
        return False


_CAST_RE = re.compile(r"CAST\((.*) AS ([\w\s(),]+)\)", re.IGNORECASE | re.DOTALL)
# DuckDB stores x::VARCHAR as CAST(x AS VARCHAR)
_SHORT_CAST_RE = re.compile(r"(.*?)\s*::\s*([\w\s(),]+)", re.DOTALL)
_STRING_RE = re.compile(r"('(?:[^']|'')*')")
_BOOLEAN_DEFAULTS = {"t": "true", "true": "true", "f": "false", "false": "false"}


def _normalize_sql_expression(value: str) -> str:
    """Normalize SQL tokens while retaining quoted identifier/literal contents."""
    parts = re.split(r"""('(?:[^']|'')*'|"(?:[^"]|"")*")""", value)
    return "".join(
        part if part.startswith(("'", '"')) else re.sub(r"\s+", "", part.lower())
        for part in parts
    )


def _normalize_default(
    value: str, boolean: bool, column_type: Optional[str] = None
) -> str:
    value = _strip_enclosing_parentheses(str(value))
    match = _CAST_RE.fullmatch(value) or _SHORT_CAST_RE.fullmatch(value)
    if match and (
        column_type is None
        or _normalize_sql_expression(match.group(2)).replace("decimal", "numeric")
        == _normalize_sql_expression(column_type).replace("decimal", "numeric")
    ):
        # Casting to the column type is redundant at insertion. A cast to a
        # different type can round or truncate first and must remain visible.
        value = _strip_enclosing_parentheses(match.group(1))
    if boolean:
        if _STRING_RE.fullmatch(value):
            value = value[1:-1].replace("''", "'")
        return _BOOLEAN_DEFAULTS.get(value.lower(), value)
    # Numeric quoted literals are DuckDB's canonical spelling of numeric
    # defaults. Other strings must stay quoted: '1+1' is not the expression 1+1.
    if _STRING_RE.fullmatch(value) and re.fullmatch(
        r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", value[1:-1]
    ):
        return value[1:-1]
    return _normalize_sql_expression(value)


def _is_varchar(type_obj: Any) -> bool:
    return isinstance(type_obj, sqltypes.String) and not isinstance(
        type_obj, sqltypes.Enum
    )


def _unwrap_type(type_obj: Any) -> Any:
    """The nested type a TypeDecorator wraps. Scalar decorators such as
    ``Interval`` stay, as they compile to their own DDL."""
    type_obj = sqltypes.to_instance(type_obj)
    while isinstance(type_obj, sqltypes.TypeDecorator):
        impl = sqltypes.to_instance(type_obj.impl_instance)
        if not isinstance(impl, (Struct, Union, Map, FixedArray, sqltypes.ARRAY)):
            break
        type_obj = impl
    return type_obj


def _is_nested(type_obj: Any) -> bool:
    if isinstance(type_obj, sqltypes.ARRAY):
        # A LIST of a named ENUM reflects with an unnamed ENUM item.
        return isinstance(
            type_obj.item_type, (Struct, Union, Map, FixedArray, sqltypes.Enum)
        )
    return isinstance(type_obj, (Struct, Union, Map, FixedArray))


_SCALAR_DUCKDB_TYPES = (DuckDBInteger, TimestampNs, TimeNs, Variant, Geometry)


def _contains_duckdb_type(type_obj: Any) -> bool:
    if isinstance(type_obj, (FixedArray, Struct, Union, Map, *_SCALAR_DUCKDB_TYPES)):
        return True
    return isinstance(type_obj, sqltypes.ARRAY) and _contains_duckdb_type(
        type_obj.item_type
    )


def _column_names(columns: Collection[Any]) -> Tuple[str, ...]:
    return tuple(column.name for column in columns)


# A string literal, a quoted identifier, or a bare word.
_SQL_TOKEN_RE = re.compile(r"""'(?:[^']|'')*'|"((?:[^"]|"")*)"|([A-Za-z_][\w$]*)""")


def _normalize_index_sql(value: str) -> str:
    """Index expression SQL without case, spacing, identifier quotes or outer
    parentheses, keeping string literals. DuckDB reports ``lower(email)`` as
    ``(lower(email))``.
    """
    value = _strip_enclosing_parentheses(value)
    parts = re.split(r"""('(?:[^']|'')*'|"(?:[^"]|"")*")""", value)
    return "".join(
        part
        if part.startswith("'")
        else part[1:-1].replace('""', '"').lower()
        if part.startswith('"')
        else re.sub(r"\s+", "", part.lower())
        for part in parts
    )


def _index_uses_column(index: Dict[str, Any], column_name: str) -> bool:
    """Whether a reflected index covers ``column_name`` or uses it in an expression."""
    if column_name in (index.get("column_names") or ()):
        return True
    target = column_name.lower()
    for expression in index.get("expressions") or ():
        for match in _SQL_TOKEN_RE.finditer(expression):
            quoted, bare = match.groups()
            if quoted is not None:
                name = quoted.replace('""', '"')
            elif bare is not None and not expression[match.end() :].lstrip().startswith(
                "("
            ):
                name = bare
            else:
                continue  # a string literal or a function name
            if name.lower() == target:
                return True
    return False


def _discard_duplicate_schemas(autogen_context: Any, schemas: Set[Any]) -> None:
    """Remove the schemas that ``include_schemas=True`` would compare twice.

    ``get_schema_names()`` names schemas ``database.schema``. Alembic replaces
    only the bare default schema name with ``None``, so it compared the current
    schema both as ``None`` and as ``db.main`` and proposed dropping every
    table in it. The ``system`` and ``temp`` catalogs hold no user tables.
    """
    connection = autogen_context.connection
    if connection is None:
        return
    preparer = connection.dialect.identifier_preparer
    database, current_schema = connection.execute(
        text("SELECT current_database(), current_schema()")
    ).one()
    current = (database.lower(), current_schema.lower())
    for name in [name for name in schemas if name is not None]:
        try:
            database_name, schema_name = preparer._separate(name)
        except ValueError:
            continue
        if database_name is None or schema_name is None:
            continue
        if database_name.lower() in ("system", "temp") or (
            (database_name.lower(), schema_name.lower()) == current
        ):
            schemas.discard(name)


if hasattr(alembic.util, "DispatchPriority"):
    # Alembic 1.18+ runs a dialect's comparators before the default ones.
    @_comparators.dispatch_for(
        "schema", qualifier="duckdb", priority=alembic.util.DispatchPriority.FIRST
    )
    def _compare_duckdb_schemas(
        autogen_context: Any, upgrade_ops: Any, schemas: Set[Any]
    ) -> Any:
        _discard_duplicate_schemas(autogen_context, schemas)
        return alembic.util.PriorityDispatchResult.CONTINUE

else:  # pragma: no cover - depends on the Alembic version
    # Earlier versions run a dialect's comparators instead of the default ones.
    @_comparators.dispatch_for("schema", qualifier="duckdb")
    def _compare_duckdb_schemas(
        autogen_context: Any, upgrade_ops: Any, schemas: Set[Any]
    ) -> Any:
        _discard_duplicate_schemas(autogen_context, schemas)
        _comparators.dispatch("schema")(autogen_context, upgrade_ops, schemas)


if _previous_impl is not None:
    # keep an implementation that env.py registered before this module loaded
    _alembic_impl._impls["duckdb"] = _previous_impl


@compiles(RenameTable, "duckdb")
def _visit_rename_table(element: RenameTable, compiler: Any, **kw: Any) -> str:
    # DuckDB keeps the table in its current schema. RENAME TO accepts only
    # the new identifier, unlike Alembic's generic schema-qualified target.
    preparer = compiler.preparer
    source = schema.Table(element.table_name, schema.MetaData(), schema=element.schema)
    table = preparer.format_table(source)
    return f"ALTER TABLE {table} RENAME TO {preparer.quote(element.new_table_name)}"


@compiles(ColumnComment, "duckdb")
def _visit_column_comment(element: ColumnComment, compiler: Any, **kw: Any) -> str:
    comment = (
        compiler.sql_compiler.render_literal_value(element.comment, sqltypes.String())
        if element.comment is not None
        else "NULL"
    )
    return "COMMENT ON COLUMN {table}.{column} IS {comment}".format(
        table=format_table_name(compiler, element.table_name, element.schema),
        column=format_column_name(compiler, element.column_name),
        comment=comment,
    )
